//! The file config provider: lyft/goruntime v0.3.0's loader (`loader/`), which keeps a snapshot
//! of a directory's files and refreshes it on file system events, and envoyproxy/ratelimit's
//! `FileProvider` (`src/provider/file_provider.go`), which loads the config from each snapshot.
//!
//! Events come from inotify on Linux, as fsnotify v1.7.0's backend reads them; elsewhere (for
//! development) the watched directory is polled.

use crate::config::{self, ConfigError, RateLimitConfig};
use crate::go;
use crate::settings::Settings;
use crate::stats::{Counter, Stats};
use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::sync::{Arc, Mutex};

/// goruntime's `FileSystemOp`: what happened to a watched path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsOp {
    Create,
    Write,
    Remove,
    Rename,
    Chmod,
}

/// fsnotify's event operations, as a set of flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Ops {
    pub create: bool,
    pub write: bool,
    pub remove: bool,
    pub rename: bool,
    pub chmod: bool,
}

impl Ops {
    /// goruntime's `getFileSystemOp`: the operation of an event with exactly one of them, or
    /// `Write` for an event with none (an overflow, an unmount), as its `switch` falls there.
    pub fn single(self) -> Option<FsOp> {
        let flags = [
            (self.create, FsOp::Create),
            (self.write, FsOp::Write),
            (self.remove, FsOp::Remove),
            (self.rename, FsOp::Rename),
            (self.chmod, FsOp::Chmod),
        ];
        let mut set = flags.iter().filter(|(on, _)| *on).map(|(_, op)| *op);
        match (set.next(), set.next()) {
            (None, _) => Some(FsOp::Write),
            (Some(op), None) => Some(op),
            _ => None,
        }
    }
}

/// goruntime's refreshers: which directory to watch, and which events reload.
#[derive(Debug, Clone)]
pub enum Refresher {
    /// `SymlinkRefresher`: watches the directory holding `runtime_path` (a symlink swapped to
    /// deploy a new config), and reloads when `runtime_path` itself is created or written.
    Symlink { runtime_path: String },
    /// `DirectoryRefresher`: watches the config directory, and reloads on the given operations
    /// on its direct entries.
    Directory { current_dir: String, ops: Vec<FsOp> },
}

impl Refresher {
    /// `WatchDirectory`: the directory to watch for `runtime_path` and `app_dir` (which the
    /// directory refresher also keeps, to compare event paths with).
    pub fn watch_directory(&mut self, runtime_path: &str, app_dir: &str) -> String {
        match self {
            Refresher::Symlink { .. } => go::dir(runtime_path),
            Refresher::Directory { current_dir, .. } => {
                *current_dir = go::join(&[runtime_path, app_dir]);
                current_dir.clone()
            }
        }
    }

    /// `ShouldRefresh`, comparing paths as Go does: as strings.
    pub fn should_refresh(&self, path: &str, op: Option<FsOp>) -> bool {
        match self {
            Refresher::Symlink { runtime_path } => {
                path == runtime_path && matches!(op, Some(FsOp::Write | FsOp::Create))
            }
            Refresher::Directory { current_dir, ops } => {
                go::dir(path) == *current_dir && op.is_some_and(|op| ops.contains(&op))
            }
        }
    }
}

/// The files of a runtime directory: key (the path relative to it, `/` replaced by `.`) to
/// contents.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Snapshot {
    pub entries: BTreeMap<String, Vec<u8>>,
}

/// goruntime's `onRuntimeChanged` walk of `watch_path/subdirectory`, as `filepath.Walk` goes:
/// in name order, without following symlinks (a symlinked file is read through, a symlinked
/// directory fails to read). Returns the snapshot and the number of failures.
pub fn load_snapshot(
    watch_path: &str,
    subdirectory: &str,
    ignore_dotfiles: bool,
) -> (Snapshot, u64) {
    let root = OsString::from(go::join(&[watch_path, subdirectory]));
    let mut walk = Walk {
        root: root.clone(),
        ignore_dotfiles,
        snapshot: Snapshot::default(),
        failures: 0,
    };
    match std::fs::symlink_metadata(&root) {
        Ok(metadata) => {
            let name = base_name(&root.to_string_lossy());
            walk.visit(&root, name.as_bytes(), &metadata);
        }
        Err(e) => walk.failed(&root, &path_error("lstat", &root, &e)),
    }
    (walk.snapshot, walk.failures)
}

/// `filepath.Base` of a cleaned path: what `FileInfo.Name()` gives for the walk's root.
fn base_name(path: &str) -> String {
    match path.trim_end_matches('/') {
        "" => "/".to_string(),
        trimmed => trimmed.rsplit('/').next().unwrap().to_string(),
    }
}

/// `filepath.Join(dir, name)` for a cleaned `dir` and a directory entry's name, on bytes (a
/// name need not be UTF-8).
fn child_path(dir: &OsStr, name: &OsStr) -> OsString {
    let mut path = match dir.as_bytes() {
        b"." => Vec::new(),
        b"/" => b"/".to_vec(),
        dir => [dir, b"/"].concat(),
    };
    path.extend_from_slice(name.as_bytes());
    OsString::from_vec(path)
}

struct Walk {
    root: OsString,
    ignore_dotfiles: bool,
    snapshot: Snapshot,
    failures: u64,
}

impl Walk {
    /// A failure goruntime counts and logs (the callback's error, `runtime: error processing`).
    fn failed(&mut self, path: &OsStr, error: &str) {
        self.failures += 1;
        crate::log::warn(&format!(
            "runtime: error processing {}: {error}",
            path.to_string_lossy()
        ));
    }

    /// `walk` and goruntime's `walkDirectoryCallback` for one path.
    fn visit(&mut self, path: &OsStr, name: &[u8], metadata: &std::fs::Metadata) {
        let hidden = self.ignore_dotfiles && name.starts_with(b".");
        if !metadata.is_dir() {
            if hidden {
                return;
            }
            match std::fs::read(path) {
                Ok(contents) => {
                    let key = self.key(path);
                    self.snapshot.entries.insert(key, contents);
                }
                Err(e) => {
                    // Opening a directory works; reading it does not.
                    let op = match e.kind() {
                        std::io::ErrorKind::IsADirectory => "read",
                        _ => "open",
                    };
                    self.failures += 1;
                    crate::log::warn(&format!(
                        "runtime: error reading {}: {}",
                        path.to_string_lossy(),
                        path_error(op, path, &e)
                    ));
                }
            }
            return;
        }
        let names = std::fs::read_dir(path).map(|entries| {
            let mut names: Vec<OsString> = entries
                .filter_map(Result::ok)
                .map(|e| e.file_name())
                .collect();
            names.sort();
            names
        });
        // The callback sees an unreadable directory once, as an error.
        let names = match names {
            Ok(names) => names,
            Err(e) => {
                self.failed(path, &path_error("open", path, &e));
                return;
            }
        };
        if hidden {
            return;
        }
        for name in names {
            let child = child_path(path, &name);
            match std::fs::symlink_metadata(&child) {
                Ok(metadata) => self.visit(&child, name.as_bytes(), &metadata),
                Err(e) => self.failed(&child, &path_error("lstat", &child, &e)),
            }
        }
    }

    /// `filepath.Rel(root, path)` with `/` replaced by `.` (`.` for the root itself).
    fn key(&self, path: &OsStr) -> String {
        let (root, path) = (self.root.as_bytes(), path.as_bytes());
        let relative = match root {
            _ if path == root => b".".as_slice(),
            b"." => path,
            b"/" => &path[1..],
            _ => &path[root.len() + 1..],
        };
        String::from_utf8_lossy(relative).replace('/', ".")
    }
}

/// goruntime's `loaderStats`.
struct LoaderStats {
    load_attempts: Counter,
    load_failures: Counter,
    /// The entries of the last snapshot.
    num_values: crate::stats::Gauge,
}

/// What `Loader`'s callbacks and the watcher thread share.
struct Shared {
    snapshot: Mutex<Snapshot>,
    /// One notification channel per callback, holding at most one pending signal.
    callbacks: Mutex<Vec<SyncSender<()>>>,
}

/// goruntime's loader: the current snapshot, refreshed on matching events, or the nil loader
/// (an empty snapshot that never changes) when no runtime directory is configured.
#[derive(Clone)]
pub struct Loader {
    shared: Option<Arc<Shared>>,
}

impl Loader {
    /// `New2`: watches the refresher's directory, takes the first snapshot, then refreshes on
    /// matching events. An empty `runtime_path` or `subdirectory` gives the nil loader.
    pub fn new(
        runtime_path: &str,
        subdirectory: &str,
        stats: &Stats,
        scope: &crate::stats::Scope,
        mut refresher: Refresher,
        ignore_dotfiles: bool,
    ) -> Result<Self, String> {
        if runtime_path.is_empty() || subdirectory.is_empty() {
            crate::log::warn("no runtime configuration. using nil loader.");
            return Ok(Self { shared: None });
        }
        let watched = refresher.watch_directory(runtime_path, subdirectory);
        let mut watcher = watcher::Watcher::new(&watched)?;
        let loader_stats = LoaderStats {
            load_attempts: stats.scope_counter(scope, "load_attempts"),
            load_failures: stats.scope_counter(scope, "load_failures"),
            num_values: stats.scope_gauge(scope, "num_values"),
        };
        let shared = Arc::new(Shared {
            snapshot: Mutex::new(Snapshot::default()),
            callbacks: Mutex::new(Vec::new()),
        });
        let (runtime_path, subdirectory) = (runtime_path.to_string(), subdirectory.to_string());
        let changed = {
            let shared = shared.clone();
            move || {
                let (snapshot, failures) =
                    load_snapshot(&runtime_path, &subdirectory, ignore_dotfiles);
                loader_stats.load_failures.add(failures);
                loader_stats.load_attempts.inc();
                loader_stats.num_values.set(snapshot.entries.len() as u64);
                *shared.snapshot.lock().unwrap() = snapshot;
                for callback in shared.callbacks.lock().unwrap().iter() {
                    let _ = callback.try_send(());
                }
            }
        };
        changed();
        std::thread::spawn(move || {
            while let Some((path, ops)) = watcher.next_event() {
                if refresher.should_refresh(&path, ops.single()) {
                    changed();
                }
            }
        });
        Ok(Self {
            shared: Some(shared),
        })
    }

    /// The current snapshot.
    pub fn snapshot(&self) -> Snapshot {
        match &self.shared {
            Some(shared) => shared.snapshot.lock().unwrap().clone(),
            None => Snapshot::default(),
        }
    }

    /// `AddUpdateCallback`: after each refresh, `callback` gets one send, waiting for it to be
    /// received; refreshes meanwhile leave at most one more pending.
    pub fn add_update_callback(&self, callback: SyncSender<()>) {
        let Some(shared) = &self.shared else {
            return;
        };
        let (notify, notified) = sync_channel::<()>(1);
        shared.callbacks.lock().unwrap().push(notify);
        std::thread::spawn(move || {
            for () in notified {
                if callback.send(()).is_err() {
                    return;
                }
            }
        });
    }
}

/// `FileProvider.setupRuntime`, configured by the `RUNTIME_*` settings: with
/// `RUNTIME_WATCH_ROOT`, watches `RUNTIME_ROOT` (a symlink) and snapshots
/// `RUNTIME_ROOT/RUNTIME_SUBDIRECTORY`; otherwise watches and snapshots
/// `RUNTIME_ROOT/RUNTIME_SUBDIRECTORY/RUNTIME_APPDIRECTORY`. A directory that cannot be watched
/// is an error (upstream panics).
pub fn loader(settings: &Settings, stats: &Stats) -> Result<Loader, String> {
    let scope = stats.scope_with_tags("runtime");
    if settings.runtime_watch_root {
        let refresher = Refresher::Symlink {
            runtime_path: settings.runtime_root.clone(),
        };
        Loader::new(
            &settings.runtime_root,
            &settings.runtime_subdirectory,
            stats,
            &scope,
            refresher,
            settings.runtime_ignoredotfiles,
        )
    } else {
        let refresher = Refresher::Directory {
            current_dir: String::new(),
            ops: vec![FsOp::Remove, FsOp::Write, FsOp::Create, FsOp::Chmod],
        };
        Loader::new(
            &go::join(&[&settings.runtime_root, &settings.runtime_subdirectory]),
            &settings.runtime_appdirectory,
            stats,
            &scope,
            refresher,
            settings.runtime_ignoredotfiles,
        )
    }
}

/// envoyproxy/ratelimit's `FileProvider`: loads the config from the runtime snapshot at start
/// and after each refresh, and hands each result to `update`. Like upstream's, it runs for the
/// life of the process: dropping it stops nothing.
pub struct FileProvider {
    _loader: Loader,
}

impl FileProvider {
    /// `NewFileProvider`: the [`loader`]'s files (with `RUNTIME_WATCH_ROOT`, those under
    /// `RUNTIME_APPDIRECTORY`), loaded as a config at start and after each refresh.
    pub fn start(
        settings: &Settings,
        stats: &Stats,
        update: impl Fn(Result<RateLimitConfig, ConfigError>) + Send + 'static,
    ) -> Result<Self, String> {
        Ok(Self::with_loader(
            loader(settings, stats)?,
            settings,
            update,
        ))
    }

    /// [`start`](Self::start) with a [`loader`] set up beforehand (upstream sets it up before
    /// connecting to Redis, and loads once the service exists).
    pub fn with_loader(
        loader: Loader,
        settings: &Settings,
        update: impl Fn(Result<RateLimitConfig, ConfigError>) + Send + 'static,
    ) -> Self {
        let watch_root = settings.runtime_watch_root;
        let app_prefix = format!("{}.", settings.runtime_appdirectory);
        let options = settings.load_options();
        let watched = loader.clone();
        std::thread::spawn(move || {
            let (callback, refreshed) = sync_channel::<()>(0);
            watched.add_update_callback(callback);
            let send_event = || {
                let files: Vec<(String, Vec<u8>)> = watched
                    .snapshot()
                    .entries
                    .into_iter()
                    .filter(|(key, _)| !watch_root || key.starts_with(&app_prefix))
                    .collect();
                update(config::load_bytes(&files, &options));
            };
            send_event();
            loop {
                crate::debug!("waiting for runtime update");
                if refreshed.recv().is_err() {
                    break;
                }
                crate::debug!("got runtime update and reloading config");
                send_event();
            }
        });
        Self { _loader: loader }
    }
}

/// An error as Go formats `"%s (%T %#v)"` of a `syscall.Errno`.
fn errno(error: &std::io::Error) -> String {
    let code = error.raw_os_error().unwrap_or(0);
    format!("{} (syscall.Errno {code:#x})", crate::go::errno_text(error))
}

/// An `*os.PathError`'s text: the operation, the path, then the errno's text.
fn path_error(op: &str, path: &OsStr, error: &std::io::Error) -> String {
    format!(
        "{op} {}: {}",
        path.to_string_lossy(),
        crate::go::errno_text(error)
    )
}

#[cfg(target_os = "linux")]
mod watcher {
    //! fsnotify v1.7.0's inotify backend for one directory.

    use super::{Ops, errno};
    use inotify::{EventMask, Inotify, WatchDescriptor, WatchMask};

    pub struct Watcher {
        inotify: Inotify,
        /// The watch and its path, as fsnotify names events (`filepath.Clean` of what was
        /// added); `None` once the watch is gone.
        watch: Option<(WatchDescriptor, String)>,
        buffer: Vec<u8>,
        pending: std::collections::VecDeque<(String, Ops)>,
    }

    impl Watcher {
        pub fn new(path: &str) -> Result<Self, String> {
            let inotify = Inotify::init()
                .map_err(|e| format!("unable to create runtime watcher: {}\n", errno(&e)))?;
            let mask = WatchMask::MOVED_TO
                | WatchMask::MOVED_FROM
                | WatchMask::CREATE
                | WatchMask::ATTRIB
                | WatchMask::MODIFY
                | WatchMask::MOVE_SELF
                | WatchMask::DELETE
                | WatchMask::DELETE_SELF;
            let clean = crate::go::clean(path);
            let descriptor = inotify
                .watches()
                .add(&clean, mask)
                .map_err(|e| format!("unable to watch file ({path}): {}", errno(&e)))?;
            Ok(Self {
                inotify,
                watch: Some((descriptor, clean)),
                buffer: vec![0; 4096 * 64],
                pending: Default::default(),
            })
        }

        /// The next event: its name and operations.
        pub fn next_event(&mut self) -> Option<(String, Ops)> {
            loop {
                if let Some(event) = self.pending.pop_front() {
                    return Some(event);
                }
                // fsnotify reports a read error and goes on reading.
                let Ok(events) = self.inotify.read_events_blocking(&mut self.buffer) else {
                    continue;
                };
                let mut moved = None;
                for event in events {
                    let mask = event.mask;
                    // Events of no known watch (an overflow) are named from "" (`byWd`).
                    let watch = self
                        .watch
                        .as_ref()
                        .filter(|(wd, _)| *wd == event.wd)
                        .cloned();
                    if watch.is_some() && mask.contains(EventMask::DELETE_SELF) {
                        self.watch = None;
                    }
                    if mask.contains(EventMask::MOVE_SELF)
                        && let Some((descriptor, _)) = &watch
                    {
                        moved = Some(descriptor.clone());
                        self.watch = None;
                    }
                    let mut name = watch.map(|(_, path)| path).unwrap_or_default();
                    if let Some(file) = event.name {
                        name = format!("{name}/{}", file.to_string_lossy());
                    }
                    if mask.contains(EventMask::IGNORED) {
                        continue;
                    }
                    let ops = Ops {
                        create: mask.intersects(EventMask::CREATE | EventMask::MOVED_TO),
                        remove: mask.intersects(EventMask::DELETE_SELF | EventMask::DELETE),
                        write: mask.contains(EventMask::MODIFY),
                        rename: mask.intersects(EventMask::MOVE_SELF | EventMask::MOVED_FROM),
                        chmod: mask.contains(EventMask::ATTRIB),
                    };
                    self.pending.push_back((name, ops));
                }
                // A moved directory is no longer watched, as fsnotify removes the watch.
                if let Some(descriptor) = moved {
                    let _ = self.inotify.watches().remove(descriptor);
                }
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod watcher {
    //! A stand-in for development off Linux: polls the watched directory's entries every
    //! 100 milliseconds and reports created, written and removed ones.

    use super::{Ops, errno};
    use std::collections::BTreeMap;
    use std::time::{Duration, SystemTime};

    pub struct Watcher {
        path: String,
        entries: BTreeMap<String, Option<SystemTime>>,
        pending: std::collections::VecDeque<(String, Ops)>,
    }

    /// The entries of a directory, or nothing for a file (which can be watched too).
    fn list(path: &str) -> std::io::Result<BTreeMap<String, Option<SystemTime>>> {
        if !std::fs::metadata(path)?.is_dir() {
            return Ok(BTreeMap::new());
        }
        Ok(std::fs::read_dir(path)?
            .filter_map(Result::ok)
            .map(|entry| {
                let modified = std::fs::symlink_metadata(entry.path())
                    .and_then(|m| m.modified())
                    .ok();
                (entry.file_name().to_string_lossy().into_owned(), modified)
            })
            .collect())
    }

    impl Watcher {
        pub fn new(path: &str) -> Result<Self, String> {
            let entries =
                list(path).map_err(|e| format!("unable to watch file ({path}): {}", errno(&e)))?;
            Ok(Self {
                path: crate::go::clean(path),
                entries,
                pending: Default::default(),
            })
        }

        pub fn next_event(&mut self) -> Option<(String, Ops)> {
            loop {
                if let Some(event) = self.pending.pop_front() {
                    return Some(event);
                }
                std::thread::sleep(Duration::from_millis(100));
                let Ok(current) = list(&self.path) else {
                    continue;
                };
                for (name, modified) in &current {
                    let ops = match self.entries.get(name) {
                        None => Ops {
                            create: true,
                            ..Default::default()
                        },
                        Some(before) if before != modified => Ops {
                            write: true,
                            ..Default::default()
                        },
                        Some(_) => continue,
                    };
                    self.pending
                        .push_back((format!("{}/{name}", self.path), ops));
                }
                for name in self.entries.keys().filter(|n| !current.contains_key(*n)) {
                    let ops = Ops {
                        remove: true,
                        ..Default::default()
                    };
                    self.pending
                        .push_back((format!("{}/{name}", self.path), ops));
                }
                self.entries = current;
            }
        }
    }
}
