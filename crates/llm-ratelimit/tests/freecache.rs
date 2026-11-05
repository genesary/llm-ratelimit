//! The local cache against the real freecache (`tools/oracle/freecache`): the same random
//! operations replayed on both, every `Get` compared, and the counts upstream reports as
//! `localcache.*` gauges. Covers eviction paths the service-level
//! differential tests cannot reach: expired entries, overwrites, crowded segments and slots,
//! keys around the size limit, and the 32-bit clock wrap.
//!
//! Needs `go`; skipped without it unless `RATELIMIT_REQUIRE_DIFFERENTIAL` is set.

use llm_ratelimit::local_cache::LocalCache;
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use xxhash_rust::xxh64::xxh64;

/// splitmix64: a small deterministic generator, so failures replay.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

enum Op {
    Set {
        key: String,
        ttl: i64,
        now: i64,
    },
    Get {
        key: String,
        now: i64,
    },
    /// The cache's counts.
    Counts,
}

struct Trace {
    size: usize,
    ops: Vec<Op>,
}

/// `mode`: 0 any key, 1 most keys in segment 0, 2 segment 0 and a few slots, 3 clock wrapping
/// past 2^32.
fn trace(seed: u64, mode: u64) -> Trace {
    const LETTERS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = Rng(seed * 4 + mode);
    let sizes = [1, 600_000, 524_288 + 256 * 7, 2 * 1024 * 1024];
    let size = sizes[(seed % 4) as usize];

    let pool_size = 400 + rng.below(800) as usize;
    let mut pool = Vec::with_capacity(pool_size);
    while pool.len() < pool_size {
        let len = match rng.below(4) {
            0 => 1 + rng.below(10),
            1 => 10 + rng.below(60),
            2 => 60 + rng.below(300),
            // Around the largest entry a 2 KiB segment accepts.
            _ => 470 + rng.below(30),
        };
        let key: String = (0..len)
            .map(|_| LETTERS[rng.below(LETTERS.len() as u64) as usize] as char)
            .collect();
        let hash = xxh64(key.as_bytes(), 0);
        if mode >= 1 && hash & 255 != 0 && rng.below(20) != 0 {
            continue;
        }
        if mode == 2 && (hash >> 8) & 3 != 0 {
            continue;
        }
        pool.push(key);
    }

    let mut now: i64 = if mode == 3 {
        (1 << 32) - 300
    } else {
        1_700_000_000
    };
    let mut ops = Vec::new();
    for _ in 0..4000 {
        match rng.below(50) {
            0 => now += rng.below(30) as i64,
            1..=5 => now += 1,
            _ => {}
        }
        let key = if rng.below(10) < 3 {
            // Hot keys.
            pool[rng.below(pool.len() as u64 / 8 + 1) as usize].clone()
        } else {
            pool[rng.below(pool.len() as u64) as usize].clone()
        };
        if rng.below(2) == 0 {
            let mut ttl = 1 + rng.below(40) as i64;
            if rng.below(10) == 0 {
                ttl = rng.below(3) as i64 - 1; // 0 and negative: no expiry
            }
            ops.push(Op::Set {
                key: key.clone(),
                ttl,
                now,
            });
            if rng.below(5) == 0 {
                ops.push(Op::Set {
                    key,
                    ttl: ttl + 1,
                    now,
                }); // overwrite
            }
        } else {
            ops.push(Op::Get { key, now });
        }
        if ops.len() % 500 == 0 {
            ops.push(Op::Counts);
        }
    }
    ops.push(Op::Counts);
    Trace { size, ops }
}

/// What each `Get` and `Counts` gives, as the oracle prints it.
fn replay_in_rust(trace: &Trace) -> Vec<String> {
    let cache = LocalCache::new(trace.size);
    trace
        .ops
        .iter()
        .filter_map(|op| match op {
            Op::Set { key, ttl, now } => {
                cache.insert(key, *ttl, *now);
                None
            }
            Op::Get { key, now } => Some(if cache.contains(key, *now) { "1" } else { "0" }.into()),
            Op::Counts => {
                let c = cache.counts();
                Some(format!(
                    "C {} {} {} {} {} {} {} {}",
                    c.evacuate,
                    c.expired,
                    c.entries,
                    c.average_access_time,
                    c.hits,
                    c.misses,
                    c.lookups,
                    c.overwrites
                ))
            }
        })
        .collect()
}

fn replay_in_freecache(traces: &[Trace]) -> Vec<String> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let binary = root.join("target/freecache-oracle");
    let status = Command::new("go")
        .args(["build", "-o"])
        .arg(&binary)
        .arg("./freecache")
        .current_dir(root.join("tools/oracle"))
        .status()
        .expect("run go build");
    assert!(status.success(), "go build failed");

    let mut input = String::new();
    for trace in traces {
        input += &format!("N {}\n", trace.size);
        for op in &trace.ops {
            match op {
                Op::Set { key, ttl, now } => input += &format!("S {key} {ttl} {now}\n"),
                Op::Get { key, now } => input += &format!("G {key} {now}\n"),
                Op::Counts => input += "C\n",
            }
        }
    }
    let mut child = Command::new(&binary)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("start the freecache oracle");
    let mut stdin = child.stdin.take().unwrap();
    let writer = std::thread::spawn(move || stdin.write_all(input.as_bytes()).unwrap());
    let output = child.wait_with_output().unwrap();
    writer.join().unwrap();
    assert!(output.status.success(), "freecache oracle failed");
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(String::from)
        .collect()
}

#[test]
fn local_cache_matches_freecache_on_random_operations() {
    let go = Command::new("go")
        .arg("version")
        .stdout(Stdio::null())
        .status()
        .is_ok();
    if !go {
        assert!(
            std::env::var_os("RATELIMIT_REQUIRE_DIFFERENTIAL").is_none(),
            "this test needs `go`"
        );
        eprintln!("skipping: this test needs `go`");
        return;
    }
    let traces: Vec<Trace> = (0..40)
        .flat_map(|seed| (0..4).map(move |mode| trace(seed, mode)))
        .collect();
    let expected = replay_in_freecache(&traces);
    let mut expected = expected.into_iter();
    for (i, trace) in traces.iter().enumerate() {
        let actual = replay_in_rust(trace);
        let upstream: Vec<String> = expected.by_ref().take(actual.len()).collect();
        let first_difference = actual.iter().zip(&upstream).position(|(a, u)| a != u);
        assert_eq!(
            first_difference.map(|at| (&actual[at], &upstream[at])),
            None,
            "trace {i} (seed {}, mode {}): first differing Get or counts",
            i / 4,
            i % 4
        );
        assert_eq!(actual.len(), upstream.len(), "trace {i}: number of Gets");
    }
    assert!(expected.next().is_none(), "freecache answered more");
}
