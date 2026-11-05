//! client_golang v1.19.1's summary (`summary.go`) and the quantile estimate it keeps,
//! beorn7/perks v1.0.1's targeted CKMS stream (`quantile/stream.go`).

/// A sample of the stream: a value, how many observations it stands for, and its error.
#[derive(Debug, Clone, Copy)]
struct Sample {
    value: f64,
    width: f64,
    delta: f64,
}

/// `quantile.Stream` made with `NewTargeted`: observations buffered (500), then merged into
/// a compressed list of samples.
#[derive(Debug, Clone)]
struct Stream {
    /// (quantile, epsilon) of each objective.
    targets: Vec<(f64, f64)>,
    n: f64,
    list: Vec<Sample>,
    buffer: Vec<Sample>,
    sorted: bool,
}

const STREAM_BUFFER: usize = 500;

impl Stream {
    fn new(targets: &[(f64, f64)]) -> Self {
        Self {
            targets: targets.to_vec(),
            n: 0.0,
            list: Vec::new(),
            buffer: Vec::with_capacity(STREAM_BUFFER),
            sorted: true,
        }
    }

    /// The targeted invariant: the smallest allowed error at rank `r`.
    fn invariant(&self, r: f64) -> f64 {
        let mut m = f64::MAX;
        for &(quantile, epsilon) in &self.targets {
            let f = if quantile * self.n <= r {
                (2.0 * epsilon * r) / quantile
            } else {
                (2.0 * epsilon * (self.n - r)) / (1.0 - quantile)
            };
            if f < m {
                m = f;
            }
        }
        m
    }

    fn insert(&mut self, value: f64) {
        self.buffer.push(Sample {
            value,
            width: 1.0,
            delta: 0.0,
        });
        self.sorted = false;
        if self.buffer.len() == STREAM_BUFFER {
            self.flush();
        }
    }

    fn query(&mut self, q: f64) -> f64 {
        if !self.flushed() {
            let l = self.buffer.len();
            if l == 0 {
                return 0.0;
            }
            let mut i = (l as f64 * q).ceil() as i64;
            if i > 0 {
                i -= 1;
            }
            self.maybe_sort();
            // An objective outside [0, 1] makes Go index out of range (docs/parity.md §7).
            return self.buffer[i.clamp(0, l as i64 - 1) as usize].value;
        }
        self.flush();
        self.query_list(q)
    }

    fn reset(&mut self) {
        self.list.clear();
        self.n = 0.0;
        self.buffer.clear();
    }

    fn count(&self) -> usize {
        self.buffer.len() + self.n as usize
    }

    fn flush(&mut self) {
        self.maybe_sort();
        let buffer = std::mem::take(&mut self.buffer);
        self.merge(&buffer);
        self.buffer = buffer;
        self.buffer.clear();
    }

    fn maybe_sort(&mut self) {
        if !self.sorted {
            self.sorted = true;
            // Go sorts by value only; equal values are alike.
            self.buffer.sort_by(|a, b| {
                a.value
                    .partial_cmp(&b.value)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }
    }

    fn flushed(&self) -> bool {
        !self.list.is_empty()
    }

    /// `stream.merge` of sorted samples.
    fn merge(&mut self, samples: &[Sample]) {
        let mut r = 0.0;
        let mut i = 0;
        for sample in samples {
            let mut inserted = false;
            while i < self.list.len() {
                let c = self.list[i];
                if c.value > sample.value {
                    let delta = sample.delta.max((self.invariant(r)).floor() - 1.0);
                    self.list.insert(
                        i,
                        Sample {
                            value: sample.value,
                            width: sample.width,
                            delta,
                        },
                    );
                    i += 1;
                    inserted = true;
                    break;
                }
                r += c.width;
                i += 1;
            }
            if !inserted {
                self.list.push(Sample {
                    value: sample.value,
                    width: sample.width,
                    delta: 0.0,
                });
                i += 1;
            }
            self.n += sample.width;
            r += sample.width;
        }
        self.compress();
    }

    fn query_list(&self, q: f64) -> f64 {
        let mut t = (q * self.n).ceil();
        t += (self.invariant(t) / 2.0).ceil();
        let mut p = self.list[0];
        let mut r = 0.0;
        for c in &self.list[1..] {
            r += p.width;
            if r + c.width + c.delta > t {
                return p.value;
            }
            p = *c;
        }
        p.value
    }

    fn compress(&mut self) {
        if self.list.len() < 2 {
            return;
        }
        let mut x = self.list[self.list.len() - 1];
        let mut xi = self.list.len() - 1;
        let mut r = self.n - 1.0 - x.width;
        for i in (0..self.list.len() - 1).rev() {
            let c = self.list[i];
            if c.width + x.width + x.delta <= self.invariant(r) {
                x.width += c.width;
                self.list[xi] = x;
                self.list.remove(i);
                xi -= 1;
            } else {
                x = c;
                xi = i;
            }
            r -= c.width;
        }
    }
}

/// The options of a summary, after client_golang's defaults (`MaxAge` 10 minutes, 5 age
/// buckets, a buffer of 500); `max_age` in nanoseconds, at least one per age bucket.
#[derive(Debug, Clone, Copy)]
pub struct Options {
    pub max_age: i64,
    pub age_buckets: u32,
    pub buf_cap: u32,
}

/// A summary with objectives: observations buffered, then inserted into one stream per age
/// bucket; quantiles come from the oldest stream still in use. Times are nanoseconds on a
/// monotonic clock, as Go's `time.Time` comparisons use.
#[derive(Debug, Clone)]
pub struct Summary {
    sorted_objectives: Vec<f64>,
    sum: f64,
    count: u64,
    hot_buf: Vec<f64>,
    cold_buf: Vec<f64>,
    buf_cap: usize,
    streams: Vec<Stream>,
    stream_duration: i64,
    head_stream: usize,
    head_stream_exp_time: i64,
    hot_buf_exp_time: i64,
}

/// What `Write` gives: the count, the sum, and each objective's quantile, by objective.
#[derive(Debug, Clone, PartialEq)]
pub struct Written {
    pub count: u64,
    pub sum: f64,
    pub quantiles: Vec<(f64, f64)>,
}

impl Summary {
    /// `newSummary`, with `objectives` (quantile → error, as Go's map keeps them) not empty.
    pub fn new(objectives: &[(f64, f64)], options: Options, now: i64) -> Self {
        let stream_duration = options.max_age / options.age_buckets as i64;
        let streams = (0..options.age_buckets)
            .map(|_| Stream::new(objectives))
            .collect();
        let mut sorted_objectives: Vec<f64> = objectives.iter().map(|(q, _)| *q).collect();
        // `sort.Float64s`: NaN first.
        sorted_objectives.sort_by(|a, b| match (a.is_nan(), b.is_nan()) {
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            _ => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
        });
        let buf_cap = options.buf_cap as usize;
        Self {
            sorted_objectives,
            sum: 0.0,
            count: 0,
            hot_buf: Vec::with_capacity(buf_cap),
            cold_buf: Vec::with_capacity(buf_cap),
            buf_cap,
            streams,
            stream_duration,
            head_stream: 0,
            head_stream_exp_time: now + stream_duration,
            hot_buf_exp_time: now + stream_duration,
        }
    }

    /// `Observe`: buffered, and flushed when the buffer expires or fills.
    pub fn observe(&mut self, v: f64, now: i64) {
        if now > self.hot_buf_exp_time {
            self.async_flush(now);
        }
        self.hot_buf.push(v);
        if self.hot_buf.len() == self.buf_cap {
            self.async_flush(now);
        }
    }

    /// `Write`: the buffers flushed, then the head stream's quantiles (NaN when it is empty).
    pub fn write(&mut self, now: i64) -> Written {
        self.swap_bufs(now);
        self.flush_cold_buf();
        let head = self.head_stream;
        let quantiles = self
            .sorted_objectives
            .clone()
            .into_iter()
            .map(|rank| {
                let stream = &mut self.streams[head];
                let q = match stream.count() {
                    0 => f64::NAN,
                    _ => stream.query(rank),
                };
                (rank, q)
            })
            .collect();
        Written {
            count: self.count,
            sum: self.sum,
            quantiles,
        }
    }

    /// `asyncFlush`, done at once: nothing else touches the summary meanwhile here.
    fn async_flush(&mut self, now: i64) {
        self.swap_bufs(now);
        self.flush_cold_buf();
    }

    fn maybe_rotate_streams(&mut self) {
        while self.hot_buf_exp_time != self.head_stream_exp_time {
            self.streams[self.head_stream].reset();
            self.head_stream += 1;
            if self.head_stream >= self.streams.len() {
                self.head_stream = 0;
            }
            self.head_stream_exp_time += self.stream_duration;
        }
    }

    fn flush_cold_buf(&mut self) {
        for &v in &self.cold_buf {
            for stream in &mut self.streams {
                stream.insert(v);
            }
            self.count += 1;
            self.sum += v;
        }
        self.cold_buf.clear();
        self.maybe_rotate_streams();
    }

    fn swap_bufs(&mut self, now: i64) {
        std::mem::swap(&mut self.hot_buf, &mut self.cold_buf);
        while now > self.hot_buf_exp_time {
            self.hot_buf_exp_time += self.stream_duration;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINUTE: i64 = 60_000_000_000;

    fn options() -> Options {
        Options {
            max_age: 10 * MINUTE,
            age_buckets: 5,
            buf_cap: 500,
        }
    }

    /// Observations leave the quantiles once every age bucket has turned over (`MaxAge`),
    /// but stay in the count and the sum.
    #[test]
    fn observations_age_out_of_the_quantiles() {
        let mut summary = Summary::new(&[(0.5, 0.05)], options(), 0);
        summary.observe(3.0, 0);
        assert_eq!(summary.write(MINUTE).quantiles, vec![(0.5, 3.0)]);
        // Each 2-minute bucket that passes resets the head stream; after 10 minutes the
        // observation has left them all.
        assert_eq!(summary.write(9 * MINUTE).quantiles, vec![(0.5, 3.0)]);
        let written = summary.write(11 * MINUTE);
        assert!(written.quantiles[0].1.is_nan(), "{written:?}");
        assert_eq!((written.count, written.sum), (1, 3.0));
    }

    /// An objective outside [0, 1] (Go indexes out of range) gives the nearest observation;
    /// NaN objectives come first, as `sort.Float64s` puts them.
    #[test]
    fn odd_objectives_do_not_panic() {
        let mut summary = Summary::new(&[(1.5, 0.01), (-1.0, 0.01), (f64::NAN, 0.1)], options(), 0);
        summary.observe(3.0, 0);
        summary.observe(5.0, 0);
        let quantiles = summary.write(0).quantiles;
        assert!(quantiles[0].0.is_nan());
        assert_eq!(&quantiles[1..], &[(-1.0, 3.0), (1.5, 5.0)]);
    }

    /// A buffer that expires is flushed on the next observation, at its own time.
    #[test]
    fn an_expired_buffer_is_flushed_on_the_next_observation() {
        let mut summary = Summary::new(&[(0.5, 0.05), (0.9, 0.01)], options(), 0);
        summary.observe(1.0, 0);
        summary.observe(2.0, 3 * MINUTE);
        let written = summary.write(3 * MINUTE);
        assert_eq!(written.count, 2);
        // The first value went into every stream before the head one was reset: the next
        // head has both.
        assert_eq!(written.quantiles, vec![(0.5, 1.0), (0.9, 2.0)]);
    }
}
