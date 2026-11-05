//! A fast per-thread random generator, for trace IDs and expiration jitter: not for secrets.

/// Random bytes from a per-thread generator (splitmix64), seeded from the system (or, if
/// it fails, from the time and the thread).
pub fn fill(out: &mut [u8]) {
    thread_local! {
        static STATE: std::cell::Cell<u64> = std::cell::Cell::new({
            let mut seed = [0u8; 8];
            match getrandom::fill(&mut seed) {
                Ok(()) => u64::from_ne_bytes(seed),
                Err(_) => {
                    use std::hash::BuildHasher;
                    std::collections::hash_map::RandomState::new()
                        .hash_one((crate::random::now_nanos(), std::thread::current().id()))
                }
            }
        });
    }
    for chunk in out.chunks_mut(8) {
        let value = STATE.with(|state| {
            let next = state.get().wrapping_add(0x9e37_79b9_7f4a_7c15);
            state.set(next);
            let mut z = next;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        });
        chunk.copy_from_slice(&value.to_be_bytes()[..chunk.len()]);
    }
}

/// A random number in `[0, n)`, by modulo (slightly biased, unlike `rand.Int63n`'s
/// rejection): `n` must not be 0.
pub fn below(n: u64) -> u64 {
    let mut bytes = [0u8; 8];
    fill(&mut bytes);
    (u64::from_be_bytes(bytes) >> 1) % n
}

/// A random number in `[0, 1)`, like `rand.Float64`.
pub fn float64() -> f64 {
    let mut bytes = [0u8; 8];
    fill(&mut bytes);
    (u64::from_be_bytes(bytes) >> 11) as f64 / (1u64 << 53) as f64
}

pub(crate) fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64)
}
