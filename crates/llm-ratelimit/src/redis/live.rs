//! A pool whose decisions fail at once while every connection is down. fred keeps a client
//! `Connected` while it reconnects and holds the commands sent meanwhile until Redis is back,
//! as radix's `WAIT` pool does once its dead connections are gone; a caller without a deadline
//! (agentgateway's remote rate limit) would then wait for the length of the outage instead of
//! applying its failure mode (docs/parity.md §7).

use crate::service::CacheError;
use fred::prelude::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::broadcast::error::RecvError;

/// Each request's pipeline on the next client whose connection is up, as fred's `Pool::next`
/// round robin.
pub struct LivePool {
    pool: Pool,
    /// Whether each client of `pool` has lost its connection and not reconnected yet.
    down: Arc<[AtomicBool]>,
    next: AtomicUsize,
}

impl LivePool {
    /// Follows each client's connection: down on a connection error, up once reconnected. A
    /// cluster client's error is one node's, so a cluster's clients are never down: a
    /// decision on a node that is down waits for it, as before.
    pub fn new(pool: Pool) -> Self {
        let down: Arc<[AtomicBool]> = pool
            .clients()
            .iter()
            .map(|_| AtomicBool::new(false))
            .collect();
        for (index, client) in pool.clients().iter().enumerate() {
            if !client.is_clustered() {
                tokio::spawn(follow(client.clone(), down.clone(), index));
            }
        }
        Self {
            pool,
            down,
            next: AtomicUsize::new(0),
        }
    }

    /// The next client whose connection is up, or radix's error for a closed connection when
    /// none is.
    pub fn next(&self) -> Result<&Client, CacheError> {
        let clients = self.pool.clients();
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        (0..clients.len())
            .map(|offset| (start + offset) % clients.len())
            .find(|index| !self.down[*index].load(Ordering::Relaxed))
            .map(|index| &clients[index])
            .ok_or_else(|| CacheError("response returned from Conn: EOF".into()))
    }
}

/// Marks `client` down on each connection error and up on each reconnection, in the order
/// fred sent them: one task reads both, errors first when both are waiting, since a
/// reconnection always follows the error it recovers from.
async fn follow(client: Client, down: Arc<[AtomicBool]>, index: usize) {
    let (mut errors, mut reconnections) = (client.error_rx(), client.reconnect_rx());
    loop {
        let lost = tokio::select! {
            biased;
            error = errors.recv() => match error {
                Ok(_) => true,
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return,
            },
            reconnection = reconnections.recv() => match reconnection {
                Ok(_) => false,
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return,
            },
        };
        down[index].store(lost, Ordering::Relaxed);
    }
}
