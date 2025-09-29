use std::sync::Arc;
use std::time::Duration;

use rand::seq::SliceRandom;
use rand::thread_rng;
use slog::Logger;
use crate::metrics::Metrics;
use tokio::sync::watch;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
const ELECTION_INTERVAL: Duration = Duration::from_secs(3);

#[derive(Clone)]
pub struct LeaderState {
    receiver: watch::Receiver<String>,
}

impl LeaderState {
    pub fn snapshot(&self) -> String {
        self.receiver.borrow().clone()
    }

    pub fn is_leader(&self, my_id: &str) -> bool {
        self.snapshot() == my_id
    }
}

#[derive(Clone)]
pub struct Replicator;

impl Replicator {
    pub fn new() -> Self {
        Self
    }

    // In a real implementation, this would append to the Raft log and await commit.
    // For now, it's a stub to satisfy replication hooks.
    pub async fn replicate(&self, _op: Vec<u8>) -> anyhow::Result<()> {
        Ok(())
    }
}

struct ConsensusActor {
    nodes: Arc<Vec<String>>,
    sender: watch::Sender<String>,
    current_leader: String,
    logger: Logger,
    metrics: Option<Metrics>,
}

impl ConsensusActor {
    fn new(nodes: Vec<String>, logger: Logger, metrics: Option<Metrics>) -> (Self, LeaderState) {
        let initial_leader = nodes
            .first()
            .cloned()
            .unwrap_or_else(|| "bootstrap".to_string());

        let (sender, receiver) = watch::channel(initial_leader.clone());

        (
            Self {
                nodes: Arc::new(nodes),
                sender,
                current_leader: initial_leader,
                logger,
                metrics,
            },
            LeaderState { receiver },
        )
    }

    async fn run(mut self) {
        loop {
            tokio::time::sleep(HEARTBEAT_INTERVAL).await;
            // Heartbeat tick; occasionally re-elect for simulation
            if (rand::random::<u8>() % 3) == 0 {
                if let Some(candidate) = self.nodes.choose(&mut thread_rng()).cloned() {
                    if candidate != self.current_leader {
                        self.current_leader = candidate.clone();
                        if self.sender.send(candidate.clone()).is_ok() {
                            slog::info!(self.logger, "leader updated"; "leader" => candidate);
                            if let Some(m) = &self.metrics {
                                m.observe_election();
                            }
                        }
                    }
                }
            }
        }
    }
}

pub fn spawn(nodes: Vec<String>, logger: Logger, metrics: Option<Metrics>) -> LeaderState {
    let (actor, state) = ConsensusActor::new(nodes, logger, metrics);
    tokio::spawn(actor.run());
    state
}

pub fn spawn_cluster(nodes: Vec<String>, logger: Logger, metrics: Option<Metrics>) -> (LeaderState, Replicator) {
    let state = spawn(nodes, logger, metrics);
    (state, Replicator::new())
}
