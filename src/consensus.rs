use std::sync::Arc;
use std::time::Duration;

use rand::seq::SliceRandom;
use rand::thread_rng;
use slog::Logger;
use tokio::sync::watch;

const LEADER_ELECTION_INTERVAL: Duration = Duration::from_secs(3);

#[derive(Clone)]
pub struct LeaderState {
    receiver: watch::Receiver<String>,
}

impl LeaderState {
    pub fn snapshot(&self) -> String {
        self.receiver.borrow().clone()
    }
}

struct ConsensusActor {
    nodes: Arc<Vec<String>>,
    sender: watch::Sender<String>,
    current_leader: String,
    logger: Logger,
}

impl ConsensusActor {
    fn new(nodes: Vec<String>, logger: Logger) -> (Self, LeaderState) {
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
            },
            LeaderState { receiver },
        )
    }

    async fn run(mut self) {
        loop {
            tokio::time::sleep(LEADER_ELECTION_INTERVAL).await;

            if let Some(candidate) = self.nodes.choose(&mut thread_rng()).cloned() {
                if candidate != self.current_leader {
                    self.current_leader = candidate.clone();
                    if self.sender.send(candidate.clone()).is_ok() {
                        slog::info!(self.logger, "leader updated"; "leader" => candidate);
                    }
                }
            }
        }
    }
}

pub fn spawn(nodes: Vec<String>, logger: Logger) -> LeaderState {
    let (actor, state) = ConsensusActor::new(nodes, logger);
    tokio::spawn(actor.run());
    state
}
