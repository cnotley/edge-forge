use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use slog::{o, Logger};
use tokio::sync::{mpsc, watch};

use crate::queue::{QueueLogEntry, QueueReplicator};

#[cfg(feature = "raft-consensus")]
use anyhow::Context;
#[cfg(feature = "raft-consensus")]
use std::sync::Mutex;
#[cfg(feature = "raft-consensus")]
use std::time::Duration;

#[derive(Clone, Debug, Default)]
pub struct LeaderInfo {
    pub leader_id: u64,
    pub term: u64,
}

#[derive(Clone)]
pub struct LeaderState {
    node_id: u64,
    receiver: watch::Receiver<LeaderInfo>,
}

impl LeaderState {
    pub fn leader_id(&self) -> u64 {
        self.receiver.borrow().leader_id
    }

    pub fn term(&self) -> u64 {
        self.receiver.borrow().term
    }

    pub fn is_leader(&self) -> bool {
        self.leader_id() == self.node_id
    }

    pub fn snapshot(&self) -> LeaderInfo {
        self.receiver.borrow().clone()
    }

    #[cfg(feature = "raft-consensus")]
    fn new(node_id: u64, receiver: watch::Receiver<LeaderInfo>) -> Self {
        Self { node_id, receiver }
    }

    fn channel(node_id: u64, info: LeaderInfo) -> (watch::Sender<LeaderInfo>, Self) {
        let (tx, rx) = watch::channel(info);
        (
            tx,
            Self {
                node_id,
                receiver: rx,
            },
        )
    }
}

#[cfg(feature = "raft-consensus")]
mod raft_impl {
    use super::*;
    use prost::Message;
    use raft::prelude::*;
    use raft::storage::MemStorage;
    use raft::{Config, RawNode};
    use tokio::time::Interval;

    const TICK_INTERVAL: Duration = Duration::from_millis(100);

    #[derive(Clone)]
    pub struct RaftReplicator {
        proposal_tx: mpsc::Sender<Vec<u8>>,
        logger: Logger,
    }

    impl RaftReplicator {
        fn new(proposal_tx: mpsc::Sender<Vec<u8>>, logger: Logger) -> Self {
            Self {
                proposal_tx,
                logger,
            }
        }
    }

    impl QueueReplicator for RaftReplicator {
        fn replicate(&self, entry: QueueLogEntry) {
            let logger = self.logger.clone();
            match postcard::to_allocvec(&entry) {
                Ok(bytes) => {
                    let tx = self.proposal_tx.clone();
                    tokio::spawn(async move {
                        if let Err(error) = tx.send(bytes).await {
                            slog::warn!(logger, "failed to submit raft proposal"; "error" => %error);
                        }
                    });
                }
                Err(error) => {
                    slog::error!(self.logger, "failed to serialize queue log entry"; "error" => %error);
                }
            }
        }
    }

    pub struct ConsensusNode {
        pub leader: LeaderState,
        pub replicator: Arc<dyn QueueReplicator>,
        pub apply_rx: mpsc::Receiver<QueueLogEntry>,
    }

    pub struct ClusterHandles {
        pub nodes: HashMap<u64, ConsensusNode>,
    }

    #[derive(Clone, Default)]
    struct RaftNetwork {
        peers: Arc<Mutex<HashMap<u64, mpsc::Sender<Message>>>>,
    }

    impl RaftNetwork {
        fn register(&self, id: u64, sender: mpsc::Sender<Message>) {
            let mut guard = self.peers.lock().expect("network registry poisoned");
            guard.insert(id, sender);
        }

        fn send(&self, message: Message, logger: &Logger) {
            let recipient = message.to;
            let guard = self.peers.lock().expect("network registry poisoned");
            if let Some(sender) = guard.get(&recipient) {
                if let Err(error) = sender.try_send(message) {
                    slog::warn!(logger, "raft network send failed"; "peer" => recipient, "error" => %error);
                }
            } else {
                slog::warn!(logger, "raft peer unavailable"; "peer" => recipient);
            }
        }
    }

    pub fn bootstrap(node_ids: &[u64], logger: Logger) -> Result<ClusterHandles> {
        let network = RaftNetwork::default();
        let mut nodes = HashMap::new();

        for node_id in node_ids.iter().copied() {
            let node_logger = logger.new(o!("raft_node" => node_id));
            let peers = node_ids.to_vec();
            let (handle, actor) =
                RaftActor::new(node_id, peers, network.clone(), node_logger.clone())
                    .context("failed to initialize raft node")?;

            network.register(node_id, actor.inbox_sender.clone());

            tokio::spawn(async move {
                if let Err(error) = actor.run().await {
                    slog::error!(node_logger, "raft actor terminated"; "error" => %error);
                }
            });

            nodes.insert(node_id, handle);
        }

        Ok(ClusterHandles { nodes })
    }

    struct RaftActor {
        node_id: u64,
        raw_node: RawNode<MemStorage>,
        network: RaftNetwork,
        inbox: mpsc::Receiver<Message>,
        inbox_sender: mpsc::Sender<Message>,
        proposal_rx: mpsc::Receiver<Vec<u8>>,
        apply_tx: mpsc::Sender<QueueLogEntry>,
        leader_tx: watch::Sender<LeaderInfo>,
        ticker: Interval,
        logger: Logger,
    }

    impl RaftActor {
        fn new(
            node_id: u64,
            peers: Vec<u64>,
            network: RaftNetwork,
            logger: Logger,
        ) -> Result<(ConsensusNode, Self)> {
            let storage = MemStorage::new_with_conf_state(ConfState::from((peers.clone(), vec![])));

            let mut config = Config {
                id: node_id,
                heartbeat_tick: 2,
                election_tick: 10,
                max_size_per_msg: 1024 * 1024,
                max_inflight_msgs: 256,
                ..Default::default()
            };
            config.validate().context("invalid raft config")?;

            let raw_node =
                RawNode::new(&config, storage, &logger).context("failed to create raw node")?;

            let (proposal_tx, proposal_rx) = mpsc::channel(256);
            let replicator: Arc<dyn QueueReplicator> =
                Arc::new(RaftReplicator::new(proposal_tx.clone(), logger.clone()));

            let (apply_tx, apply_rx) = mpsc::channel(256);
            let (leader_tx, leader_rx) = watch::channel(LeaderInfo::default());
            let (inbox_sender, inbox) = mpsc::channel(256);

            let leader = LeaderState::new(node_id, leader_rx);

            let actor = Self {
                node_id,
                raw_node,
                network,
                inbox,
                inbox_sender: inbox_sender.clone(),
                proposal_rx,
                apply_tx,
                leader_tx,
                ticker: tokio::time::interval(TICK_INTERVAL),
                logger,
            };

            let handle = ConsensusNode {
                leader,
                replicator,
                apply_rx,
            };

            Ok((handle, actor))
        }

        async fn run(mut self) -> Result<()> {
            self.raw_node
                .campaign()
                .context("raft campaign start failed")?;

            loop {
                tokio::select! {
                    _ = self.ticker.tick() => {
                        self.raw_node.tick();
                    }
                    Some(message) = self.inbox.recv() => {
                        self.raw_node.step(message).context("raft step failed")?;
                    }
                    Some(proposal) = self.proposal_rx.recv() => {
                        if let Err(error) = self.raw_node.propose(vec![], proposal) {
                            slog::warn!(self.logger, "raft proposal rejected"; "error" => %error);
                        }
                    }
                    else => break,
                }

                self.process_ready().await?;
            }

            Ok(())
        }

        async fn process_ready(&mut self) -> Result<()> {
            if !self.raw_node.has_ready() {
                self.update_leader();
                return Ok(());
            }

            let mut committed_data = Vec::new();

            {
                let mut ready = self.raw_node.ready();

                if !ready.snapshot().is_empty() {
                    self.raw_node
                        .mut_store()
                        .apply_snapshot(ready.snapshot().clone())
                        .context("failed to apply snapshot")?;
                }

                let entries = ready.take_entries();
                if !entries.is_empty() {
                    self.raw_node
                        .mut_store()
                        .append(&entries)
                        .context("failed to append raft entries")?;
                }

                for message in ready.take_messages() {
                    self.network.send(message, &self.logger);
                }

                if let Some(entries) = ready.take_committed_entries() {
                    for entry in entries {
                        if entry.data.is_empty() {
                            continue;
                        }

                        match entry.entry_type() {
                            EntryType::EntryNormal => {
                                committed_data.push(entry.data.clone());
                            }
                            EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => {
                                let cc = ConfChangeV2::decode(entry.data.as_slice())
                                    .context("conf change decode failed")?;
                                let cs = self
                                    .raw_node
                                    .apply_conf_change(&cc)
                                    .context("conf change apply failed")?;
                                self.raw_node.mut_store().set_conf_state(cs);
                            }
                            EntryType::EntrySnapshot => {}
                        }
                    }
                }

                if let Some(hard_state) = ready.hs() {
                    self.raw_node.mut_store().set_hardstate(hard_state.clone());
                }

                self.raw_node.advance(ready);
            }

            for data in committed_data {
                match postcard::from_bytes::<QueueLogEntry>(&data) {
                    Ok(entry) => {
                        if let Err(error) = self.apply_tx.send(entry).await {
                            slog::warn!(self.logger, "apply channel closed"; "error" => %error);
                        }
                    }
                    Err(error) => {
                        slog::warn!(self.logger, "failed to decode queue log entry"; "error" => %error);
                    }
                }
            }

            self.update_leader();

            Ok(())
        }

        fn update_leader(&self) {
            let leader_id = self.raw_node.raft.leader_id;
            let term = self.raw_node.raft.term;
            let info = LeaderInfo { leader_id, term };
            if self.leader_tx.borrow().leader_id != leader_id
                || self.leader_tx.borrow().term != term
            {
                let _ = self.leader_tx.send(info);
            }
        }
    }
}

#[cfg(feature = "raft-consensus")]
pub use raft_impl::{bootstrap, ClusterHandles, ConsensusNode};

#[cfg(not(feature = "raft-consensus"))]
mod stub_impl {
    use super::*;
    use rand::seq::SliceRandom;
    use rand::thread_rng;

    const APPLY_CHANNEL_CAPACITY: usize = 256;

    #[derive(Clone)]
    struct StubReplicator {
        peers: Arc<Vec<mpsc::Sender<QueueLogEntry>>>,
        logger: Logger,
    }

    impl StubReplicator {
        fn new(peers: Arc<Vec<mpsc::Sender<QueueLogEntry>>>, logger: Logger) -> Self {
            Self { peers, logger }
        }
    }

    impl QueueReplicator for StubReplicator {
        fn replicate(&self, entry: QueueLogEntry) {
            for sender in self.peers.iter() {
                let sender = sender.clone();
                let logger = self.logger.clone();
                let entry = entry.clone();
                tokio::spawn(async move {
                    if let Err(error) = sender.send(entry).await {
                        slog::warn!(logger, "stub replication failed"; "error" => %error);
                    }
                });
            }
        }
    }

    pub struct ConsensusNode {
        pub leader: LeaderState,
        pub replicator: Arc<dyn QueueReplicator>,
        pub apply_rx: mpsc::Receiver<QueueLogEntry>,
    }

    pub struct ClusterHandles {
        pub nodes: HashMap<u64, ConsensusNode>,
    }

    pub fn bootstrap(node_ids: &[u64], logger: Logger) -> Result<ClusterHandles> {
        if node_ids.is_empty() {
            return Ok(ClusterHandles {
                nodes: HashMap::new(),
            });
        }

        let leader_id = *node_ids.choose(&mut thread_rng()).unwrap_or(&node_ids[0]);
        let initial_info = LeaderInfo { leader_id, term: 1 };

        let mut apply_channels = Vec::new();
        let mut leader_channels = Vec::new();

        for &node_id in node_ids {
            let (apply_tx, apply_rx) = mpsc::channel(APPLY_CHANNEL_CAPACITY);
            let (leader_tx, leader_state) = LeaderState::channel(node_id, initial_info.clone());
            apply_channels.push((node_id, apply_tx, apply_rx));
            leader_channels.push((node_id, leader_tx, leader_state));
        }

        let peer_senders = Arc::new(
            apply_channels
                .iter()
                .map(|(_, tx, _)| tx.clone())
                .collect::<Vec<_>>(),
        );

        let mut nodes = HashMap::new();

        for ((node_id, _, apply_rx), (_, _, leader_state)) in
            apply_channels.into_iter().zip(leader_channels.into_iter())
        {
            let replicator: Arc<dyn QueueReplicator> = Arc::new(StubReplicator::new(
                peer_senders.clone(),
                logger.new(o!("stub_node" => node_id)),
            ));

            nodes.insert(
                node_id,
                ConsensusNode {
                    leader: leader_state,
                    replicator,
                    apply_rx,
                },
            );
        }

        Ok(ClusterHandles { nodes })
    }
}

#[cfg(not(feature = "raft-consensus"))]
pub use stub_impl::{bootstrap, ClusterHandles, ConsensusNode};
