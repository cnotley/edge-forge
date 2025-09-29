use edgeforge::consensus::{self, LeaderInfo, LeaderState};
use edgeforge::coordinator::{self, CoordinatorContext, COORDINATOR_PORT};
use edgeforge::metrics::Metrics;
use edgeforge::queue::{Task, TaskKind, TaskQueue};
use edgeforge::worker::{Worker, WorkerConfig, DEFAULT_BATCH_SIZE};

use anyhow::{anyhow, bail, Context, Result};
use rand::Rng;
use slog::{o, Drain, Logger};
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::signal;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;

const DEFAULT_SIM_NODES: usize = 3;
const DEFAULT_SIM_WORKERS: usize = 4;
const DEFAULT_SEED_TASKS: usize = 16;
const DEFAULT_SIM_DURATION_SECS: u64 = 10;
const DEFAULT_IO_BATCH_SIZE: usize = 3;

enum Command {
    Simulate(SimulateOptions),
    Worker(WorkerOptions),
    Coordinator(CoordinatorOptions),
    Scale(ScaleOptions),
}

#[derive(Debug, Clone)]
struct SimulateOptions {
    nodes: usize,
    worker_count: usize,
    seed_tasks: usize,
    duration: Duration,
    compute_batch: usize,
    io_batch: usize,
}

impl Default for SimulateOptions {
    fn default() -> Self {
        Self {
            nodes: DEFAULT_SIM_NODES,
            worker_count: DEFAULT_SIM_WORKERS,
            seed_tasks: DEFAULT_SEED_TASKS,
            duration: Duration::from_secs(DEFAULT_SIM_DURATION_SECS),
            compute_batch: DEFAULT_BATCH_SIZE,
            io_batch: DEFAULT_IO_BATCH_SIZE,
        }
    }
}

impl SimulateOptions {
    fn parse(args: &[String]) -> Result<Self> {
        let mut opts = Self::default();
        let mut iter = args.iter();

        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--nodes" | "-n" => {
                    let value = iter.next().context("missing value for --nodes")?;
                    opts.nodes = value
                        .parse::<usize>()
                        .context("invalid value for --nodes")?;
                }
                "--workers" | "-w" => {
                    let value = iter.next().context("missing value for --workers")?;
                    opts.worker_count = value
                        .parse::<usize>()
                        .context("invalid value for --workers")?;
                }
                "--seed" => {
                    let value = iter.next().context("missing value for --seed")?;
                    opts.seed_tasks = value.parse::<usize>().context("invalid value for --seed")?;
                }
                "--duration" => {
                    let value = iter.next().context("missing value for --duration")?;
                    opts.duration = Duration::from_secs(
                        value
                            .parse::<u64>()
                            .context("invalid value for --duration")?,
                    );
                }
                "--compute-batch" => {
                    let value = iter.next().context("missing value for --compute-batch")?;
                    opts.compute_batch = clamp_batch(
                        value
                            .parse::<usize>()
                            .context("invalid value for --compute-batch")?,
                    );
                }
                "--io-batch" => {
                    let value = iter.next().context("missing value for --io-batch")?;
                    opts.io_batch = clamp_batch(
                        value
                            .parse::<usize>()
                            .context("invalid value for --io-batch")?,
                    );
                }
                value if !value.starts_with('-') => {
                    opts.nodes = value
                        .parse::<usize>()
                        .context("invalid positional nodes value")?;
                }
                unknown => bail!("unknown simulate option '{unknown}'"),
            }
        }

        if opts.nodes == 0 {
            bail!("node count must be greater than zero");
        }
        if opts.worker_count == 0 {
            bail!("worker count must be greater than zero");
        }

        Ok(opts)
    }
}

#[derive(Debug, Clone)]
struct WorkerOptions {
    id: Option<String>,
    hint: Option<String>,
    service: Option<String>,
    capability: TaskKind,
    batch_size: usize,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            id: None,
            hint: None,
            service: None,
            capability: TaskKind::Compute,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }
}

impl WorkerOptions {
    fn parse(args: &[String]) -> Result<Self> {
        let mut opts = Self::default();
        let mut iter = args.iter();

        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--id" => {
                    opts.id = Some(iter.next().context("missing value for --id")?.clone());
                }
                "--hint" => {
                    opts.hint = Some(iter.next().context("missing value for --hint")?.clone());
                }
                "--service" => {
                    opts.service =
                        Some(iter.next().context("missing value for --service")?.clone());
                }
                "--capability" => {
                    let value = iter.next().context("missing value for --capability")?;
                    opts.capability = parse_task_kind(value)?;
                }
                "--batch" => {
                    let value = iter.next().context("missing value for --batch")?;
                    opts.batch_size = clamp_batch(
                        value
                            .parse::<usize>()
                            .context("invalid value for --batch")?,
                    );
                }
                value if !value.starts_with('-') && opts.id.is_none() => {
                    opts.id = Some(value.to_string());
                }
                unknown => bail!("unknown worker option '{unknown}'"),
            }
        }

        Ok(opts)
    }
}

#[derive(Debug, Clone)]
struct CoordinatorOptions {
    node_id: u64,
    port: u16,
    seed_tasks: usize,
}

impl Default for CoordinatorOptions {
    fn default() -> Self {
        Self {
            node_id: 1,
            port: COORDINATOR_PORT,
            seed_tasks: 0,
        }
    }
}

impl CoordinatorOptions {
    fn parse(args: &[String]) -> Result<Self> {
        let mut opts = Self::default();
        let mut iter = args.iter();

        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--id" => {
                    let value = iter.next().context("missing value for --id")?;
                    opts.node_id = value.parse::<u64>().context("invalid value for --id")?;
                }
                "--port" => {
                    let value = iter.next().context("missing value for --port")?;
                    opts.port = value.parse::<u16>().context("invalid value for --port")?;
                }
                "--seed" => {
                    let value = iter.next().context("missing value for --seed")?;
                    opts.seed_tasks = value.parse::<usize>().context("invalid value for --seed")?;
                }
                value if !value.starts_with('-') && opts.node_id == 1 => {
                    opts.node_id = value.parse::<u64>().context("invalid positional node id")?;
                }
                unknown => bail!("unknown coordinator option '{unknown}'"),
            }
        }

        Ok(opts)
    }
}

#[derive(Debug, Clone)]
struct ScaleOptions {
    nodes: usize,
    workers: usize,
    seed_tasks: usize,
    duration: Duration,
    target_throughput: Option<f64>,
}

impl Default for ScaleOptions {
    fn default() -> Self {
        Self {
            nodes: 100,
            workers: 400,
            seed_tasks: 100_000,
            duration: Duration::from_secs(60),
            target_throughput: Some(1_000.0),
        }
    }
}

impl ScaleOptions {
    fn parse(args: &[String]) -> Result<Self> {
        let mut opts = Self::default();
        let mut iter = args.iter();

        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--nodes" | "-n" => {
                    let value = iter.next().context("missing value for --nodes")?;
                    opts.nodes = value
                        .parse::<usize>()
                        .context("invalid value for --nodes")?;
                }
                "--workers" | "-w" => {
                    let value = iter.next().context("missing value for --workers")?;
                    opts.workers = value
                        .parse::<usize>()
                        .context("invalid value for --workers")?;
                }
                "--seed" => {
                    let value = iter.next().context("missing value for --seed")?;
                    opts.seed_tasks = value.parse::<usize>().context("invalid value for --seed")?;
                }
                "--duration" => {
                    let value = iter.next().context("missing value for --duration")?;
                    opts.duration = Duration::from_secs(
                        value
                            .parse::<u64>()
                            .context("invalid value for --duration")?,
                    );
                }
                "--target-throughput" => {
                    let value = iter
                        .next()
                        .context("missing value for --target-throughput")?;
                    opts.target_throughput = Some(
                        value
                            .parse::<f64>()
                            .context("invalid value for --target-throughput")?,
                    );
                }
                value if !value.starts_with('-') && opts.nodes == 100 => {
                    opts.nodes = value
                        .parse::<usize>()
                        .context("invalid positional node count")?;
                }
                unknown => bail!("unknown scale option '{unknown}'"),
            }
        }

        Ok(opts)
    }
}

struct SimulationNode {
    id: u64,
    addr: SocketAddr,
    queue: TaskQueue,
    leader_state: LeaderState,
    server: JoinHandle<()>,
    apply_handle: JoinHandle<()>,
    label: String,
    metrics: Metrics,
}

#[tokio::main]
async fn main() -> Result<()> {
    let logger = build_logger();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let command = parse_command(&args)?;

    match command {
        Command::Simulate(options) => run_cluster_simulation(logger, options).await,
        Command::Worker(options) => run_worker_mode(logger, options).await,
        Command::Coordinator(options) => run_coordinator_mode(logger, options).await,
        Command::Scale(options) => run_scale(logger, options).await,
    }
}

fn build_logger() -> Logger {
    let decorator = slog_term::TermDecorator::new().stderr().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    Logger::root(drain, o!("app" => "edgeforge"))
}

async fn run_worker_mode(logger: Logger, options: WorkerOptions) -> Result<()> {
    let WorkerOptions {
        id,
        hint,
        service,
        capability,
        batch_size,
    } = options;

    let worker_id = id.unwrap_or_else(|| format!("worker-{:04}", rand::thread_rng().gen::<u16>()));

    let mut config = WorkerConfig::new(worker_id.clone())
        .with_capability(capability.clone())
        .with_batch_size(batch_size);

    if let Some(service_name) = service {
        config = config.with_service_name(service_name);
    }

    if let Some(hint) = hint {
        config = config.with_coordinator_hint(normalize_hint(&hint));
    }

    let worker_logger = logger.new(o!("component" => "worker", "id" => worker_id.clone()));
    let worker = Worker::new(config, worker_logger.clone())?;
    let mut worker_task = worker.spawn();
    let mut shutdown = Box::pin(signal::ctrl_c());

    slog::info!(worker_logger, "worker runtime started";
        "worker_id" => worker_id.as_str(),
        "capability" => capability.to_string(),
        "batch_size" => batch_size);

    tokio::select! {
        res = &mut worker_task => {
            match res {
                Ok(()) => slog::info!(worker_logger, "worker loop exited cleanly"),
                Err(err) => slog::error!(worker_logger, "worker loop failed"; "error" => %err),
            }
        }
        res = &mut shutdown => {
            match res {
                Ok(()) => slog::info!(worker_logger, "shutdown signal received";
                    "worker_id" => worker_id.as_str()),
                Err(err) => slog::warn!(worker_logger, "shutdown listener failed";
                    "worker_id" => worker_id.as_str(), "error" => %err),
            }
            worker_task.abort();
        }
    }

    Ok(())
}

async fn run_coordinator_mode(logger: Logger, options: CoordinatorOptions) -> Result<()> {
    let options = options;
    let consensus_logger = logger.new(o!("component" => "consensus", "mode" => "coordinator"));
    let cluster = consensus::bootstrap(&[options.node_id], consensus_logger)?;
    let mut nodes: Vec<(u64, _)> = cluster.nodes.into_iter().collect();
    let (node_id, handle) = nodes
        .pop()
        .ok_or_else(|| anyhow!("consensus bootstrap produced no nodes"))?;

    let consensus::ConsensusNode {
        leader,
        replicator,
        apply_rx,
    } = handle;

    let node_label = format!("node-{node_id}");
    let queue = TaskQueue::with_replicator_and_actor(replicator.clone(), node_label.clone());
    let apply_queue = queue.clone();
    let mut apply_rx = apply_rx;
    let apply_logger = logger.new(o!("component" => "apply", "node" => node_id));
    let apply_handle = tokio::spawn(async move {
        while let Some(entry) = apply_rx.recv().await {
            apply_queue.apply_log(entry).await;
        }
        slog::debug!(apply_logger, "apply loop terminated");
    });

    let metrics = Metrics::new()?;
    let (broadcaster, _) = broadcast::channel(128);
    let metrics_handle = metrics.clone();
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, options.port));
    let coordinator_logger = logger.new(o!("component" => "coordinator", "node" => node_id));
    let pending_checksums = Arc::new(Mutex::new(HashMap::new()));
    let context = CoordinatorContext::new(
        queue.clone(),
        metrics,
        leader.clone(),
        coordinator_logger.clone(),
        node_label.clone(),
        broadcaster.clone(),
        pending_checksums.clone(),
    );
    let server_handle = coordinator::spawn(context, addr);

    slog::info!(coordinator_logger, "coordinator online";
        "node_id" => node_id,
        "addr" => addr);

    let initial_depth = queue.len().await;
    metrics_handle.set_queue_depth(&node_label, initial_depth);

    let leader_info = wait_for_leader_state(&leader, Duration::from_secs(5)).await?;
    metrics_handle.inc_leader_election(&node_label);
    slog::info!(coordinator_logger, "leader elected";
        "leader_id" => leader_info.leader_id,
        "term" => leader_info.term);

    if options.seed_tasks > 0 {
        seed_queue(&queue, options.seed_tasks).await?;
        let depth = queue.len().await;
        metrics_handle.set_queue_depth(&node_label, depth);
        slog::info!(coordinator_logger, "seeded task backlog";
            "tasks" => options.seed_tasks);
    }

    signal::ctrl_c().await?;
    slog::info!(coordinator_logger, "shutdown signal received");

    server_handle.abort();
    apply_handle.abort();

    Ok(())
}

async fn run_cluster_simulation(logger: Logger, options: SimulateOptions) -> Result<()> {
    let node_ids: Vec<u64> = (1..=options.nodes as u64).collect();
    let consensus_logger = logger.new(o!("component" => "consensus", "mode" => "simulate"));
    let cluster = consensus::bootstrap(&node_ids, consensus_logger)?;
    let mut nodes: Vec<(u64, _)> = cluster.nodes.into_iter().collect();
    nodes.sort_by_key(|(id, _)| *id);

    let mut runtimes = Vec::with_capacity(nodes.len());

    for (offset, (node_id, handle)) in nodes.into_iter().enumerate() {
        let consensus::ConsensusNode {
            leader,
            replicator,
            apply_rx,
        } = handle;

        let node_label = format!("node-{node_id}");
        let queue = TaskQueue::with_replicator_and_actor(replicator.clone(), node_label.clone());
        let apply_queue = queue.clone();
        let mut apply_rx = apply_rx;
        let apply_logger = logger.new(o!("component" => "apply", "node" => node_id));
        let apply_handle = tokio::spawn(async move {
            while let Some(entry) = apply_rx.recv().await {
                apply_queue.apply_log(entry).await;
            }
            slog::debug!(apply_logger, "apply loop terminated");
        });

        let metrics = Metrics::new()?;
        let (broadcaster, _) = broadcast::channel(128);
        let metrics_handle = metrics.clone();
        let pending_checksums = Arc::new(Mutex::new(HashMap::new()));
        let port = COORDINATOR_PORT + offset as u16;
        let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
        let coordinator_logger = logger.new(o!("component" => "coordinator", "node" => node_id));
        let context = CoordinatorContext::new(
            queue.clone(),
            metrics,
            leader.clone(),
            coordinator_logger.clone(),
            node_label.clone(),
            broadcaster.clone(),
            pending_checksums.clone(),
        );
        let server_handle = coordinator::spawn(context, addr);

        slog::info!(coordinator_logger, "coordinator spawned";
            "addr" => addr);

        let initial_depth = queue.len().await;
        metrics_handle.set_queue_depth(&node_label, initial_depth);

        runtimes.push(SimulationNode {
            id: node_id,
            addr,
            queue,
            leader_state: leader,
            server: server_handle,
            apply_handle,
            label: node_label,
            metrics: metrics_handle,
        });
    }

    let primary_state = runtimes
        .first()
        .ok_or_else(|| anyhow!("cluster bootstrap produced no nodes"))?
        .leader_state
        .clone();

    let leader_info = wait_for_leader_state(&primary_state, Duration::from_secs(10)).await?;
    slog::info!(logger, "cluster leader elected";
        "leader_id" => leader_info.leader_id,
        "term" => leader_info.term);

    let (leader_addr, leader_queue, leader_label, leader_metrics) = {
        let node = runtimes
            .iter()
            .find(|node| node.id == leader_info.leader_id)
            .ok_or_else(|| anyhow!("leader {} not found in runtime set", leader_info.leader_id))?;
        node.metrics.inc_leader_election(&node.label);
        (
            node.addr,
            node.queue.clone(),
            node.label.clone(),
            node.metrics.clone(),
        )
    };

    if options.seed_tasks > 0 {
        seed_queue(&leader_queue, options.seed_tasks).await?;
        let depth = leader_queue.len().await;
        leader_metrics.set_queue_depth(&leader_label, depth);
        slog::info!(logger, "seeded task backlog";
            "leader_id" => leader_info.leader_id,
            "tasks" => options.seed_tasks);
    }

    let leader_hint = format!("http://{leader_addr}/");
    let mut worker_handles = Vec::with_capacity(options.worker_count);

    for idx in 0..options.worker_count {
        let capability = if idx % 2 == 0 {
            TaskKind::Compute
        } else {
            TaskKind::Io
        };
        let batch_size = match capability {
            TaskKind::Compute => options.compute_batch,
            TaskKind::Io => options.io_batch,
        };

        let worker_id = format!("sim-worker-{idx}");
        let worker_config = WorkerConfig::new(worker_id.clone())
            .with_coordinator_hint(leader_hint.clone())
            .with_capability(capability.clone())
            .with_batch_size(batch_size);
        let worker_logger = logger.new(o!("component" => "worker", "id" => worker_id.clone()));

        slog::info!(worker_logger, "simulation worker started";
            "capability" => capability.to_string(),
            "batch_size" => batch_size);

        let worker = Worker::new(worker_config, worker_logger)?;
        worker_handles.push(worker.spawn());
    }

    slog::info!(logger, "cluster simulation running";
        "nodes" => options.nodes,
        "workers" => options.worker_count,
        "duration_secs" => options.duration.as_secs());

    tokio::select! {
        _ = tokio::time::sleep(options.duration) => {
            slog::info!(logger, "simulation completed";
                "duration_secs" => options.duration.as_secs());
        }
        _ = signal::ctrl_c() => {
            slog::warn!(logger, "simulation interrupted");
        }
    }

    for handle in worker_handles {
        handle.abort();
    }

    for node in runtimes {
        node.server.abort();
        node.apply_handle.abort();
    }

    Ok(())
}

async fn run_scale(logger: Logger, options: ScaleOptions) -> Result<()> {
    let simulate_options = SimulateOptions {
        nodes: options.nodes,
        worker_count: options.workers,
        seed_tasks: options.seed_tasks,
        duration: options.duration,
        compute_batch: DEFAULT_BATCH_SIZE,
        io_batch: DEFAULT_IO_BATCH_SIZE,
    };

    slog::info!(logger, "starting scale simulation";
        "nodes" => options.nodes,
        "workers" => options.workers,
        "seed_tasks" => options.seed_tasks,
        "duration_secs" => options.duration.as_secs());

    let started = Instant::now();
    run_cluster_simulation(logger.clone(), simulate_options).await?;
    let elapsed = started.elapsed();
    let throughput = options.seed_tasks as f64 / elapsed.as_secs_f64();

    if let Some(target) = options.target_throughput {
        if throughput < target {
            slog::warn!(logger, "throughput below target";
                "throughput" => throughput,
                "target" => target);
        }
    }

    slog::info!(logger, "scale simulation complete";
        "elapsed_secs" => elapsed.as_secs_f64(),
        "throughput" => throughput);

    Ok(())
}

fn parse_command(args: &[String]) -> Result<Command> {
    if args.is_empty() {
        return Ok(Command::Simulate(SimulateOptions::default()));
    }

    match args[0].as_str() {
        "worker" | "--worker" => WorkerOptions::parse(&args[1..]).map(Command::Worker),
        "coordinator" | "--coordinator" => {
            CoordinatorOptions::parse(&args[1..]).map(Command::Coordinator)
        }
        "simulate" | "--simulate" => SimulateOptions::parse(&args[1..]).map(Command::Simulate),
        "scale" | "--scale" => ScaleOptions::parse(&args[1..]).map(Command::Scale),
        other if other.starts_with("--") => SimulateOptions::parse(args).map(Command::Simulate),
        _ => SimulateOptions::parse(args).map(Command::Simulate),
    }
}

async fn wait_for_leader_state(state: &LeaderState, timeout: Duration) -> Result<LeaderInfo> {
    let deadline = Instant::now() + timeout;
    loop {
        let snapshot = state.snapshot();
        if snapshot.leader_id != 0 {
            return Ok(snapshot);
        }
        if Instant::now() >= deadline {
            bail!("leader election timed out after {:?}", timeout);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn seed_queue(queue: &TaskQueue, count: usize) -> Result<()> {
    if count == 0 {
        return Ok(());
    }

    let mut rng = rand::thread_rng();
    for _ in 0..count {
        let id = rng.gen::<u64>();
        let priority = rng.gen_range(1..=10);
        let kind = if rng.gen_bool(0.5) {
            TaskKind::Compute
        } else {
            TaskKind::Io
        };
        let payload: Vec<u8> = (0..32).map(|_| rng.gen()).collect();
        let task = Task::new(id, kind, priority, payload);
        queue.push(task).await;
    }

    Ok(())
}

fn clamp_batch(value: usize) -> usize {
    value.max(1).min(10)
}

fn normalize_hint(input: &str) -> String {
    let mut url = input.to_string();
    if !url.contains("://") {
        url = format!("http://{url}");
    }
    if !url.ends_with('/') {
        url.push('/');
    }
    url
}

fn parse_task_kind(value: &str) -> Result<TaskKind> {
    match value.to_ascii_lowercase().as_str() {
        "compute" | "cpu" | "ml" => Ok(TaskKind::Compute),
        "io" | "io-bound" | "sensor" => Ok(TaskKind::Io),
        other => bail!("unknown capability '{other}'"),
    }
}
