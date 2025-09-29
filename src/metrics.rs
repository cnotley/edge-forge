use std::sync::Arc;

use anyhow::Result;
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder,
};

use crate::queue::TaskKind;

#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    registry: Registry,
    tasks_completed: IntCounterVec,
    tasks_enqueued: IntCounterVec,
    queue_depth: IntGaugeVec,
    task_latency: HistogramVec,
    leader_elections: IntCounterVec,
}

impl Metrics {
    pub fn new() -> Result<Self> {
        let registry = Registry::new();

        let tasks_completed = IntCounterVec::new(
            Opts::new("edgeforge_tasks_completed", "Tasks completed by worker"),
            &["worker"],
        )?;
        registry.register(Box::new(tasks_completed.clone()))?;

        let tasks_enqueued = IntCounterVec::new(
            Opts::new(
                "edgeforge_tasks_enqueued_total",
                "Tasks enqueued by coordinator",
            ),
            &["kind"],
        )?;
        registry.register(Box::new(tasks_enqueued.clone()))?;

        let queue_depth = IntGaugeVec::new(
            Opts::new(
                "edgeforge_queue_depth",
                "Current queue depth per coordinator",
            ),
            &["node"],
        )?;
        registry.register(Box::new(queue_depth.clone()))?;

        let latency_opts = HistogramOpts::new(
            "edgeforge_task_latency_seconds",
            "Observed task latencies reported by workers",
        )
        .buckets(prometheus::exponential_buckets(0.001, 2.0, 16)?);
        let task_latency = HistogramVec::new(latency_opts, &["kind"])?;
        registry.register(Box::new(task_latency.clone()))?;

        let leader_elections = IntCounterVec::new(
            Opts::new("edgeforge_leader_elections_total", "Leader election events"),
            &["node"],
        )?;
        registry.register(Box::new(leader_elections.clone()))?;

        Ok(Self {
            inner: Arc::new(MetricsInner {
                registry,
                tasks_completed,
                tasks_enqueued,
                queue_depth,
                task_latency,
                leader_elections,
            }),
        })
    }

    pub fn observe_completion(&self, worker_id: &str) {
        self.inner
            .tasks_completed
            .with_label_values(&[worker_id])
            .inc();
    }

    pub fn inc_enqueued(&self, kind: &TaskKind) {
        self.inner
            .tasks_enqueued
            .with_label_values(&[kind_label(kind)])
            .inc();
    }

    pub fn set_queue_depth(&self, node: &str, depth: usize) {
        self.inner
            .queue_depth
            .with_label_values(&[node])
            .set(depth as i64);
    }

    pub fn observe_task_latency(&self, kind: &TaskKind, seconds: f64) {
        self.inner
            .task_latency
            .with_label_values(&[kind_label(kind)])
            .observe(seconds);
    }

    pub fn inc_leader_election(&self, node: &str) {
        self.inner.leader_elections.with_label_values(&[node]).inc();
    }

    pub fn render(&self) -> Result<String> {
        let families = self.inner.registry.gather();
        let mut buffer = Vec::new();
        TextEncoder::new().encode(&families, &mut buffer)?;
        Ok(String::from_utf8(buffer)?)
    }
}

fn kind_label(kind: &TaskKind) -> &'static str {
    match kind {
        TaskKind::Compute => "compute",
        TaskKind::Io => "io",
    }
}
