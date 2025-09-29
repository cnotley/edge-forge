use std::sync::Arc;

use anyhow::Result;
use prometheus::{Encoder, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts, Registry, TextEncoder};

#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    registry: Registry,
    tasks_completed: IntCounterVec,
    tasks_enqueued: IntCounterVec,
    tasks_dequeued: IntCounterVec,
    queue_depth: IntGauge,
    elections_total: IntCounter,
    pull_latency: HistogramVec,
    process_latency: HistogramVec,
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
            Opts::new("edgeforge_tasks_enqueued_total", "Tasks enqueued"),
            &["kind"],
        )?;
        registry.register(Box::new(tasks_enqueued.clone()))?;

        let tasks_dequeued = IntCounterVec::new(
            Opts::new("edgeforge_tasks_dequeued_total", "Tasks dequeued"),
            &["kind"],
        )?;
        registry.register(Box::new(tasks_dequeued.clone()))?;

        let queue_depth = IntGauge::new("edgeforge_queue_depth", "Current queue depth")?;
        registry.register(Box::new(queue_depth.clone()))?;

        let elections_total = IntCounter::new("edgeforge_elections_total", "Leader elections")?;
        registry.register(Box::new(elections_total.clone()))?;

        let pull_latency = HistogramVec::new(
            HistogramOpts::new("edgeforge_pull_latency_seconds", "Worker pull latency"),
            &["worker"],
        )?;
        registry.register(Box::new(pull_latency.clone()))?;

        let process_latency = HistogramVec::new(
            HistogramOpts::new("edgeforge_process_latency_seconds", "Worker process latency"),
            &["worker"],
        )?;
        registry.register(Box::new(process_latency.clone()))?;

        Ok(Self {
            inner: Arc::new(MetricsInner {
                registry,
                tasks_completed,
                tasks_enqueued,
                tasks_dequeued,
                queue_depth,
                elections_total,
                pull_latency,
                process_latency,
            }),
        })
    }

    pub fn observe_completion(&self, worker_id: &str) {
        self.inner
            .tasks_completed
            .with_label_values(&[worker_id])
            .inc();
    }

    pub fn observe_enqueued(&self, kind: &str, count: u64) {
        self.inner
            .tasks_enqueued
            .with_label_values(&[kind])
            .inc_by(count);
    }

    pub fn observe_dequeued(&self, kind: &str, count: u64) {
        self.inner
            .tasks_dequeued
            .with_label_values(&[kind])
            .inc_by(count);
    }

    pub fn set_queue_depth(&self, depth: i64) {
        self.inner.queue_depth.set(depth);
    }

    pub fn observe_election(&self) {
        self.inner.elections_total.inc();
    }

    pub fn observe_pull_latency(&self, worker_id: &str, seconds: f64) {
        self.inner
            .pull_latency
            .with_label_values(&[worker_id])
            .observe(seconds);
    }

    pub fn observe_process_latency(&self, worker_id: &str, seconds: f64) {
        self.inner
            .process_latency
            .with_label_values(&[worker_id])
            .observe(seconds);
    }

    pub fn render(&self) -> Result<String> {
        let families = self.inner.registry.gather();
        let mut buffer = Vec::new();
        TextEncoder::new().encode(&families, &mut buffer)?;
        Ok(String::from_utf8(buffer)?)
    }
}
