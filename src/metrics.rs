use std::sync::Arc;

use anyhow::Result;
use prometheus::{Encoder, IntCounterVec, Opts, Registry, TextEncoder};

#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    registry: Registry,
    tasks_completed: IntCounterVec,
}

impl Metrics {
    pub fn new() -> Result<Self> {
        let registry = Registry::new();

        let tasks_completed = IntCounterVec::new(
            Opts::new("edgeforge_tasks_completed", "Tasks completed by worker"),
            &["worker"],
        )?;

        registry.register(Box::new(tasks_completed.clone()))?;

        Ok(Self {
            inner: Arc::new(MetricsInner {
                registry,
                tasks_completed,
            }),
        })
    }

    pub fn observe_completion(&self, worker_id: &str) {
        self.inner
            .tasks_completed
            .with_label_values(&[worker_id])
            .inc();
    }

    pub fn render(&self) -> Result<String> {
        let families = self.inner.registry.gather();
        let mut buffer = Vec::new();
        TextEncoder::new().encode(&families, &mut buffer)?;
        Ok(String::from_utf8(buffer)?)
    }
}
