//! Executor registry

use dashmap::DashMap;
use std::sync::Arc;

use crate::executor::Executor;

/// a registry to store all the different `Executor` implementations to be referneced in workflows
#[derive(Clone, Default)]
pub struct ExecutorRegistry {
    executors: Arc<DashMap<String, Box<dyn Executor>>>,
}

impl ExecutorRegistry {
    /// stores an executor insidethe registry
    pub fn register(&self, executor: impl Executor) {
        self.executors
            .insert(executor.name().to_string(), Box::new(executor));
    }

    pub fn get(&self, name: &str) -> Option<Box<dyn Executor>> {
        self.executors.get(name).map(|x| x.value().clone())
    }
}
