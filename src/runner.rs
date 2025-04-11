//! Unit that handles orchestration of executors
use crate::pipeline::PipelineGraph;
use crate::executor::Executor;

#[derive(Debug)]
pub struct Runner;

impl Runner {
    pub fn submit<E: Executor>(&self, graph: PipelineGraph, executor: E) -> anyhow::Result<()> {
        for job in graph.jobs() {
            executor.execute(job)?;
        }
        Ok(())
    }
}