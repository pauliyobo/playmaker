//! Unit that handles job execution
pub mod docker;
use crate::pipeline::JobNode;

#[async_trait::async_trait]
pub trait Executor: Clone + Send + Sync + 'static {
    async fn execute(&self, job: JobNode) -> anyhow::Result<()>;
}

/// simple debug executor that prints the commands
#[derive(Debug, Clone)]
pub struct DebugExecutor;

#[async_trait::async_trait]
impl Executor for DebugExecutor {
    async fn execute(&self, job: JobNode) -> anyhow::Result<()> {
        for cmd in job.script {
            println!("{}", cmd);
        }
        Ok(())
    }
}
