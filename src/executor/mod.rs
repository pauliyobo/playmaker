//! Unit that handles job execution
pub mod docker;
use crate::pipeline::JobNode;

/// An execution context responsible for keeping information useful to a JobNode
#[derive(Clone, Debug)]
pub struct ExecutionContext {
    pub job: JobNode,
}

#[async_trait::async_trait]
pub trait Executor: Clone + Send + Sync + 'static {
    async fn execute(&self, ctx: &ExecutionContext) -> anyhow::Result<()>;
}
