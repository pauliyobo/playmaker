//! Unit that handles job execution
pub mod docker;
use crate::pipeline::JobNode;

pub trait Executor {
    async fn execute(&self, job: JobNode) -> anyhow::Result<()>;
}

/// simple debug executor that prints the commands
#[derive(Debug)]
pub struct DebugExecutor;

impl Executor for DebugExecutor {
    async fn execute(&self, job: JobNode) -> anyhow::Result<()> {
        for cmd in job.script {
            println!("{}", cmd);
        }
        Ok(())
    }
}
