//! Unit that handles job execution

use crate::pipeline::JobNode;

pub trait Executor {
    fn execute(&self, job: JobNode) -> anyhow::Result<()>;
}


/// simple debug executor that prints the commands
#[derive(Debug)]
pub struct DebugExecutor;

impl Executor for DebugExecutor {
    fn execute(&self, job: JobNode) -> anyhow::Result<()> {
        for cmd in job.script {
            println!("{}", cmd);
        }
        Ok(())
    }
}

