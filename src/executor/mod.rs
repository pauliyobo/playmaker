//! Unit that handles job execution
pub mod docker;
use crate::{models::ArtifactRef, pipeline::JobNode, runner::JobState};
use crate::log::Logger;
use std::{collections::HashMap, fs, path::PathBuf};

/// An execution context responsible for keeping information useful to a JobNode
#[derive(Clone, Debug)]
pub struct ExecutionContext {
    /// pipeline name
    pub name: String,
    /// Node associated to this context
    pub job: JobNode,
    /// Directory in which artifacts will be placed
    pub artifacts_dir: PathBuf,
    /// list of references of artifacts that are collected from previous jobs from which the current one depends
    pub dependencies: Vec<ArtifactRef>,
    pub logger: Logger,
}

impl ExecutionContext {
    pub fn log<S: Into<String>>(&self, message: S) -> anyhow::Result<()> {
        let message = message.into();
        self.logger.log(&self.name, &message)?;
        Ok(())
    }
}
#[derive(Clone, Debug)]
pub struct ExecutionResult {
    pub artifacts: Vec<ArtifactRef>,
    pub state: JobState,
}

impl ExecutionContext {
    /// returns the artifact directory dedicated to the job
    pub fn artifact_path(&self) -> PathBuf {
        self.artifacts_dir.join(&self.job.name)
    }

    /// ensures that the artifacts dir exists
    pub fn ensure_artifacts(&self) -> anyhow::Result<()> {
        if self.artifact_path().exists() {
            return Ok(());
        }
        fs::create_dir_all(&self.artifact_path())?;
        Ok(())
    }

    /// Returns the environment variables that will be injected inside the executor
    pub fn environment_variables(&self) -> HashMap<String, String> {
        let mut predefined = HashMap::from([("JOB_ID".to_string(), self.job.name.clone())]);
        predefined.extend(self.job.variables.clone().unwrap_or_default().into_iter());
        predefined
    }
}

#[async_trait::async_trait]
pub trait Executor: Clone + Send + Sync + 'static {
    async fn execute(&self, ctx: &ExecutionContext) -> anyhow::Result<ExecutionResult>;
    async fn cancel(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
