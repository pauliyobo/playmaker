mod executor;
mod models;
mod pipeline;
mod registry;
mod runner;

use std::env;
use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
use runner::Runner;
use serde::{Deserialize, Serialize};
use tokio::signal;

use crate::registry::ExecutorRegistry;

const DEFAULT_EXECUTOR: &str = "docker";

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Pipeline {
    name: String,
    stages: Vec<String>,
    jobs: Vec<Job>,
    executor: Option<String>,
}

impl Pipeline {
    #[cfg(test)]
    pub fn new(name: &str) -> Self {
        Self {
            name: name.into(),
            stages: Vec::new(),
            jobs: Vec::new(),
            executor: Some("mock".into()),
        }
    }

    #[cfg(test)]
    pub fn with_stages(mut self, stages: Vec<String>) -> Self {
        self.stages = stages;
        self
    }

    #[cfg(test)]
    pub fn add_job(&mut self, name: &str, stage: &str, script: &str, needs: Option<Vec<String>>) {
        let job = Job {
            name: name.into(),
            script: script.split("\n").map(|x| x.to_string()).collect(),
            needs,
            stage: stage.into(),
            image: None,
            artifacts: None,
            ..Default::default()
        };
        self.jobs.push(job);
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Artifact {
    paths: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct Job {
    name: String,
    script: Vec<String>,
    needs: Option<Vec<String>>,
    stage: String,
    image: Option<String>,
    artifacts: Option<Vec<Artifact>>,
    variables: Option<HashMap<String, String>>,
    executor: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = env::args().collect::<Vec<_>>();
    if args.len() == 1 {
        eprintln!("No filename provided");
        return Ok(());
    }
    let file_name = args.get(1).unwrap();
    let mut pipeline: Pipeline = serde_saphyr::from_str(
        &std::fs::read_to_string(file_name).context(format!("failed to read file {file_name}"))?,
    )
    .context("Failed to validate yaml input")?;
    // if no default executor was set for the pipeline, we use the default executor defined by us
    if pipeline.executor.is_none() {
        pipeline.executor = Some(DEFAULT_EXECUTOR.into());
    }
    let executor = executor::docker::DockerExecutor::new();
    let registry = ExecutorRegistry::default();
    registry.register(executor);
    let runner = Arc::new(Runner::new(pipeline, &registry));
    let runner2 = runner.clone();
    tokio::select! {
        result = tokio::spawn(async move {
            let runner = runner2.clone();
            runner.run().await
        }) => {
            result.unwrap()?;
        }
        _ = signal::ctrl_c() => {
            println!("Graceful shutdown");
            runner.cancel().await?;
        }
    }
    Ok(())
}
