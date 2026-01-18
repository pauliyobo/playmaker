mod executor;
mod models;
mod pipeline;
mod runner;

use std::env;
use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
use runner::Runner;
use serde::{Deserialize, Serialize};
use tokio::signal;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Pipeline {
    name: String,
    stages: Vec<String>,
    jobs: Vec<Job>,
}

impl Pipeline {
    #[cfg(test)]
    pub fn new(name: &str) -> Self {
        Self {
            name: name.into(),
            stages: Vec::new(),
            jobs: Vec::new(),
        }
    }

    #[cfg(test)]
    pub fn with_stages(mut self, stages: Vec<String>) -> Self {
        self.stages = stages;
        self
    }

    #[cfg(test)]
    pub fn add_job(&mut self, name: &str, stage: &str, script: &str, needs: Vec<String>) {
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
struct Artifact {
    paths: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct Job {
    name: String,
    script: Vec<String>,
    needs: Vec<String>,
    stage: String,
    image: Option<String>,
    artifacts: Option<Vec<Artifact>>,
    variables: Option<HashMap<String, String>>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = env::args().collect::<Vec<_>>();
    if args.len() == 1 {
        eprintln!("No filename provided");
        return Ok(());
    }
    let file_name = args.get(1).unwrap();
    let pipeline = serde_saphyr::from_str(
        &std::fs::read_to_string(file_name).context(format!("failed to read file {file_name}"))?,
    )
    .context("Failed to validate yaml input")?;
    let executor = executor::docker::DockerExecutor::new();
    let runner = Arc::new(Runner::new(pipeline, executor));
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
