mod executor;
mod pipeline;
mod runner;

use runner::Runner;
use serde::{Deserialize, Serialize};

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
        };
        self.jobs.push(job);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct Job {
    name: String,
    script: Vec<String>,
    needs: Vec<String>,
    stage: String,
    image: Option<String>,
}

#[tokio::main]
async fn main() {
    let pipeline = serde_yml::from_str(&std::fs::read_to_string("pipeline.yaml").unwrap())
        .expect("Failed to validate yaml.");
    let graph = pipeline::PipelineGraph::new(pipeline);
    let executor = executor::docker::DockerExecutor::new();
    let runner = Runner::new(graph);
    runner.submit(executor).await.unwrap();
}
