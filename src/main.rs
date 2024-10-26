mod pipeline;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Pipeline {
    name: String,
    stages: Vec<String>,
    jobs: Vec<Job>,
}

impl Pipeline {
    pub fn new(name: &str) -> Self {
        Self {
             name: name.into(),
            stages: Vec::new(),
            jobs: Vec::new(),
        }
    }

    pub fn with_stages(mut self, stages: Vec<String>) -> Self {
        self.stages = stages;
        self
    }

    pub fn add_job(&mut self, name: &str, stage: &str, script: &str, needs: Vec<String>) {
        let job = Job {
            name: name.into(),
            script: script.split("\n").map(|x| x.to_string()).collect(),
            needs,
            stage: stage.into(),
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
}


fn main() {
    let pipeline = Pipeline::new("test");
    let graph = pipeline::PipelineGraph::new(pipeline);
    let sorted_jobs = graph.jobs();
    println!("{:?}", sorted_jobs);
    println!("{:?}", sorted_jobs[0]);
}
