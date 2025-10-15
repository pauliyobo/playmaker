//! Unit that handles orchestration of executors
use crate::executor::Executor;
use crate::pipeline::PipelineGraph;
use dashmap::DashMap;
use std::sync::Arc;

/// State for a Job
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum JobState {
    Pending,
    Running,
    Failed,
    Complete,
}

#[derive(Debug)]
pub struct Runner {
    graph: PipelineGraph,
    states: Arc<DashMap<String, JobState>>,
}

impl Runner {
    pub fn is_ready(&self, job_name: &str) -> bool {
        let parents = self.graph.job_parents(job_name);
        if parents.is_empty() {
            return true;
        }
        parents
            .iter()
            .all(|x| *self.states.get(&x.name).unwrap().value() == JobState::Complete)
    }

    pub fn new(graph: PipelineGraph) -> Self {
        let states = graph
            .jobs()
            .iter()
            .map(|x| (x.name.clone(), JobState::Pending))
            .collect::<DashMap<_, _>>();
        Self {
            graph,
            states: Arc::new(states),
        }
    }

    pub async fn submit<E: Executor>(&self, executor: E) -> anyhow::Result<()> {
        for job in self.graph.jobs() {
            let job_name = job.name.clone();
            if !self.is_ready(&job_name) {
                println!("Job {job_name} not ready.");
            }
            if let Some(mut entry) = self.states.get_mut(&job.name) {
                *entry.value_mut() = JobState::Running;
            }
            match executor.execute(job).await {
                Ok(_) => *self.states.get_mut(&job_name).unwrap().value_mut() = JobState::Complete,
                Err(_) => *self.states.get_mut(&job_name).unwrap().value_mut() = JobState::Failed,
            }
        }
        Ok(())
    }
}
