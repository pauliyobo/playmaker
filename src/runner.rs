//! Unit that handles orchestration of executors
use crate::Pipeline;
use crate::executor::{ExecutionContext, Executor};
use crate::models::ArtifactRef;
use crate::pipeline::{JobNode, PipelineGraph};
use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::sleep;

/// State for a Job
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum JobState {
    Pending,
    Running,
    Failed,
    Complete,
}

/// A job Runner responsible for orchestrating and executing jobs and managing
#[derive(Debug)]
pub struct Runner {
    graph: PipelineGraph,
    states: Arc<DashMap<String, JobState>>,
    artifact_refs: Arc<DashMap<String, Vec<ArtifactRef>>>,
}

impl Runner {
    pub fn build_context(&self, job: &JobNode) -> ExecutionContext {
        let parents = self.graph.job_parents(&job.name);
        let dependencies = parents.into_iter().fold(vec![], |mut acc, x| {
            let deps = {
                if let Some(value) = self.artifact_refs.get(&x.name) {
                    value.value().clone()
                } else {
                    vec![]
                }
            };
            acc.extend(deps);
            acc
        });
        ExecutionContext {
            job: job.clone(),
            artifacts_dir: PathBuf::from("artifacts"),
            dependencies,
        }
    }

    /// Returns true when there are jobs that are still waiting to be run
    pub fn jobs_available(&self) -> bool {
        self.states.iter().any(|x| x.value() == &JobState::Pending)
    }

    pub fn jobs(&self) -> Vec<JobNode> {
        self.graph
            .jobs()
            .into_iter()
            .filter(|x| *self.states.get(&x.name).unwrap().value() == JobState::Pending)
            .collect()
    }

    pub fn all_parents_match(&self, job_name: &str, state: JobState) -> bool {
        let parents = self.graph.job_parents(job_name);
        if parents.is_empty() {
            return true;
        }
        parents
            .iter()
            .all(|x| *self.states.get(&x.name).unwrap().value() == state)
    }

    pub fn any_parents_match(&self, job_name: &str, state: JobState) -> bool {
        let parents = self.graph.job_parents(job_name);
        if parents.is_empty() {
            return false;
        }
        parents
            .iter()
            .any(|x| *self.states.get(&x.name).unwrap().value() == state)
    }

    pub fn new(pipeline: Pipeline) -> Self {
        let graph = PipelineGraph::new(pipeline);
        let states = graph
            .jobs()
            .iter()
            .map(|x| (x.name.clone(), JobState::Pending))
            .collect::<DashMap<_, _>>();
        Self {
            graph,
            states: Arc::new(states),
            artifact_refs: Arc::new(DashMap::new()),
        }
    }

    pub async fn submit<E: Executor + Send>(&self, executor: E) -> anyhow::Result<()> {
        while self.jobs_available() {
            let mut tasks = JoinSet::new();
            for job in self.jobs() {
                let job_name = job.name.clone();
                if self.any_parents_match(&job_name, JobState::Failed) {
                    println!(
                        "Marking {job_name} as failed since one of the jobs it depends on has failed."
                    );
                    *self.states.get_mut(&job_name).unwrap().value_mut() = JobState::Failed;
                    continue;
                }
                if !self.all_parents_match(&job_name, JobState::Complete) {
                    println!("Waiting for {job_name} to become ready.");
                    continue;
                }
                if let Some(mut entry) = self.states.get_mut(&job.name) {
                    *entry.value_mut() = JobState::Running;
                }
                let ctx = self.build_context(&job);
                let executor = executor.clone();
                let states = self.states.clone();
                let artifact_refs = self.artifact_refs.clone();
                let task = async move {
                    match executor.execute(&ctx).await {
                        Ok(res) => {
                            *states.get_mut(&job_name).unwrap().value_mut() = JobState::Complete;
                            artifact_refs
                                .entry(job_name)
                                .and_modify(|v| v.extend(res.artifacts.clone()))
                                .or_insert(res.artifacts);
                        }
                        Err(_) => {
                            *states.get_mut(&job_name).unwrap().value_mut() = JobState::Failed
                        }
                    };
                };
                tasks.spawn(Box::pin(task));
            }
            tasks.join_all().await;
            sleep(Duration::from_secs(3)).await;
        }
        println!("{:?}", self.states);
        Ok(())
    }
}
