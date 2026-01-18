//! Unit that handles orchestration of executors
use crate::Pipeline;
use crate::executor::{ExecutionContext, Executor};
use crate::models::ArtifactRef;
use crate::pipeline::{JobNode, PipelineGraph};
use dashmap::DashMap;
use serde::Serialize;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

static MASTER_TOKEN: OnceLock<CancellationToken> = OnceLock::new();

pub(crate) fn master_token() -> CancellationToken {
    MASTER_TOKEN
        .get_or_init(|| CancellationToken::new())
        .clone()
}

/// State for a Job
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
pub enum JobState {
    Pending,
    Running,
    Failed,
    Complete,
    Cancelled,
}

/// A job Runner responsible for orchestrating and executing jobs and managing
#[derive(Clone, Debug)]
pub struct Runner<E: Executor> {
    graph: PipelineGraph,
    pub states: Arc<DashMap<String, JobState>>,
    artifact_refs: Arc<DashMap<String, Vec<ArtifactRef>>>,
    executor: E,
    token: CancellationToken,
}

impl<E: Executor> Runner<E> {
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

    pub fn new(pipeline: Pipeline, executor: E) -> Self {
        let graph = PipelineGraph::new(pipeline).expect("Failed to create pipeline");
        let states = graph
            .jobs()
            .iter()
            .map(|x| (x.name.clone(), JobState::Pending))
            .collect::<DashMap<_, _>>();
        Self {
            graph,
            states: Arc::new(states),
            artifact_refs: Arc::new(DashMap::new()),
            executor,
            token: master_token(),
        }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let executor = self.executor.clone();
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
                            *states.get_mut(&job_name).unwrap().value_mut() = res.state;
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

    pub async fn cancel(&self) -> anyhow::Result<()> {
        self.token.cancel();
        println!("Waiting for jobs to cancel.");
        loop {
            // if all jobs have completed or failed there's no need to wait around
            if self
                .states
                .iter()
                .all(|x| *x.value() == JobState::Complete || *x.value() == JobState::Failed)
            {
                break;
            }
            if self
                .states
                .iter()
                .any(|x| *x.value() != JobState::Cancelled)
            {
                sleep(Duration::from_secs(5)).await;
            } else {
                break;
            }
            println!("{:?}", self.states);
        }
        println!("cancelled all jobs");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Pipeline;
    use crate::executor::{ExecutionContext, ExecutionResult, Executor};

    #[derive(Clone)]
    struct MockExecutor {
        should_fail: Arc<DashMap<String, bool>>,
    }

    impl MockExecutor {
        fn new() -> Self {
            Self {
                should_fail: Arc::new(DashMap::new()),
            }
        }

        fn set_job_failure(&self, job_name: &str, should_fail: bool) {
            self.should_fail.insert(job_name.to_string(), should_fail);
        }
    }

    #[async_trait::async_trait]
    impl Executor for MockExecutor {
        async fn execute(&self, ctx: &ExecutionContext) -> anyhow::Result<ExecutionResult> {
            if self
                .should_fail
                .get(&ctx.job.name)
                .map(|v| *v.value())
                .unwrap_or(false)
            {
                anyhow::bail!("Job {} failed", ctx.job.name);
            }
            Ok(ExecutionResult {
                artifacts: vec![],
                state: JobState::Complete,
            })
        }
    }

    #[tokio::test]
    async fn test_runner_simple_pipeline_all_jobs_complete() {
        let mut pipeline = Pipeline::new("test").with_stages(vec!["build".into()]);
        pipeline.add_job("job1", "build", "echo test", None);
        pipeline.add_job("job2", "build", "echo test2", None);

        let executor = MockExecutor::new();
        let runner = Runner::new(pipeline, executor);

        runner.run().await.unwrap();

        assert_eq!(
            *runner.states.get("job1").unwrap().value(),
            JobState::Complete
        );
        assert_eq!(
            *runner.states.get("job2").unwrap().value(),
            JobState::Complete
        );
    }

    #[tokio::test]
    async fn test_runner_job_failure_propagates_to_dependents() {
        let mut pipeline = Pipeline::new("test").with_stages(vec!["build".into(), "test".into()]);
        pipeline.add_job("build-job", "build", "echo build", None);
        pipeline.add_job(
            "test-job",
            "test",
            "echo test",
            Some(vec!["build-job".to_string()]),
        );

        let executor = MockExecutor::new();
        executor.set_job_failure("build-job", true);
        let runner = Runner::new(pipeline, executor);

        runner.run().await.unwrap();

        assert_eq!(
            *runner.states.get("build-job").unwrap().value(),
            JobState::Failed
        );
        assert_eq!(
            *runner.states.get("test-job").unwrap().value(),
            JobState::Failed
        );
    }

    #[tokio::test]
    async fn test_runner_parallel_jobs_in_same_stage() {
        let mut pipeline = Pipeline::new("test").with_stages(vec!["test".into()]);
        pipeline.add_job("parallel1", "test", "echo 1", None);
        pipeline.add_job("parallel2", "test", "echo 2", None);
        pipeline.add_job("parallel3", "test", "echo 3", None);

        let executor = MockExecutor::new();
        let runner = Runner::new(pipeline, executor);

        runner.run().await.unwrap();

        assert_eq!(
            *runner.states.get("parallel1").unwrap().value(),
            JobState::Complete
        );
        assert_eq!(
            *runner.states.get("parallel2").unwrap().value(),
            JobState::Complete
        );
        assert_eq!(
            *runner.states.get("parallel3").unwrap().value(),
            JobState::Complete
        );
    }

    #[tokio::test]
    async fn test_runner_multi_stage_with_dependencies() {
        let mut pipeline =
            Pipeline::new("test").with_stages(vec!["build".into(), "test".into(), "deploy".into()]);

        pipeline.add_job("build", "build", "echo build", None);
        pipeline.add_job("test1", "test", "echo test1", Some(vec!["build".to_string()]));
        pipeline.add_job("test2", "test", "echo test2", Some(vec!["build".to_string()]));
        pipeline.add_job(
            "deploy",
            "deploy",
            "echo deploy",
            Some(vec!["test1".to_string(), "test2".to_string()]),
        );

        let executor = MockExecutor::new();
        let runner = Runner::new(pipeline, executor);

        runner.run().await.unwrap();

        assert_eq!(
            *runner.states.get("build").unwrap().value(),
            JobState::Complete
        );
        assert_eq!(
            *runner.states.get("test1").unwrap().value(),
            JobState::Complete
        );
        assert_eq!(
            *runner.states.get("test2").unwrap().value(),
            JobState::Complete
        );
        assert_eq!(
            *runner.states.get("deploy").unwrap().value(),
            JobState::Complete
        );
    }

    #[tokio::test]
    async fn test_runner_partial_failure_with_multiple_parents() {
        let mut pipeline = Pipeline::new("test").with_stages(vec!["build".into(), "test".into()]);

        pipeline.add_job("build1", "build", "echo build1", None);
        pipeline.add_job("build2", "build", "echo build2", None);
        pipeline.add_job(
            "test",
            "test",
            "echo test",
            Some(vec!["build1".to_string(), "build2".to_string()]),
        );

        let executor = MockExecutor::new();
        executor.set_job_failure("build1", true);
        let runner = Runner::new(pipeline, executor);

        runner.run().await.unwrap();

        assert_eq!(
            *runner.states.get("build1").unwrap().value(),
            JobState::Failed
        );
        assert_eq!(
            *runner.states.get("build2").unwrap().value(),
            JobState::Complete
        );
        assert_eq!(
            *runner.states.get("test").unwrap().value(),
            JobState::Failed
        );
    }

    #[test]
    fn test_all_parents_match_empty_parents() {
        let pipeline = Pipeline::new("test").with_stages(vec!["build".into()]);
        let runner = Runner::new(pipeline, MockExecutor::new());

        assert!(runner.all_parents_match("nonexistent", JobState::Complete));
    }

    #[test]
    fn test_any_parents_match_empty_parents() {
        let pipeline = Pipeline::new("test").with_stages(vec!["build".into()]);
        let runner = Runner::new(pipeline, MockExecutor::new());

        assert!(!runner.any_parents_match("nonexistent", JobState::Failed));
    }

    #[test]
    fn test_build_context_includes_parent_artifacts() {
        let mut pipeline = Pipeline::new("test").with_stages(vec!["build".into(), "test".into()]);

        pipeline.add_job("build", "build", "echo build", None);
        pipeline.add_job("test", "test", "echo test", Some(vec!["build".to_string()]));

        let runner = Runner::new(pipeline, MockExecutor::new());

        // Simulate artifacts from build job
        runner.artifact_refs.insert(
            "build".to_string(),
            vec![ArtifactRef {
                path: "/home/text.txt".to_string(),
                host_path: PathBuf::from("artifacts/build/text.txt"),
            }],
        );

        let test_job = runner
            .graph
            .jobs()
            .into_iter()
            .find(|j| j.name == "test")
            .unwrap();
        let ctx = runner.build_context(&test_job);

        assert_eq!(ctx.dependencies.len(), 1);
        assert_eq!(ctx.dependencies[0].path, "/home/text.txt");
    }

    #[tokio::test]
    async fn test_runner_diamond_dependency() {
        let mut pipeline = Pipeline::new("test").with_stages(vec![
            "stage1".into(),
            "stage2".into(),
            "stage3".into(),
        ]);

        pipeline.add_job("root", "stage1", "echo root", None);
        pipeline.add_job("left", "stage2", "echo left", Some(vec!["root".to_string()]));
        pipeline.add_job("right", "stage2", "echo right", Some(vec!["root".to_string()]));
        pipeline.add_job(
            "merge",
            "stage3",
            "echo merge",
            Some(vec!["left".to_string(), "right".to_string()]),
        );

        let runner = Runner::new(pipeline, MockExecutor::new());

        runner.run().await.unwrap();

        assert_eq!(
            *runner.states.get("root").unwrap().value(),
            JobState::Complete
        );
        assert_eq!(
            *runner.states.get("left").unwrap().value(),
            JobState::Complete
        );
        assert_eq!(
            *runner.states.get("right").unwrap().value(),
            JobState::Complete
        );
        assert_eq!(
            *runner.states.get("merge").unwrap().value(),
            JobState::Complete
        );
    }
}
