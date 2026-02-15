use crate::{Artifact, Pipeline};
use itertools::Itertools;
use petgraph::{
    Direction,
    graph::{DiGraph, NodeIndex},
};
use std::collections::HashMap;

/// Pipeline validation error
#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum PipelineError {
    /// The stage on which the job is supposed to run on does not exist
    #[error("The stage {0} does not exist")]
    InvalidStage(String),
    /// The name of the dependent job does not exist
    #[error("{0} is not a valid job dependency")]
    InvalidDependency(String),
}

/// a Node that represents a job
#[derive(Clone, Debug, Default)]
pub struct JobNode {
    pub name: String,
    pub stage: String,
    pub script: Vec<String>,
    pub image: Option<String>,
    pub artifacts: Option<Vec<Artifact>>,
    pub variables: Option<HashMap<String, String>>,
    pub executor: Option<String>,
}

/// pipeline graph
#[derive(Clone, Debug)]
pub struct PipelineGraph {
    /// graph that holds the jobs
    graph: DiGraph<JobNode, String>,
    /// map between job names and node indexes of the graph
    nodes: HashMap<String, NodeIndex<u32>>,
    /// stage names
    /// This is used to also keep track off the stage order
    stages: Vec<String>,
}

impl PipelineGraph {
    /// Creates and validates a new pipeline graph from the Pipeline definition
    pub fn new(pipeline: Pipeline) -> Result<Self, PipelineError> {
        let mut jobs = pipeline
            .jobs
            .iter()
            .map(|job| {
                (
                    JobNode {
                        name: job.name.clone(),
                        script: job.script.clone(),
                        stage: job.stage.clone(),
                        image: job.image.clone(),
                        artifacts: job.artifacts.clone(),
                        variables: job.variables.clone(),
                        executor: job.executor.clone(),
                    },
                    job.needs.clone().unwrap_or_default(),
                )
            })
            .collect::<Vec<_>>();
        // validate that all stages referenced by the actual jobs exist
        jobs.iter().try_for_each(|(job, _)| {
            if !pipeline.stages.contains(&job.stage) {
                return Err(PipelineError::InvalidStage(job.stage.clone()));
            }
            Ok(())
        })?;
        jobs.sort_by_key(|(job, _)| {
            pipeline
                .stages
                .iter()
                .position(|x| &job.stage == x)
                .unwrap()
        });
        let mut graph = DiGraph::<JobNode, String>::new();
        let mut nodes: HashMap<String, NodeIndex<u32>> = HashMap::new();
        let mut deps: HashMap<String, Vec<String>> = HashMap::new();
        for (job, needs) in jobs {
            println!("adding jobs for stage {}", job.stage);
            deps.insert(job.name.clone(), needs);
            let job_name = job.name.clone();
            let node = graph.add_node(job);
            nodes.insert(job_name, node);
        }
        let mut edges: Vec<(NodeIndex<u32>, NodeIndex<u32>)> = vec![];
        for node in graph.node_weights() {
            // validate that the dependencies do actually exist
            let it = deps
                .get(&node.name)
                .unwrap()
                .iter()
                .try_fold(vec![], |mut acc, x| {
                    if !nodes.contains_key(x) {
                        // We are referencing an invalid job, abort the pipeline
                        return Err(PipelineError::InvalidDependency(x.clone()));
                    }
                    acc.push((
                        nodes.get(x).unwrap().to_owned(),
                        nodes.get(&node.name).unwrap().to_owned(),
                    ));
                    Ok(acc)
                })?;
            edges.extend(it);
        }
        graph.extend_with_edges(edges);
        Ok(Self {
            graph,
            nodes,
            stages: pipeline.stages.clone(),
        })
    }

    /// returns the jobs ordered by stage that need to be executed
    pub fn jobs(&self) -> Vec<JobNode> {
        let sorted: Vec<JobNode> = petgraph::algo::toposort(&self.graph, None)
            .unwrap()
            .into_iter()
            .map(|x| {
                // by this point we always assume all node indexes are valid
                self.graph.node_weight(x).unwrap().clone()
            })
            .collect();
        sorted
            .into_iter()
            .sorted_by_key(|k| self.stages.iter().position(|x| x == &k.stage).unwrap())
            .collect()
    }

    pub fn job_dependencies(&self, job_name: &str) -> Vec<JobNode> {
        let Some(job_index) = self.nodes.get(job_name) else {
            return vec![];
        };
        self.graph
            .neighbors_directed(*job_index, Direction::Outgoing)
            .filter_map(|x| self.graph.node_weight(x))
            .cloned()
            .collect()
    }

    pub fn job_parents(&self, job_name: &str) -> Vec<JobNode> {
        let Some(job_index) = self.nodes.get(job_name) else {
            return vec![];
        };
        self.graph
            .neighbors_directed(*job_index, Direction::Incoming)
            .filter_map(|x| self.graph.node_weight(x))
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_order_with_dependencies() {
        let mut pipeline = Pipeline::new("test").with_stages(vec!["test".into()]);
        pipeline.add_job("job1", "test", "cd", Some(vec!["job2".into()]));
        pipeline.add_job("job2", "test", "cd ..", None);
        let graph = PipelineGraph::new(pipeline).unwrap();
        let jobs = graph.jobs();
        assert_eq!(jobs[0].name, String::from("job2"))
    }

    #[test]
    fn test_jobs_order_respected_across_stages() {
        let mut pipeline =
            Pipeline::new("test").with_stages(vec!["stage1".into(), "stage2".into()]);
        pipeline.add_job("job1", "stage1", "", None);
        pipeline.add_job("job2", "stage2", "", None);
        pipeline.add_job("job3", "stage1", "", None);
        let graph = PipelineGraph::new(pipeline).unwrap();
        let mut jobs = graph.jobs().into_iter();
        assert_eq!(jobs.next().unwrap().stage, String::from("stage1"));
        assert_eq!(jobs.next().unwrap().stage, String::from("stage1"));
        assert_eq!(jobs.next().unwrap().stage, String::from("stage2"))
    }

    #[test]
    fn test_job_dependencies() {
        let mut pipeline =
            Pipeline::new("test").with_stages(vec!["stage1".into(), "stage2".into()]);
        pipeline.add_job("job1", "stage1", "", None);
        pipeline.add_job("job2", "stage2", "", Some(vec!["job1".to_string()]));
        let graph = PipelineGraph::new(pipeline).unwrap();
        assert_eq!(graph.job_dependencies("job1").len(), 1)
    }

    #[test]
    fn test_pipeline_fails_if_job_has_invalid_stage() {
        let mut pipeline =
            Pipeline::new("test").with_stages(vec!["stage1".into(), "stage2".into()]);
        pipeline.add_job("job1", "stage3", "", None);
        let graph = PipelineGraph::new(pipeline);
        assert!(graph.is_err());
        assert_eq!(
            graph.unwrap_err(),
            PipelineError::InvalidStage("stage3".into())
        )
    }

    #[test]
    fn test_pipeline_fails_if_invalid_job_dependency() {
        let mut pipeline =
            Pipeline::new("test").with_stages(vec!["stage1".into(), "stage2".into()]);
        pipeline.add_job("job1", "stage1", "", None);
        pipeline.add_job("job2", "stage2", "", Some(vec!["job3".into()]));
        let graph = PipelineGraph::new(pipeline);
        assert!(graph.is_err());
        assert_eq!(
            graph.unwrap_err(),
            PipelineError::InvalidDependency("job3".into())
        )
    }
}
