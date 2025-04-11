use crate::Pipeline;
use itertools::Itertools;
use petgraph::graph::{DiGraph, NodeIndex};
use std::collections::HashMap;

/// a Node that represents a job
#[derive(Clone, Debug, Default)]
pub struct JobNode {
    pub name: String,
    pub stage: String,
    pub script: Vec<String>,
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
    pub fn new(pipeline: Pipeline) -> Self {
        let mut jobs = pipeline
            .jobs
            .iter()
            .map(|job| {
                (
                    JobNode {
                        name: job.name.clone(),
                        script: job.script.clone(),
                        stage: job.stage.clone(),
                    },
                    job.needs.clone(),
                )
            })
            .collect::<Vec<_>>();
        jobs.sort_by_key(|(job, _)| pipeline.stages.iter().position(|x| &job.stage == x).unwrap());
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
            edges.extend(
                deps.get(&node.name)
                    .unwrap()
                    .iter()
                    .filter(|x| nodes.contains_key(*x))
                    .map(|x| {
                        (
                            nodes.get(x).unwrap().to_owned(),
                            nodes.get(&node.name).unwrap().to_owned(),
                        )
                    }),
            );
        }
        graph.extend_with_edges(edges);
        Self { graph, nodes, stages: pipeline.stages.clone() }
    }

    // returns the jobs ordered by stage, that need to be executed
    pub fn jobs(&self) -> Vec<JobNode> {
        let sorted: Vec<JobNode> = petgraph::algo::toposort(&self.graph, None)
            .unwrap()
            .into_iter()
            .map(|x| {
                // by this point we always assume all node indexes are valid
                self.graph.node_weight(x).unwrap().clone()
            })
            .collect();
        sorted.into_iter().sorted_by_key(|k| self.stages.iter().position(|x| x == &k.stage).unwrap()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_order_with_dependencies() {
        let mut pipeline = Pipeline::new("test").with_stages(vec!["test".into()]);
        pipeline.add_job("job1", "test", "cd", vec!["job2".into()]);
        pipeline.add_job("job2", "test", "cd ..", vec![]);
        let graph = PipelineGraph::new(pipeline);
        let jobs = graph.jobs();
        assert_eq!(jobs[0].name, String::from("job2"))
    }

    #[test]
    fn test_jobs_order_respected_across_stages() {
        let mut pipeline = Pipeline::new("test").with_stages(vec!["stage1".into(), "stage2".into()]);
        pipeline.add_job("job1", "stage1", "", vec![]);
        pipeline.add_job("job2", "stage2", "", vec![]);
        pipeline.add_job("job3", "stage1", "", vec![]);
        let graph = PipelineGraph::new(pipeline);
        let mut jobs = graph.jobs().into_iter();
        assert_eq!(jobs.next().unwrap().stage, String::from("stage1"));
        assert_eq!(jobs.next().unwrap().stage, String::from("stage1"));
        assert_eq!(jobs.next().unwrap().stage, String::from("stage2"))
    }
}
