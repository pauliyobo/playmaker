# Playmaker
## What is this?
I have no idea actually. originally it was meant to be a sort of gitlab CI clone, but recently I've been considering to turn it into a multi purpose workflow orchestration engine. Oh well, the core itself is flexible enough to allow either roads. Probably.
In any case, this is my attempt at building a workflow orchestration system.
## components
The foundation of this project lays primarily in 3 components:

* a pipeline definition. Earlier I said I wanted it to be a workflow orchestration system but originally this was meant to run CI / CD  pipelines only. So I'll stick with that. The pipeline definition can be represented through yaml, and it is heavily, if not completely inspired from gitlab's CI's sintax.
* A runner. The runner is responsible for turning the pipeline into a directed acyclic graph which will have the single jobs as nodes and the relations between them as edges.
* An executor. The executor will just make sure to run the job in an isolated environment by receiving and returning artifacts that it may need to communicate with other jobs. Currently only a docker executor exists.

## Building and running
You can simply

```
cargo run -- file.yaml
```

To run a pipeline

## What can you do with it?
Probably not much. Right now, only running jobs insider docker images is supported. You can however share artifacts between jobs.
Here's a simple example

```yaml
name: hello world
stages:
  - hello

jobs:
    - name: print hello
      stage: hello
      # this represents which jobs we depend from, it is a mandatory field,
      # Will have to change this
      needs: []
      # by default, the executor uses alpine:3 for the job's image
      script:
        - echo "Hello world"
```

For more examples, look at the examples folder.  
**note**: This project may change direction at any time. Use at your own peril.
## Where will it go from here?
While I have not yet decided whether I'll turn this into a multi purpose workflow orchestration engine, a pure CI / CD orchestrator, or scrap the whole thing entirely, there are still a few things that would be nice to add such as:

* An API to manage operations and retrieve logs from jobs
* More executors
* state persistence
