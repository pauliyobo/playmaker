use crate::{
    executor::ExecutionResult,
    models::ArtifactRef,
    runner::{JobState, master_token},
};
use std::{path::PathBuf, sync::Arc};

use super::{ExecutionContext, Executor};
use anyhow::Context;
use bollard::{
    Docker,
    exec::StartExecResults,
    models::ContainerCreateBody,
    query_parameters::{
        CreateContainerOptions, CreateImageOptionsBuilder, DownloadFromContainerOptionsBuilder,
        RemoveContainerOptionsBuilder, StartContainerOptions, UploadToContainerOptionsBuilder,
    },
};
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt};
use tokio::{fs::File, io::AsyncWriteExt, sync::Mutex};
use tokio_util::sync::CancellationToken;

const IMAGE: &str = "alpine:3";

/// The docker executor is responsible for creating and running job nodes in isolated environments
#[derive(Debug, Clone)]
pub struct DockerExecutor {
    /// Underlying docker client
    client: Docker,
    /// Cancellation token used to gracefully terminate jobs
    token: CancellationToken,
}

impl DockerExecutor {
    pub fn new() -> Self {
        let client = Docker::connect_with_local_defaults().unwrap();
        let token = master_token().child_token();
        Self { client, token }
    }

    async fn exec_command(&self, id: &str, cmd: &[&str]) -> anyhow::Result<()> {
        let mut script = String::from("set -e\n");
        script.push_str(&cmd.join("\n"));
        let exec = self
            .client
            .create_exec(
                &id,
                bollard::models::ExecConfig {
                    attach_stderr: Some(true),
                    attach_stdin: Some(true),
                    attach_stdout: Some(true),
                    tty: Some(true),
                    cmd: Some(vec!["/bin/sh".to_string(), "-c".to_string(), script]),
                    ..Default::default()
                },
            )
            .await?
            .id;
        if let StartExecResults::Attached { mut output, .. } =
            self.client.start_exec(&exec, None).await?
        {
            let read_task = async move {
                while let Some(msg) = output.next().await {
                    match msg {
                        Ok(msg) => println!("{msg}"),
                        Err(e) => println!("Error: {:?}", e),
                    }
                }
            };
            /* let write_task = async move {
                for item in cmd {
                    input
                        .write_all(format!("{item}\n").as_bytes())
                        .await
                        .expect("Failed to write command");
                }
                input.write_all(b"exit\n").await.unwrap();
            };*/
            tokio::join!(read_task);
        }
        let inspect = self.client.inspect_exec(&exec).await?;
        let Some(status) = inspect.exit_code else {
            anyhow::bail!("Failed to inspect exec");
        };
        if status != 0 {
            anyhow::bail!("Job failed.");
        }
        Ok(())
    }

    async fn finish(
        &self,
        container_id: Option<String>,
        artifacts: Vec<ArtifactRef>,
        state: JobState,
    ) -> anyhow::Result<ExecutionResult> {
        if let Some(id) = container_id {
            println!("Removing container {}", &id);
            self.client
                .remove_container(
                    &id,
                    Some(RemoveContainerOptionsBuilder::default().force(true).build()),
                )
                .await?;
        }
        Ok(ExecutionResult { artifacts, state })
    }
}

#[async_trait::async_trait]
impl Executor for DockerExecutor {
    async fn execute(&self, ctx: &ExecutionContext) -> anyhow::Result<ExecutionResult> {
        let mut artifact_refs = Vec::new();
        let container_id = Arc::new(Mutex::new(None));
        let job = ctx.job.clone();
        let closure_container_id = container_id.clone();
        let result = self
            .token
            .run_until_cancelled(async move {
                println!("Creating image");
                let image = job.image.unwrap_or(IMAGE.to_string());
                let env = ctx
                    .environment_variables()
                    .into_iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>();
                self.client
                    .create_image(
                        Some(
                            CreateImageOptionsBuilder::default()
                                .from_image(&image)
                                .build(),
                        ),
                        None,
                        None,
                    )
                    .try_collect::<Vec<_>>()
                    .await
                    .context("Failed to create image")?;
                let container_config = ContainerCreateBody {
                    image: Some(image),
                    tty: Some(true),
                    entrypoint: Some(vec!["/bin/sh".into()]),
                    env: Some(env),
                    ..Default::default()
                };
                let id = self
                    .client
                    .create_container(None::<CreateContainerOptions>, container_config)
                    .await?;
                let mut guard = closure_container_id.lock().await;
                *guard = Some(id.id);
                anyhow::Ok(())
            })
            .await;
        let guard = container_id.lock().await;
        let id = guard.clone();
        match result {
            None => {
                return self.finish(id, artifact_refs, JobState::Cancelled).await;
            }
            Some(status) => {
                // we are interested in failing only if there was an error while bootstrapping
                if status.is_err() {
                    return self.finish(id, artifact_refs, JobState::Failed).await;
                }
            }
        }
        let id = id.unwrap();
        println!("Created container {}", id);
        let res: Option<anyhow::Result<()>> = self
            .token
            .run_until_cancelled(async {
                let id = id.clone();
                self.client
                    .start_container(&id, None::<StartContainerOptions>)
                    .await?;
                println!("Started container {}", id);
                for dep in ctx.dependencies.iter() {
                    let mut buf = PathBuf::from(&dep.path);
                    if buf.file_name().is_some() {
                        buf.pop();
                    }
                    let opts = UploadToContainerOptionsBuilder::new()
                        .path(buf.to_str().unwrap())
                        .build();
                    let data = tokio::fs::read(&dep.host_path).await?;
                    let bytes = Bytes::copy_from_slice(data.as_slice());
                    let body = bollard::body_full(bytes);
                    match self
                        .client
                        .upload_to_container(&id, Some(opts), body)
                        .await
                        .context("failed to upload artifact inside container")
                    {
                        Ok(_) => {}
                        Err(e) => {
                            println!("Failed to upload artifact to container, {:?}", e);
                            continue;
                        }
                    }
                }
                // we don't unwrap the result now because we want the container to be cleaned up properly
                let res = self
                    .exec_command(
                        &id,
                        job.script
                            .iter()
                            .map(AsRef::as_ref)
                            .collect::<Vec<_>>()
                            .as_slice(),
                    )
                    .await;
                if let Some(artifacts) = job.artifacts {
                    ctx.ensure_artifacts()
                        .context("Failed to create artifacts directory")?;
                    for artifact in artifacts {
                        for path in artifact.paths {
                            let options = DownloadFromContainerOptionsBuilder::new()
                                .path(&path)
                                .build();
                            let dest = ctx
                                .artifacts_dir
                                .join(format!("{}/artifacts.tar", job.name));
                            let mut file = File::create(dest.as_path()).await?;
                            let mut stream =
                                self.client.download_from_container(&id, Some(options));
                            while let Some(Ok(body)) = stream.next().await {
                                file.write_all(&body).await?;
                            }
                            artifact_refs.push(ArtifactRef {
                                host_path: dest,
                                path,
                            });
                        }
                    }
                }
                Ok(res?)
            })
            .await;
        let state = match res {
            // If we got none, it means the job has cancelled before completion
            None => JobState::Cancelled,
            // If we got some, we need to match on the result inside the option in order to obtain the correct state
            Some(v) => match v {
                Ok(_) => JobState::Complete,
                Err(_) => JobState::Failed,
            },
        };
        self.finish(Some(id), artifact_refs, state).await
    }

    async fn cancel(&self) -> anyhow::Result<()> {
        self.token.cancel();
        Ok(())
    }
}
