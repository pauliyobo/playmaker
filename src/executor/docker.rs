use std::{fs, path::PathBuf};

use crate::{executor::ExecutionResult, models::ArtifactRef};

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

const IMAGE: &str = "alpine:3";

#[derive(Debug, Clone)]
pub struct DockerExecutor {
    client: Docker,
}

impl DockerExecutor {
    pub fn new() -> Self {
        let client = Docker::connect_with_local_defaults().unwrap();
        Self { client }
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
}

#[async_trait::async_trait]
impl Executor for DockerExecutor {
    async fn execute(&self, ctx: &ExecutionContext) -> anyhow::Result<ExecutionResult> {
        let mut artifact_refs = Vec::new();
        let job = ctx.job.clone();
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
        println!("Created container {}", id.id);
        self.client
            .start_container(&id.id, None::<StartContainerOptions>)
            .await?;
        println!("Started container {}", id.id);
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
                .upload_to_container(&id.id, Some(opts), body)
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
                &id.id,
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
                    match self
                        .client
                        .download_from_container(&id.id, Some(options))
                        .try_next()
                        .await
                    {
                        Ok(stream) => {
                            if let Some(body) = stream {
                                let dest = ctx
                                    .artifacts_dir
                                    .join(format!("{}/artifacts.tar", job.name));
                                fs::write(&dest, body.to_vec())?;
                                artifact_refs.push(ArtifactRef {
                                    host_path: dest,
                                    path,
                                });
                            }
                        }
                        Err(e) => {
                            println!("Failed to copy artifact, {:?}", e);
                        }
                    }
                }
            }
        }
        println!("Removing container {}", &id.id);
        self.client
            .remove_container(
                &id.id,
                Some(RemoveContainerOptionsBuilder::default().force(true).build()),
            )
            .await?;
        res?;
        Ok(ExecutionResult {
            artifacts: artifact_refs,
        })
    }
}
