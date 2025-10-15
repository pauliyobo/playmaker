use super::Executor;
use bollard::{
    Docker,
    exec::StartExecResults,
    query_parameters::{
        CreateContainerOptions, CreateImageOptionsBuilder, RemoveContainerOptionsBuilder,
        StartContainerOptions,
    },
    secret::ContainerCreateBody,
};
use futures_util::{FutureExt, StreamExt, TryStreamExt};

#[derive(Debug, Clone)]
pub struct DockerExecutor {
    client: Docker,
}

impl DockerExecutor {
    pub fn new() -> Self {
        let client = Docker::connect_with_local_defaults().unwrap();
        Self { client }
    }
    async fn exec_command(&self, id: &str, cmd: &str) -> anyhow::Result<()> {
        let exec = self
            .client
            .create_exec(
                &id,
                bollard::models::ExecConfig {
                    attach_stderr: Some(true),
                    attach_stdin: Some(true),
                    attach_stdout: Some(true),
                    cmd: Some(vec![
                        "/bin/sh".to_string(),
                        "-c".to_string(),
                        cmd.to_string(),
                    ]),
                    ..Default::default()
                },
            )
            .await?
            .id;
        if let StartExecResults::Attached { mut output, .. } =
            self.client.start_exec(&exec, None).await?
        {
            while let Some(Ok(msg)) = output.next().await {
                println!("{}", msg);
            }
        }
        Ok(())
    }
}

impl Executor for DockerExecutor {
    async fn execute(&self, job: crate::pipeline::JobNode) -> anyhow::Result<()> {
        println!("Creating image");
        self.client
            .create_image(
                Some(
                    CreateImageOptionsBuilder::default()
                        .from_image("alpine:3")
                        .build(),
                ),
                None,
                None,
            )
            .try_collect::<Vec<_>>()
            .await?;
        let container_config = ContainerCreateBody {
            image: Some("alpine:3".into()),
            tty: Some(true),
            entrypoint: Some(vec!["/bin/sh".into()]),
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
        for cmd in job.script {
            self.exec_command(&id.id, &cmd).await?;
        }
        println!("Removing container {}", &id.id);
        self.client
            .remove_container(
                &id.id,
                Some(RemoveContainerOptionsBuilder::default().force(true).build()),
            )
            .await?;
        Ok(())
    }
}
