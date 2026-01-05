use crate::{
    Pipeline,
    executor::docker::DockerExecutor,
    runner::{JobState, Runner},
};
use anyhow::anyhow;
use axum::{
    Router,
    extract::{Json, Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::net::TcpListener;
use tokio::signal;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum APIError {
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl IntoResponse for APIError {
    fn into_response(self) -> axum::response::Response {
        match self {
            APIError::Internal(e) => {
                eprintln!("{:?}", e);
                return (StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
                    .into_response();
            }
        }
    }
}

type Result<T, E = APIError> = anyhow::Result<T, E>;

#[derive(Clone, Debug)]
pub struct AppState {
    /// active pipelines
    pub runners: Arc<DashMap<Uuid, Runner<DockerExecutor>>>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            runners: Arc::new(DashMap::new()),
        }
    }
}

#[derive(Debug, Deserialize)]
struct CreatePipelineRequestBody {
    /// YAML text
    text: String,
}

#[derive(Debug, Serialize)]
struct CreatePipelineResponse {
    pub id: Uuid,
}

#[derive(Debug, Serialize)]
struct GetPipelineStatusResponse {
    jobs: HashMap<String, JobState>,
}

#[axum::debug_handler]
async fn create_pipeline(
    State(state): State<AppState>,
    Json(payload): Json<CreatePipelineRequestBody>,
) -> Result<(StatusCode, Json<CreatePipelineResponse>)> {
    let pipeline: Pipeline =
        serde_saphyr::from_str(&payload.text).map_err(|e| APIError::from(anyhow!(e)))?;
    let uuid = Uuid::new_v4();
    let executor = DockerExecutor::new();
    let runner = Runner::new(pipeline, executor);
    state.runners.insert(uuid.clone(), runner.clone());
    tokio::spawn(async move {
        runner.run().await.unwrap();
    });
    Ok((
        StatusCode::CREATED,
        Json(CreatePipelineResponse { id: uuid }),
    ))
}

async fn get_pipeline_status(
    State(state): State<AppState>,
    Path(pipeline_id): Path<Uuid>,
) -> Result<Json<Option<GetPipelineStatusResponse>>> {
    if let Some(runner) = state.runners.get(&pipeline_id) {
        let mut jobs = HashMap::new();
        for item in runner.states.iter() {
            jobs.insert(item.key().to_owned(), *item.value());
        }
        let response = GetPipelineStatusResponse { jobs };
        return Ok(Json(Some(response)));
    }
    Ok(Json(None))
}

fn router(state: &AppState) -> Router {
    Router::new()
        .route("/pipelines", post(create_pipeline))
        .route("/pipelines/{pipeline_id}", get(get_pipeline_status))
        .with_state(state.clone())
}

pub async fn setup() -> anyhow::Result<()> {
    let state = AppState::new();
    let router = Router::new().merge(router(&state));
    let listener = TcpListener::bind("0.0.0.0:8000").await?;
    println!("Listening on port 8000");
    tokio::select! {
        result = tokio::spawn(async move {
            axum::serve(listener, router).await?;
            anyhow::Ok(())
        }) => {
            result.unwrap().unwrap();
        }
        _ = signal::ctrl_c() => {
            println!("Graceful shutdown");
            for v in state.runners.iter() {
                let _ = v.value().cancel().await;
            }
        }
    }
    Ok(())
}
