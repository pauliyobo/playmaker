use std::path::PathBuf;

/// Artifact reference that will be passed between jobs that depend on each other
#[derive(Clone, Debug)]
pub struct ArtifactRef {
    /// path in which the container should be uploaded to
    pub path: String,
    pub host_path: PathBuf,
}
