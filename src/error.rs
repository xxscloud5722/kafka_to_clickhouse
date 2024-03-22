use thiserror::Error;

#[derive(Error, Debug)]
pub enum SyncError {
    #[error("ConfigError: {0}")]
    ConfigError(#[from] config::ConfigError),

    #[error("PipBuilderError: {0}")]
    PipBuilderError(#[from] crate::PipBuilderError),
}