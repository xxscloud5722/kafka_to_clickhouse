use thiserror::Error;

#[derive(Error, Debug)]
pub enum SyncError {
    #[error("ConfigError: {0}")]
    ConfigError(#[from] config::ConfigError),

    #[error("PipBuilderError: {0}")]
    PipBuilderError(#[from] crate::PipBuilderError),

    #[error("ParseIntError: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error("{0}")]
    KafkaError(#[from] rdkafka::error::KafkaError),

    #[error("None")]
    Option,

    #[error("{0}")]
    MissingParams(&'static str),
}