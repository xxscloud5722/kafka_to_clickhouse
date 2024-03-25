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

    #[error("{0}")]
    JSONError(#[from] serde_json::Error),

    #[error("{0}")]
    RegexError(#[from] regex::Error),

    #[error("{0}")]
    ClickhouseError(#[from] clickhouse::error::Error),

    #[error("None occurred")]
    Option,

    #[error("{0}")]
    MissingParams(&'static str),

    #[error("{0}")]
    OptionParams(String),
}