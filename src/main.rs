use std::collections::HashMap;
use std::error::Error;
use std::io::Write;
use std::sync::Arc;

use chrono::{DateTime, Local, TimeZone};
use clap::Parser;
use config::Config;
use log::{error, info, Level, log};
use serde::Deserialize;
use serde_json::Value;

use KafkaSync::{CObject, Pip, PipBuilder, ReceiveTrait, SendTrait};
use KafkaSync::error::SyncError;
use KafkaSync::sink::Clickhouse;
use KafkaSync::source::Kafka;

#[derive(Parser, Debug)]
#[command(version = "1.0.1", about = "Log2Click: Rust program for Kafka log processing with seamless Clickhouse integration and efficient handling of large volumes.", long_about = None)]
#[command(next_line_help = true)]
struct Args {
    /// Specify a YAML configuration file for easy customization of settings and options.
    #[arg(short, long, default_value = "config.yaml")]
    config: String,
}

async fn try_main() -> Result<(), SyncError> {
    let args = Args::parse();
    info!("Import Configuration: {}", &args.config);

    let settings = Config::builder()
        .add_source(config::File::with_name(&args.config))
        .build()?;

    let conf = settings.try_deserialize::<HashMap<String, CObject>>()?;
    let source: Arc<dyn ReceiveTrait> = Arc::new(Kafka::create(&conf)?);
    let sink: Arc<dyn SendTrait> = Arc::new(Clickhouse);
    PipBuilder::default()
        .conf(conf)
        .source(Some(source))
        .sink(Some(sink))
        .build()?
        .run().await?;

    return Ok(());
}

#[tokio::main]
async fn main() {
    // Log Format Definition
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format(|buffer, record| {
            writeln!(
                buffer,
                "{} {} - {}",
                Local::now().format("%Y-%m-%d %H:%M:%S"),
                match record.level() {
                    Level::Trace => "\x1b[35mTRACE\x1b[0m",
                    Level::Debug => "\x1b[34mDEBUG\x1b[0m",
                    Level::Info => "\x1b[32mINFO\x1b[0m",
                    Level::Warn => "\x1b[33mWARN\x1b[0m",
                    Level::Error => "\x1b[31mERROR\x1b[0m",
                },
                record.args()
            )
        })
        .init();

    match try_main().await {
        Ok(_) => {}
        Err(error) => {
            match error.source() {
                None => {
                    error!("Undefined errors.");
                }
                Some(error) => {
                    error!("{:?}", error);
                }
            }
        }
    };
}

