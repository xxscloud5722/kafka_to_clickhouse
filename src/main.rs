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

use KafkaSync::{CObject, Filter, Pip, PipBuilder, ReceiveTrait, SendTrait};
use KafkaSync::error::SyncError;
use KafkaSync::parser::{Json, Regular};
use KafkaSync::sink::Clickhouse;
use KafkaSync::source::Kafka;

#[derive(Parser, Debug)]
#[command(version = "1.0.1", about = "Log2Click: Rust program for Kafka log processing with seamless Clickhouse integration and efficient handling of large volumes.", long_about = None)]
#[command(next_line_help = true)]
struct Args {
    /// Specify a YAML configuration file for easy customization of settings and options.
    #[arg(short, long, default_value = "config.yaml")]
    config: String,
    /// Enable debugging
    #[arg(long, default_value = "true")]
    debug: bool,
}

async fn try_main(conf: HashMap<String, CObject>) -> Result<(), SyncError> {
    let source: Arc<dyn ReceiveTrait> = Arc::new(Kafka::create(&conf)?);
    let sink: Arc<dyn SendTrait> = Arc::new(Clickhouse::create(&conf)?);
    let mut filters = Vec::new();
    filters.push(Arc::new(Json) as Arc<dyn Filter>);
    filters.push(Arc::new(Regular::create(&conf)?) as Arc<dyn Filter>);
    PipBuilder::default()
        .conf(conf)
        .source(Some(source))
        .filters(filters)
        .sink(Some(sink))
        .build()?
        .run().await?;

    return Ok(());
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    info!("Import Configuration: {}", &args.config);


    // Log Format Definition
    let level = if args.debug { "debug" } else { "info" };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(level))
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

    let settings = Config::builder()
        .add_source(config::File::with_name(&args.config))
        .build()
        .unwrap();
    let conf = settings.try_deserialize::<HashMap<String, CObject>>()
        .unwrap();

    match try_main(conf).await {
        Ok(_) => {}
        Err(error) => {
            match error.source() {
                None => {
                    error!("{}", error.to_string());
                }
                Some(error) => {
                    error!("{:?}", error);
                }
            }
        }
    };
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test1() -> Result<(), SyncError> {
        let settings = Config::builder()
            .add_source(config::File::with_name("config.yaml"))
            .build()?;

        let conf = settings.try_deserialize::<HashMap<String, CObject>>()?;
        let text = "2024-03-19 19:01:32.737 INFO [                main] [SS.:DD12] org.springframework.data.repository.config.RepositoryConfigurationDelegate : Multiple Spring Data modules found, entering strict repository configuration mode
    at 123441321
    at 12321312321321321";
        let values = Regular::create(&conf)?.regex(text);
        for (index, x) in values.iter().enumerate() {
            println!("[{}] => {}", index, x);
        }
        Ok(())
    }
}