use std::collections::HashMap;
use std::error::Error;
use std::io::Write;
use std::sync::Arc;

use chrono::{ Local};
use clap::Parser;
use config::Config;
use log::{error, info, Level};

use log2click::{CObject, Filter, PipBuilder, ReceiveTrait, SendTrait};
use log2click::error::SyncError;
use log2click::parser::{Json, Regular};
use log2click::sink::Clickhouse;
use log2click::source::Kafka;

#[derive(Parser, Debug)]
#[command(version = "1.0.3", about = "Log2Click: Rust program for Kafka log processing with seamless Clickhouse integration and efficient handling of large volumes.", long_about = None)]
#[command(next_line_help = true)]
struct Args {
    /// Specify a YAML configuration file for easy customization of settings and options.
    #[arg(short, long, default_value = "config.yaml")]
    config: String,
    /// Enable debugging
    #[arg(long, default_value = "false")]
    debug: bool,
}

async fn try_main(conf: HashMap<String, CObject>) -> Result<(), SyncError> {
    let source: Arc<dyn ReceiveTrait> = Arc::new(Kafka::create(&conf)?);
    let sink: Arc<dyn SendTrait> = Arc::new(Clickhouse::create(&conf)?);
    let mut filters = Vec::new();
    filters.push(Arc::new(Json) as Arc<dyn Filter>);
    filters.push(Arc::new(Regular::create(&conf)?) as Arc<dyn Filter>);
    PipBuilder::default()
        .source(Some(source))
        .filters(filters)
        .sink(Some(sink))
        .build()?
        .run().await?;

    return Ok(());
}

async fn read_config(path: &str) -> Result<HashMap<String, CObject>, SyncError> {
    let settings = Config::builder()
        .add_source(config::File::with_name(path))
        .build()?;
    Ok(settings.try_deserialize::<HashMap<String, CObject>>()?)
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

    match read_config(&args.config).await {
        Ok(conf) => {
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
    }
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