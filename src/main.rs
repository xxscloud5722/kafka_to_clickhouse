use std::collections::HashMap;
use std::error::Error;
use std::io::Write;

use chrono::{DateTime, Local, TimeZone};
use clap::Parser;
use config::Config;
use log::{error, info, Level, log};
use serde::Deserialize;
use serde_json::Value;

use KafkaSync::{CObject, Pip, PipBuilder};
use KafkaSync::error::SyncError;

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

    PipBuilder::default().conf(conf).build()?
        .run().await;

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

    //info!("Import Configuration: {:?}", data);

    // env::set_var("receive.kafka.server", "logs.liexiong.net:29094");
    // env::set_var("receive.kafka.topic", "public");
    // env::set_var("receive.kafka.group_id", "test10");
    // env::set_var("receive.kafka.size", "1000");
    // env::set_var("receive.kafka.timeout", "3000");
    //
    // env::set_var("sender.clickhouse.server", "10.10.1.5");
    // env::set_var("sender.clickhouse.username", "default");
    // env::set_var("sender.clickhouse.password", "Lxasd123!@#");

    // create_pipeline()
    //     .source(SourceEnum::Kafka)
    //     .filter(vec![Box::new(parser::Json)])
    //     .sink(SinkEnum::Clickhouse)
    //     .run().await;
}
