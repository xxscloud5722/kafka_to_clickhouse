use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use derive_builder::Builder;
use rdkafka::Message;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::sink::SendTrait;

mod source;
mod sink;
pub mod parser;
pub mod error;

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum CObject {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Array(Vec<CObject>),
    Object(HashMap<String, CObject>),
}

#[derive(Debug, Clone)]
pub struct LogMessage {
    pub topic: String,
    pub body: String,
    pub partition: i32,
    pub log: Option<String>,
    pub map: Option<Map<String, Value>>,
}

#[async_trait(? Send)]
pub trait Filter {
    async fn process(&self, data: Vec<LogMessage>) -> Vec<LogMessage>;
}

#[derive(Default, Builder, Debug)]
#[builder(setter(into))]
pub struct Pip {
    conf: HashMap<String, CObject>,
    filters: Option<Box<dyn Filter>>,
}

impl Pip {
    pub async fn run(&self) {}
}


// impl PipelineBuild {
//     pub async fn run(&self) {
//         let source_type = match &self.source.value {
//             None => SourceEnum::Kafka,
//             Some(v) => v.to_owned()
//         };
//         let sink_type = match &self.sink.value {
//             None => &SinkEnum::Clickhouse,
//             Some(v) => v
//         };
//
//         let (tx, mut rx) = mpsc::channel::<Vec<LogMessage>>(100);
//         let mut source_instance = source::create_instance(source_type);
//         let sink_instance = sink::create_instance(sink_type);
//         let filters = self.filter.filters.clone().unwrap();
//         select! {
//             // accept message
//             _ = source_instance.pull(tx) => {
//             },
//             // relay the message
//             _ = sink_instance.push(filters.as_ref(), rx) => {
//             }
//         }
//     }
// }
//
// pub fn create_pipeline() -> PipelineSourceBuild {
//     return PipelineSourceBuild { value: None };
// }