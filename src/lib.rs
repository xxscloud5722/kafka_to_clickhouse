use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use derive_builder::Builder;
use rdkafka::Message;
use serde::Deserialize;
use serde_json::{Map, Value};
use tokio::select;
use tokio::sync::mpsc;

use crate::sink::SendTrait;

mod source;
mod sink;
pub mod parser;

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

#[derive(Default, Builder, Debug)]
#[builder(setter(into))]
pub struct Pip {
    conf: HashMap<String, CObject>,
}

impl Pip {
    pub async fn run(&self) {}
}

#[derive(Debug, Clone, Copy)]
pub enum SourceEnum {
    Kafka
}

#[derive(Debug, Clone, Copy)]
pub enum SinkEnum {
    Clickhouse
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

#[async_trait(? Send)]
trait MessageCallback {
    async fn handle(&self, message: Vec<LogMessage>);
}

#[derive(Clone)]
pub struct PipelineSourceBuild {
    value: Option<SourceEnum>,
}

impl PipelineSourceBuild {
    pub fn source(&mut self, source: SourceEnum) -> PipelineFilterBuild {
        self.value = Some(source);
        return PipelineFilterBuild { source: self.to_owned(), filters: None };
    }
}


#[derive(Clone)]
pub struct PipelineFilterBuild {
    source: PipelineSourceBuild,
    filters: Option<Arc<Vec<Box<dyn Filter>>>>,
}

impl PipelineFilterBuild {
    pub fn filter(&mut self, filters: Vec<Box<dyn Filter>>) -> PipelineSinkBuild {
        self.filters = Some(Arc::new(filters));
        return PipelineSinkBuild {
            value: None,
            source: self.source.to_owned(),
            filter: self.to_owned(),
        };
    }
}

#[derive(Clone)]
pub struct PipelineSinkBuild {
    value: Option<SinkEnum>,
    source: PipelineSourceBuild,
    filter: PipelineFilterBuild,
}

impl PipelineSinkBuild {
    pub fn sink(&mut self, sink: SinkEnum) -> PipelineBuild {
        self.value = Some(sink);
        return PipelineBuild {
            source: self.source.to_owned(),
            filter: self.filter.to_owned(),
            sink: self.to_owned(),
        };
    }
}

pub struct PipelineBuild {
    source: PipelineSourceBuild,
    filter: PipelineFilterBuild,
    sink: PipelineSinkBuild,
}

impl PipelineBuild {
    pub async fn run(&self) {
        let source_type = match &self.source.value {
            None => SourceEnum::Kafka,
            Some(v) => v.to_owned()
        };
        let sink_type = match &self.sink.value {
            None => &SinkEnum::Clickhouse,
            Some(v) => v
        };

        let (tx, mut rx) = mpsc::channel::<Vec<LogMessage>>(100);
        let mut source_instance = source::create_instance(source_type);
        let sink_instance = sink::create_instance(sink_type);
        let filters = self.filter.filters.clone().unwrap();
        select! {
            // accept message
            _ = source_instance.pull(tx) => {
            },
            // relay the message
            _ = sink_instance.push(filters.as_ref(), rx) => {
            }
        }
    }
}

pub fn create_pipeline() -> PipelineSourceBuild {
    return PipelineSourceBuild { value: None };
}