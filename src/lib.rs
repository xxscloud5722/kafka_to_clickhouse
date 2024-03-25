use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use derive_builder::Builder;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::error::SyncError;

pub mod source;
pub mod sink;
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
    pub offset: i64,
    pub log: Option<String>,
    pub map: Option<Map<String, Value>>,
}

impl From<&CObject> for Option<HashMap<String, CObject>> {
    fn from(obj: &CObject) -> Self {
        match obj {
            CObject::Object(map) => Some(map.clone()),
            _ => None
        }
    }
}

impl From<&CObject> for Vec<String> {
    fn from(obj: &CObject) -> Self {
        match obj {
            CObject::Array(array) => {
                array.to_vec().iter().map(|x| {
                    match x {
                        CObject::String(val) => val.to_owned(),
                        _ => String::default()
                    }
                }).filter(|x| x != "").collect()
            }
            _ => Vec::default()
        }
    }
}

impl From<&CObject> for String {
    fn from(obj: &CObject) -> Self {
        match obj {
            CObject::String(v) => v.to_owned(),
            _ => String::default()
        }
    }
}

impl From<&CObject> for f64 {
    fn from(obj: &CObject) -> Self {
        match obj {
            CObject::Number(v) => *v,
            _ => 0f64
        }
    }
}

#[async_trait(? Send)]
pub trait Filter {
    async fn process(&self, data: Vec<LogMessage>) -> Result<Vec<LogMessage>, SyncError>;
}

#[async_trait(? Send)]
pub trait ReceiveTrait {
    async fn pull(&self) -> Result<Vec<LogMessage>, SyncError>;

    async fn confirm(&self) -> Result<(), SyncError>;
}

#[async_trait(? Send)]
pub trait SendTrait {
    async fn push(&self, message: Vec<LogMessage>) -> Result<(), SyncError>;
}


#[derive(Default, Builder)]
#[builder(setter(into))]
pub struct Pip {
    #[builder(default = "None")]
    filters: Option<Vec<Arc<dyn Filter>>>,
    source: Option<Arc<dyn ReceiveTrait>>,
    sink: Option<Arc<dyn SendTrait>>,
}

impl Pip {
    pub async fn run(self) -> Result<(), SyncError> {
        let receive = self.source.ok_or(SyncError::Option)?;
        let send = self.sink.ok_or(SyncError::Option)?;
        let filters = self.filters.unwrap_or(Vec::default());
        loop {
            let mut messasge = receive.pull().await?;
            for x in &filters {
                messasge = x.process(messasge).await?;
            }
            send.push(messasge).await?;
            receive.confirm().await?;
        }
    }
}