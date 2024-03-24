use std::collections::HashMap;

use async_trait::async_trait;
use log::{debug, warn};
use regex::Regex;
use serde_json::Value;

use crate::{CObject, Filter, LogMessage};
use crate::error::SyncError;

pub struct Json;

#[async_trait(? Send)]
impl Filter for Json {
    async fn process(&self, mut data: Vec<LogMessage>) -> Result<Vec<LogMessage>, SyncError> {
        for mut x in &mut data {
            let json_value: Value = serde_json::from_str(&x.body)?;
            let map = json_value.as_object().ok_or(SyncError::Option)?;
            let log = map.get("log").ok_or(SyncError::Option)?.as_str().ok_or(SyncError::Option)?.trim().to_string();
            debug!("[Log] {}", &log);
            x.log = Some(log);
            x.map = Some(map.to_owned());
        }
        Ok(data)
    }
}

pub struct Regular {
    regex: Regex,
    mapping: Vec<String>,
}

impl Regular {
    pub fn create(conf: &HashMap<String, CObject>) -> Result<Regular, SyncError> {
        let conf: Option<HashMap<String, CObject>> = conf.get("parser")
            .ok_or(SyncError::MissingParams("Environment variable 'parser' could not be found."))?.into();
        let conf = conf
            .ok_or(SyncError::MissingParams("Environment variable 'parser' could not be found."))?;
        let regex: String = conf.get("regex")
            .ok_or(SyncError::MissingParams("Environment variable 'parser.regex' could not be found."))?.into();
        let mapping: String = conf.get("mapping")
            .ok_or(SyncError::MissingParams("Environment variable 'parser.mapping' could not be found."))?.into();
        debug!("[Regex] Expression : {}", regex);
        Ok(Regular { regex: Regex::new(&regex)?, mapping: mapping.split(",").map(|it| it.trim().to_owned()).collect() })
    }
    pub fn regex(&self, text: &str) -> Vec<String> {
        return match self.regex.captures(text) {
            None => {
                warn!("{}", text);
                Vec::default()
            }
            Some(cap) => {
                let mut values: Vec<String> = cap.iter().map(|c| {
                    match c {
                        None => String::default(),
                        Some(val) => val.as_str().to_owned()
                    }
                }).collect();
                if values.len() > 0 {
                    values.remove(0);
                }
                debug!("[Regex] Analysis Results : {:?}", values);
                values
            }
        };
    }
}

#[async_trait(? Send)]
impl Filter for Regular {
    async fn process(&self, mut data: Vec<LogMessage>) -> Result<Vec<LogMessage>, SyncError> {
        for mut x in &mut data {
            match &mut x.map {
                None => continue,
                Some(attr) => {
                    let text = &x.log.to_owned().ok_or(SyncError::Option)?;
                    let values = self.regex(text);
                    if self.mapping.len() >= values.len() {
                        for (index, key) in self.mapping.iter().enumerate() {
                            let value = values.get(index).ok_or(SyncError::Option)?;
                            attr.insert(key.to_owned(), Value::String(value.to_owned()));
                        }
                    }
                }
            }
        }
        Ok(data)
    }
}





