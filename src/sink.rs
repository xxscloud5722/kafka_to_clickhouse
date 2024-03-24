use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{NaiveDateTime, ParseResult};
use clickhouse::{Client, Row};
use log::{debug, info};
use serde::{Serialize, Serializer};

use crate::{CObject, LogMessage, SendTrait};
use crate::error::SyncError;

pub struct Clickhouse {
    mapping: HashMap<String, String>,
    table: String,
    field: String,
    params: String,
    date_format: HashMap<String, String>,
    ck: Client,
}

impl Clickhouse {
    pub fn create(conf: &HashMap<String, CObject>) -> Result<Clickhouse, SyncError> {
        let conf: Option<HashMap<String, CObject>> = conf.get("sender")
            .ok_or(SyncError::MissingParams("Environment variable 'sender' could not be found."))?.into();
        let conf = conf
            .ok_or(SyncError::MissingParams("Environment variable 'sender' could not be found."))?;

        let mapping: Option<HashMap<String, CObject>> = conf.get("mapping")
            .ok_or(SyncError::MissingParams("Environment variable 'sender.mapping' could not be found."))?.into();
        let mapping = mapping
            .ok_or(SyncError::MissingParams("Environment variable 'sender.mapping' could not be found."))?;
        let mapping: HashMap<String, String> = mapping.iter().map(|(key, value)| (key.to_owned(), value.into())).collect();

        let date_format: Option<HashMap<String, CObject>> = conf.get("date-format")
            .ok_or(SyncError::MissingParams("Environment variable 'sender.date-format' could not be found."))?.into();
        let date_format = date_format
            .ok_or(SyncError::MissingParams("Environment variable 'sender.date-format' could not be found."))?;
        let date_format: HashMap<String, String> = date_format.iter().map(|(key, value)| (key.to_owned(), value.into())).collect();


        let conf: Option<HashMap<String, CObject>> = conf.get("clickhouse")
            .ok_or(SyncError::MissingParams("Environment variable 'sender.clickhouse' could not be found."))?.into();
        let clickhouse = conf
            .ok_or(SyncError::MissingParams("Environment variable 'sender.clickhouse' could not be found."))?;
        let database: String = clickhouse.get("database")
            .ok_or(SyncError::MissingParams("Environment variable 'sender.database' could not be found."))?.into();
        let table: String = clickhouse.get("table")
            .ok_or(SyncError::MissingParams("Environment variable 'sender.table' could not be found."))?.into();
        let server: String = clickhouse.get("server")
            .ok_or(SyncError::MissingParams("Environment variable 'sender.server' could not be found."))?.into();
        let username: String = clickhouse.get("username")
            .ok_or(SyncError::MissingParams("Environment variable 'sender.username' could not be found."))?.into();
        let password: String = clickhouse.get("password")
            .ok_or(SyncError::MissingParams("Environment variable 'sender.password' could not be found."))?.into();

        let field: Vec<String> = mapping.iter().map(|(key, value)| key.to_owned()).collect();
        let field_count = mapping.len();
        Ok(Clickhouse {
            mapping,
            table,
            field: field.join(", "),
            params: format!("({})", std::iter::repeat(String::from("?")).take(field_count).collect::<Vec<String>>().join(", ")),
            date_format,
            ck: Client::default()
                .with_url(server)
                .with_user(username)
                .with_password(password)
                .with_database(database),

        })
    }
}


#[derive(Row, Serialize, Debug)]
struct Log {
    date: String,
    system_code: String,
    env: String,
    service_code: String,
    trace_id: String,
    level: String,
    thread: String,
    class: String,
    message: String,
}

#[async_trait(? Send)]
impl SendTrait for Clickhouse {
    async fn push(&self, message: Vec<LogMessage>) -> Result<(), SyncError> {
        if message.is_empty() {
            return Ok(());
        }
        let params: Vec<String> = message.iter().map(|x| self.params.to_owned()).collect();
        let sql = format!("INSERT INTO {} ({}) VALUES", &self.table, self.field);
        debug!("[Clickhouse] {} ==> {} ...", message.len(), sql);
        let mut handler = self.ck.query(&format!("{}\n{}", sql, params.join(", ")));
        for x in &message {
            let data_item = x.map.clone().ok_or(SyncError::Option)?;
            for (key, data_key) in &self.mapping {
                let value = data_item.get(data_key).ok_or(SyncError::OptionParams(format!("params {}", data_key)))?
                    .as_str().unwrap_or("").to_owned();
                if self.date_format.contains_key(key) {
                    match self.date_format.get(key) {
                        Some(format) => {
                            handler = handler.bind(NaiveDateTime::parse_from_str(&value, format)
                                .unwrap_or(NaiveDateTime::default())
                                .format("%Y-%m-%d %H:%M:%S").to_string());
                            continue;
                        }
                        None => {}
                    }
                }
                handler = handler.bind(value);
            }
        }
        handler.execute().await?;
        let last = message.last().ok_or(SyncError::Option)?;
        info!("[Clickhouse] {} data pushes have been completed, and the current kafka offset value is {}",
            &message.len(), last.offset);
        Ok(())
    }
}