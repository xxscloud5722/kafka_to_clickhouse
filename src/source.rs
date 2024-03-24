use std::collections::HashMap;
use std::env;
use std::future::Future;
use std::time::Duration;

use async_trait::async_trait;
use config::ConfigBuilder;
use log::{debug, info, warn};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::BorrowedMessage;
use tokio::{spawn, time};
use tokio::sync::mpsc::{Receiver, Sender};

use crate::{CObject, LogMessage, ReceiveTrait};
use crate::error::SyncError;

impl From<BorrowedMessage<'_>> for LogMessage {
    fn from(message: BorrowedMessage) -> Self {
        let payload = match message.payload_view::<str>() {
            None => "",
            Some(Ok(v)) => v,
            Some(Err(e)) => {
                warn!("Error while deserializing message payload: {:?}", e);
                ""
            }
        };

        LogMessage {
            topic: message.topic().to_string(),
            body: payload.to_string(),
            partition: message.partition(),
            offset: message.offset(),
            log: None,
            map: None,
        }
    }
}

pub struct Kafka {
    server: String,
    topic: String,
    group_id: String,
    size: usize,
    timeout: u64,
    consumer: StreamConsumer,
}

impl Kafka {
    pub fn create(conf: &HashMap<String, CObject>) -> Result<Kafka, SyncError> {
        info!("Welcome to Kafka Synchronization ...");
        let conf: Option<HashMap<String, CObject>> = conf.get("receive").ok_or(SyncError::Option)?.into();
        let conf: Option<HashMap<String, CObject>> = conf.ok_or(SyncError::Option)?.get("kafka").ok_or(SyncError::Option)?.into();
        let conf = conf.ok_or(SyncError::Option)?;

        let server: String = conf.get("server")
            .ok_or(SyncError::MissingParams("Environment variable 'receive.kafka.server' could not be found."))?.into();
        let topic: String = conf.get("topic")
            .ok_or(SyncError::MissingParams("Environment variable 'receive.kafka.topic' could not be found."))?.into();
        let group_id: String = conf.get("group_id")
            .ok_or(SyncError::MissingParams("Environment variable 'receive.kafka.group_id' could not be found."))?.into();
        let username: String = conf.get("username")
            .ok_or(SyncError::MissingParams("Environment variable 'receive.kafka.username' could not be found."))?.into();
        let password: String = conf.get("password")
            .ok_or(SyncError::MissingParams("Environment variable 'receive.kafka.password' could not be found."))?.into();
        let size: f64 = conf.get("size")
            .ok_or(SyncError::MissingParams("Environment variable 'receive.kafka.size' could not be found."))?.into();
        let timeout: f64 = conf.get("timeout")
            .ok_or(SyncError::MissingParams("Environment variable 'receive.kafka.timeout' could not be found."))?.into();

        info!("[Kafka] Server: {}, Topic: {}, GroupId: {}", server, topic, group_id);

        // Create kafka Config.
        let mut consumer_config = ClientConfig::new();
        consumer_config
            .set("group.id", &group_id)
            .set("bootstrap.servers", &server)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .set("sasl.mechanisms", "PLAIN")
            .set("security.protocol", "SASL_PLAINTEXT")
            .set("sasl.username", username)
            .set("sasl.password", password);

        // Kafka Consumer.
        let consumer: StreamConsumer = consumer_config.create()?;

        // Kafka Subscribe.
        consumer.subscribe(&[&topic.to_owned()])?;

        Ok(Kafka { server, topic, group_id, size: size as usize, timeout: timeout as u64, consumer })
    }

    pub async fn kafka_recv(&self, message_vec: &mut Vec<LogMessage>) -> Result<(), SyncError> {
        loop {
            let message = self.consumer.recv().await?;
            debug!("[Kafka] Recv offset: {:?}", message.offset());
            self.consumer.store_offset_from_message(&message)?;
            message_vec.push(message.into());
            if message_vec.len() >= self.size {
                return Ok(());
            }
        }
    }
}

#[async_trait(? Send)]
impl ReceiveTrait for Kafka {
    async fn pull(&self) -> Result<Vec<LogMessage>, SyncError> {
        loop {
            let mut vec = Vec::new();
            tokio::select! {
                _ = self.kafka_recv(&mut vec) => {
                    debug!("[kafka] Number of triggers. ");
                }
                _ = time::sleep(Duration::from_millis(self.timeout)) => {
                    debug!("[kafka] Time of triggers. ");
                }
            }
            if vec.len() > 0 {
                return Ok(vec);
            }
        }
    }

    async fn confirm(&self) -> Result<(), SyncError> {
        self.consumer.commit_consumer_state(CommitMode::Sync)?;
        Ok(())
    }
}


