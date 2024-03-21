use std::env;
use std::future::Future;
use std::time::Duration;

use async_trait::async_trait;
use log::{debug, info, warn};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::BorrowedMessage;
use tokio::{spawn, time};
use tokio::sync::mpsc::{Receiver, Sender};

use crate::{LogMessage, MessageCallback, SourceEnum};

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
            log: None,
            map: None,
        }
    }
}


#[async_trait(? Send)]
pub trait ReceiveTrait {
    async fn pull(&mut self, sender: Sender<Vec<LogMessage>>);
}

pub struct Kafka;

#[async_trait(? Send)]
impl ReceiveTrait for Kafka {
    async fn pull(&mut self, sender: Sender<Vec<LogMessage>>) {
        info!("Welcome to Kafka Synchronization ...");
        let server = env::var("receive.kafka.server").expect("Environment variable 'receive.kafka.server' could not be found.");
        let topic = env::var("receive.kafka.topic").expect("Environment variable 'receive.kafka.topic' could not be found.");
        let group_id = env::var("receive.kafka.group_id").expect("Environment variable 'receive.kafka.group_id' could not be found.");
        let size: usize = env::var("receive.kafka.size").expect("Environment variable 'receive.kafka.size' could not be found.").parse().unwrap();
        let timeout: u64 = env::var("receive.kafka.timeout").expect("Environment variable 'receive.kafka.timeout' could not be found.").parse().unwrap();

        info!("[Kafka] Server: {}, Topic: {}, GroupId: {}", server, topic, group_id);

        // Create kafka Config.
        let mut consumer_config = ClientConfig::new();
        consumer_config
            .set("group.id", group_id)
            .set("bootstrap.servers", server)
            .set("auto.offset.reset", "latest")
            .set("enable.auto.commit", "true");

        // Kafka Consumer.
        let consumer: StreamConsumer = consumer_config.create().expect("Consumer creation failed");

        // Kafka Subscribe.
        consumer.subscribe(&[&topic]).expect("Can't subscribe to specified topic");

        // let mut tp_list = TopicPartitionList::new();
        // tp_list.add_partition(&topic, 0).set_offset(Offset::Offset(101091379)).expect("TODO: panic message");
        // consumer.assign(&tp_list).expect("Can't assign partitions");

        loop {
            let mut vec = Vec::new();
            tokio::select! {
                data = kafka_recv(&consumer, size, &mut vec) => {
                    debug!("[kafka] Number of triggers. ");
                }
                _ = time::sleep(Duration::from_millis(timeout)) => {
                    debug!("[kafka] Time of triggers. ");
                }
            }
            if vec.len() > 0 {
                sender.send(vec).await.unwrap();
            }
        }
    }
}


async fn kafka_recv(consumer: &StreamConsumer, length: usize, message_vec: &mut Vec<LogMessage>) {
    loop {
        match consumer.recv().await {
            Ok(message) => {
                //consumer.commit_message(&message, CommitMode::Sync).unwrap();
                println!("================>>>  {}", message.offset());
                message_vec.push(message.into());
                if message_vec.len() >= length {
                    return;
                }
            }
            Err(_) => {}
        };
    }
}

pub fn create_instance(source: SourceEnum) -> Box<dyn ReceiveTrait> {
    info!("[Source] Initialize {:?} instance.", source);
    Box::new(Kafka {})
}