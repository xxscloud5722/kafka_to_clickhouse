use async_trait::async_trait;
use tokio::sync::mpsc::Receiver;

use crate::{Filter, LogMessage, SinkEnum};

#[async_trait(? Send)]
pub trait SendTrait {
    async fn push(&self, f: &Vec<Box<dyn Filter>>, rx: Receiver<Vec<LogMessage>>);
}


struct Clickhouse;

#[async_trait(? Send)]
impl SendTrait for Clickhouse {
    async fn push(&self, filters: &Vec<Box<dyn Filter>>, mut rx: Receiver<Vec<LogMessage>>) {
        loop {
            while let Some(mut message) = rx.recv().await {
                for f in filters {
                    message = f.process(message).await;
                }
                // 写入到Clickhouse
                // println!("{:?}", message);
            }
        }
    }
}


pub fn create_instance(sink: &SinkEnum) -> Box<dyn SendTrait> {
    Box::new(Clickhouse)
}