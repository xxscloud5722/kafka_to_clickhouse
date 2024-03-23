use async_trait::async_trait;

use crate::{LogMessage, SendTrait};
use crate::error::SyncError;

pub struct Clickhouse;

#[async_trait(? Send)]
impl SendTrait for Clickhouse {
    async fn push(&self, message: Vec<LogMessage>) -> Result<(), SyncError> {
        Ok(())
    }
}