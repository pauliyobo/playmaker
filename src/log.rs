use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Logger for a workflow
/// The logger will be responsible for receiving data from all jobs and it will broadcast them to interested consumers
/// Such a consumer can for example be an API endpoint which will stream realtime logs for a particular job
#[derive(Clone, Debug)]
pub struct Logger {
    topics: Arc<DashMap<String, broadcast::Sender<String>>>,
}

impl Logger {
    pub fn new() -> Self {
        Self {
            topics: Arc::new(DashMap::new()),
        }
    }

    /// subscribe to a topic
    pub fn subscribe(&self, topic: &str) -> broadcast::Receiver<String> {
        if let Some(topic) = self.topics.get(topic) {
            return topic.subscribe();
        }
        // we use 500 as upper bound, not really sure if it needs to be lower
        let (tx, rx) = broadcast::channel(500);
        self.topics.insert(topic.to_string(), tx);
        rx
    }

    pub fn log(&self, topic: &str, message: &str) -> anyhow::Result<()> {
        println!("{message}");
        if let Some(item) = self.topics.get(topic) {
            item.send(message.into())?;
        }
        Ok(())
    }
}
