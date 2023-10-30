use serde::Deserialize;
use std::sync::{Arc, Mutex};
use rdkafka::consumer::StreamConsumer;


#[derive(Deserialize, Debug)]
pub struct KafkaConfiguration {
    pub(crate) hosts: Vec<String>,
    pub(crate) consumer_group_id: String,
    pub(crate) client_id: String
}

impl Default for KafkaConfiguration {
    fn default() -> Self {
        Self {
            hosts: vec!["localhost:9092".to_string()],
            consumer_group_id: "consumer".to_string(),
            client_id: "nn-fe".to_string()
        }
    }
}


#[derive(Default, Clone)]
pub struct KafkaConsumerContainer {
    pub consumers: Arc<Mutex<Vec<(Arc<StreamConsumer>, Vec<String>)>>>
}
