use std::thread::sleep;
use std::time::Duration;
use knockoff_logging::info;
use rdkafka::util::TokioRuntime;
use serde::{Deserialize, Serialize};
use crate::data_publisher::{DataPublisher, MessageSource, MessageSourceImpl};
use crate::kafka_data_publisher::{KafkaDataPublisher, KafkaSenderHandle};
use crate::{EventReceiver, NetworkEvent};

use knockoff_logging::knockoff_logging::default_logging::StandardLoggingFacade;
use knockoff_logging::knockoff_logging::logging_facade::LoggingFacade;
use knockoff_logging::knockoff_logging::log_level::LogLevel;
use knockoff_logging::knockoff_logging::logger::Logger;

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct TestEvent {
    test_value: String
}

impl NetworkEvent for TestEvent {
    fn topic_matcher() -> &'static str {
        "test*"
    }

    fn publish_topics() -> Vec<&'static str> {
        vec!["test_one", "test_two"]
    }
}

#[tokio::test]
async fn test_publisher() {
    info!("Testing publisher");
    let mut kafka_sender_handle = KafkaSenderHandle::default_value();
    let runtime = TokioRuntime {};
    let mut message_source: MessageSourceImpl<TestEvent> = MessageSourceImpl::new();
    let mut event_receiver = message_source.initialize();
    KafkaDataPublisher::publish(
        kafka_sender_handle, TokioRuntime{},
        TokioRuntime{}, event_receiver
    );
    for _ in 0..100 {
        info!("Sending message");
        tokio::time::sleep(Duration::from_secs(2)).await;
        message_source.send(TestEvent { test_value: "hello!".to_string() }).await.unwrap();
    }
}