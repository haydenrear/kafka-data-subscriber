use std::fs::metadata;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use rdkafka::util::{Timeout, TokioRuntime};
use serde::{Deserialize, Serialize};
use crate::data_publisher::{DataPublisher, MessageSource, MessageSourceImpl};
use crate::data_publisher::{KafkaDataPublisher, KafkaSenderHandle};
use crate::{EventReceiver, NetworkEvent};

use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{Message, Offset};
use testcontainers::clients::Cli;
use wait_for::WaitFor;
use crate::test::kafka_test_containers::start_kafka_container;


use std::sync::Mutex;
use knockoff_logging::*;
use crate::logger_lazy;

import_logger!("publisher_test.rs");

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

    let cli = Cli::default();
    println!("Starting kafka...");

    let container = start_kafka_container(TestEvent::publish_topics(), &cli).await;


    let mut kafka_client_provider = container.kafka_client_provider.unwrap();
    let consumer = kafka_client_provider.get_consumer(TestEvent::publish_topics()).await.unwrap();

    let mut kafka_sender_handle = KafkaSenderHandle::with_client_provider(kafka_client_provider);
    let mut message_source: MessageSourceImpl<TestEvent> = MessageSourceImpl::new();
    let mut event_receiver = message_source.initialize();

    let waited = wait_for::WaitFor::<()>::wait_for(
        Duration::from_secs(10),
        &|| consumer.fetch_metadata(None, Timeout::Never),
        &|metadata|
            metadata.as_ref().is_ok() && metadata.as_ref().unwrap().topics().len() == 2
    );

    assert!(waited);

    let waited = wait_for::WaitFor::<()>::wait_for(
        Duration::from_secs(10),
        &|| consumer.subscription(),
        &|subscription|
            subscription.as_ref().is_ok() && subscription.as_ref().unwrap().elements().len() == 2
    );
    assert!(waited);

    KafkaDataPublisher::publish(
        kafka_sender_handle, TokioRuntime{},
        TokioRuntime{}, event_receiver
    );

    message_source.send(TestEvent { test_value: "hello!".to_string() }).await.unwrap();

    let waited_for = Arc::new(WaitFor { t: Some(consumer) });
    let matched = WaitFor::<StreamConsumer>::wait_for_matcher_async_multiple(
            waited_for,
        Duration::from_secs(10),
        &|matched: Option<TestEvent>| {
            if let Some(b) = matched {
                info!("Found test event: {}!", b.test_value);
                b.test_value == String::from("hello!")
            } else {
                info!("Did not find test event!");
                false
            }
        },
        &|consumer: Arc<WaitFor<StreamConsumer>>| Box::pin(async move {
            let out = consumer.t.as_ref().unwrap().recv().await;
            if out.as_ref().is_ok() {
                info!("Kafka result was ok.");
                let value = out.as_ref().unwrap();
                let payload = value.payload();
                if payload.is_some() {
                    return Some(serde_json::from_slice(payload.unwrap()).unwrap());
                }
            }
            info!("Kafka result was error: {:?}", &out.as_ref().err());
            None
        })).await;

    assert!(matched);

}