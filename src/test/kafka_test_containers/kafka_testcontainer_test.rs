use rdkafka::consumer::Consumer;
use rdkafka::error::KafkaResult;
use rdkafka::metadata::{Metadata};
use rdkafka::util::Timeout;
use testcontainers::clients::Cli;
use crate::test::kafka_test_containers::start_kafka_container;


#[tokio::test]
async fn test_kafka_testcontainer_starts() {
    let cli = Cli::default();
    println!("Starting kafka...");

    let container = start_kafka_container(vec!["one", "two", "three"], &cli).await;
    let mut kafka_client_provider = container.kafka_client_provider.unwrap();

    let consumer = kafka_client_provider.get_consumer(vec!["one", "two"])
        .await.unwrap();
    consumer.subscribe(&["one", "two"]).unwrap();

    let m: KafkaResult<Metadata> = consumer.client().fetch_metadata(None, Timeout::Never);

    let topics = m.unwrap().topics()
        .iter()
        .map(|f| f.name().to_string())
        .collect::<Vec<String>>();

    assert!(topics.contains(&String::from("one")));
    assert!(topics.contains(&String::from("two")));
    assert!(topics.contains(&String::from("three")));
    assert_eq!(topics.len(), 3);

}
