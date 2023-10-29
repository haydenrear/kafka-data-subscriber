use std::time::Duration;
use crate::config::KafkaClientProvider;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;

use testcontainers::clients::Cli;
use testcontainers::core::WaitFor;
use testcontainers::{Container, Image};
use crate::test::kafka_test_containers::kafka_image::Kafka;

pub(crate) mod kafka_image;

mod kafka_testcontainer_test;

pub(crate) async fn start_kafka_container<'a>(topics: Vec<&str>, cli: &'a Cli) -> KafkaContainer<'a> {
    let mut kafka_container_create = KafkaContainer::default(cli);
    let port = kafka_container_create.port;
    let client_provider = KafkaClientProvider::new(port);
    kafka_container_create.kafka_client_provider = Some(client_provider);
    create_topics(kafka_container_create.kafka_client_provider.as_ref().unwrap().get_admin_client().as_ref().unwrap(), topics).await;
    kafka_container_create
}

pub(crate) async fn create_topics(admin: &AdminClient<DefaultClientContext>, topics: Vec<&str>) {
    let new_topics = topics.iter()
        .map(|topic| NewTopic::new(topic, 1, TopicReplication::Fixed(1)))
        .collect::<Vec<NewTopic>>();
    let admin_opts = AdminOptions::default();
    let _ = admin.create_topics(new_topics.as_slice(), &admin_opts).await
        .map(|topic_creation| {
            print!("{:?} is topic result", &topic_creation);
        })
        .or_else(|e| {
            println!("Error creating topics: {:?}", e);
            Err(e)
        });
}

pub struct KafkaContainer<'a> {
    pub(crate) container: Container<'a, Kafka>,
    pub(crate) port: u16,
    pub(crate) cli: &'a Cli,
    pub(crate) kafka_client_provider: Option<KafkaClientProvider>
}

impl <'a> KafkaContainer<'a>
{
    fn default(cli: &'a Cli) -> Self
    {
        let kafka = Kafka::default();
        kafka.ready_conditions().push(WaitFor::StdOutMessage {
            message: "started (kafka.server.KafkaServer)".to_string(),
        });
        let container = cli.run(kafka);
        let port = container.get_host_port_ipv4(9093);
        Self {
            container, port, cli, kafka_client_provider: Some(KafkaClientProvider::new(port))
        }
    }
}
