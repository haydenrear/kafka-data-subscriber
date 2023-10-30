use std::fmt::Debug;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication, TopicResult};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::producer::FutureProducer;
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
use std::time::Duration;
use rdkafka::config::FromClientConfig;
use crate::config::ConfigurationProperties;
use crate::config::KafkaConsumerContainer;

use std::sync::Mutex;
use crate::{NetworkEvent};
use crate::logger_lazy;
use knockoff_logging::*;
import_logger!("kafka_data_subscriber.rs");

pub struct KafkaClientProvider {
    pub kafka_client: Option<AdminClient<DefaultClientContext>>,
    pub hosts: Vec<String>,
    pub client_id: String,
    pub group_id: String,
    pub num_consumers_per_event: u8,
    pub num_producers_per_event: u8,
    pub consumers: KafkaConsumerContainer
}


impl Default for KafkaClientProvider {
    fn default() -> Self {
        let properties = ConfigurationProperties::read_config();
        info!("Read config: {:?}", &properties);
        let client_config = Self::admin_client_config_properties_set(&properties, properties.kafka.hosts.join(","));
        let mut kc = AdminClient::from_config(&client_config)
            .or_else(|e| {
                error!("Error connecting to kafka with AdminClient: {:?}", e);
                Err(e)
            })
            .ok();
        Self {
            kafka_client: kc,
            group_id: properties.kafka.consumer_group_id,
            client_id: properties.kafka.client_id,
            hosts: properties.kafka.hosts,
            num_consumers_per_event: 1,
            num_producers_per_event: 1,
            consumers: Default::default(),
        }
    }
}

impl KafkaClientProvider {
    pub async fn get_consumer(&mut self, topics: Vec<&str>) -> Result<StreamConsumer, KafkaError> {
        let client_config = self.admin_client_config_properties();

        let consumer: Result<StreamConsumer, KafkaError> =
            client_config.create_with_context(DefaultConsumerContext);

        let consumer = consumer.map(|consumer| {
            let topics_to_subscribe = Self::fetch_topic_patterns(&topics, &consumer);
            info!("Subscribing to topics: {:?}.", &topics);
            let subscribe_topics_slice = topics_to_subscribe.iter()
                .map(|t| t.as_str())
                .collect::<Vec<&str>>();

            Self::subscribe_to_topics(&consumer, subscribe_topics_slice.as_slice());

            consumer

        });
        consumer
    }

    pub async fn get_producer<E>(&self) -> Result<FutureProducer, KafkaError>
        where E: NetworkEvent + Send + Sync + 'static + Debug
    {
        let mut client_config = self.admin_client_config_properties();
        let admin_client = self.get_admin_client();
        if admin_client.is_none() {
            panic!("Error creating topics. Admin client did not exist!");
        } else {
            let admin_client = admin_client.as_ref().unwrap();
            let publish_topics = E::publish_topics().iter()
                .map(|t| NewTopic::new(*t, 1, TopicReplication::Fixed(1)))
                .collect::<Vec<NewTopic>>();
            info!("Creating topics: {:?}", &publish_topics);
            let _ = admin_client.create_topics(publish_topics.iter(), &AdminOptions::default()).await
                .map_err(|err| {
                    error!("Error creating topics: {:?}", &err);
                    Err::<Vec<TopicResult>, KafkaError>(err)
                })
                .map(|s| {
                    info!("Created topics: {:?}", &s);
                    Ok::<Vec<TopicResult>, KafkaError>(s)
                });
        }
        FutureProducer::from_config(&client_config)
    }

    pub(crate) fn subscribe_to_topics(consumer: &StreamConsumer, subscribe_topics_slice: &[&str]) {
        let _ = consumer.subscribe(subscribe_topics_slice)
            .or_else(|e| {
                error!("Error subscribing to topics: {:?}.", e);
                Err(e)
            });

        subscribe_topics_slice.iter().for_each(|topic| {
            let mut partitions = TopicPartitionList::new();
            partitions.add_partition(topic, 0);
            let _ = partitions.set_all_offsets(Offset::Beginning)
                .or_else(|e| {
                    error!("Error assigning partitions offset {}.", topic);
                    Err(e)
                });
            let _ = consumer.assign(&partitions)
                .or_else(|e| {
                    error!("Error assigning partitions for {}.", topic);
                    Err(e)
                });
        });
    }

    fn fetch_topic_patterns(topics: &Vec<&str>, consumer: &StreamConsumer) -> Vec<String> {
        let mut topic_to_subcribe = vec![];
        let _ = consumer.client()
            .fetch_metadata(None, rdkafka::util::Timeout::After(Duration::from_secs(3)))
            .map(|all_topics_metadata| {
                all_topics_metadata.topics().iter()
                    .filter(|topic| topics.iter()
                        .any(|topic_match| matches!(topic.name(), topic_match))
                    )
                    .map(|topic| topic.name().to_string())
                    .for_each(|topic| topic_to_subcribe.push(topic));
            })
            .or_else(|e| {
                error!("Could not fetch topics to determine which topics to subscribe: {:?}.", e);
                Err(e)
            });
        topic_to_subcribe
    }

    pub fn new(port: u16) -> Self {
        let properties = ConfigurationProperties::read_config();
        let mut client_config = Self::admin_client_config_properties_set(&properties, format!("localhost:{}", port));
        let mut kc = AdminClient::from_config(&client_config)
            .or_else(|e| {
                error!("Error connecting to kafka with AdminClient: {:?}", e);
                Err(e)
            })
            .ok();
        Self {
            kafka_client: kc,
            group_id: properties.kafka.consumer_group_id,
            client_id: properties.kafka.client_id,
            hosts: vec![format!("localhost:{}", port)],
            num_consumers_per_event: 1,
            num_producers_per_event: 1,
            consumers: Default::default(),
        }
    }

    pub fn get_admin_client(&self) -> &Option<AdminClient<DefaultClientContext>> {
        &self.kafka_client
    }

    fn admin_client_config_properties(&self) -> ClientConfig {
        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", self.hosts.join(","));
        client_config.set("group.id", self.group_id.clone());
        client_config.set("client.id", self.client_id.clone());
        client_config
    }

    fn admin_client_config_properties_set(config: &ConfigurationProperties, hosts: String) -> ClientConfig {
        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", hosts);
        client_config.set("group.id", config.kafka.consumer_group_id.clone());
        client_config.set("client.id", config.kafka.client_id.clone());
        client_config
    }
}

pub trait MessageClientProvider<Client> where Client: 'static {
    fn create_get_client<'a>(&'a mut self) -> &'a mut  Client where Self: 'a;
}
