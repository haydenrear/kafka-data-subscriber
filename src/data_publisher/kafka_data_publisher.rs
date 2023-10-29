use std::default::Default;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use knockoff_logging::{error, info};
use crate::data_publisher::DataPublisher;
use crate::config::{KafkaClientProvider, MessageClientProvider};
use crate::{ConsumerSink, JoinableConsumerSink, NetworkEvent};
use crate::sender::SenderHandle;

use knockoff_logging::knockoff_logging::default_logging::StandardLoggingFacade;
use knockoff_logging::knockoff_logging::logging_facade::LoggingFacade;
use knockoff_logging::knockoff_logging::log_level::LogLevel;
use knockoff_logging::knockoff_logging::logger::Logger;
use rdkafka::config::FromClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::message::OwnedMessage;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::{Timeout, TokioRuntime};
use serde::Serialize;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use crate::EventReceiver;
use crate::receiver::ReceiverHandler;

pub struct KafkaMessagePublisher<
    E, EventReceiverHandlerT, ConsumerSinkT
>
    where
        E: NetworkEvent + 'static + Debug,
        EventReceiverHandlerT: SenderHandle<E, KafkaPublishResult, AggregatedKafkaErrors, ConsumerSinkT>,
        ConsumerSinkT: JoinableConsumerSink,
{
    phantom: PhantomData<E>,
    phantom_1: PhantomData<EventReceiverHandlerT>,
    phantom_3: PhantomData<ConsumerSinkT>,
}

#[derive(Default)]
pub struct KafkaSenderHandle<E: NetworkEvent + Debug + 'static, JoinableConsumerSinkT: JoinableConsumerSink> {
    pub client_provider: KafkaClientProvider,
    pub producer: Vec<FutureProducer>,
    pub initialized: bool,
    pub phantom: PhantomData<JoinableConsumerSinkT>,
    pub phantom_1: PhantomData<E>
}

impl<E: NetworkEvent + Debug + 'static> KafkaSenderHandle<E, TokioRuntime> {
    pub fn default_value() -> Self {
        Self {
            client_provider: KafkaClientProvider::default(),
            phantom_1: PhantomData::default() ,
            phantom: PhantomData::default(),
            producer: vec![],
            initialized: false,
        }
    }
}

impl<E: NetworkEvent + Debug + 'static, JoinableConsumerSinkT: JoinableConsumerSink>
KafkaSenderHandle<E, JoinableConsumerSinkT> {
    pub async fn initialize(&mut self) {
        if !self.initialized {
            info!("Initializing producers. {} producers existed before.", self.producer.len());
            for i in 0..self.client_provider.num_producers_per_event {
                self.producer.push(self.client_provider.get_producer::<E>().await.unwrap())
            }
            self.initialized = true;
            info!("Initialized producers. {} producers existed after.", self.producer.len());
        } else {
            info!("Producer was already initialized.");
        }
    }
}

#[derive(Debug, Default)]
pub struct AggregatedKafkaErrors {
    errors: Vec<(KafkaError, OwnedMessage)>,
    successes: Vec<(i32, i64)>
}

impl Display for AggregatedKafkaErrors {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl Error for AggregatedKafkaErrors {
}

#[derive(Default)]
pub struct KafkaPublishResult {
    msg: Vec<(i32, i64)>
}

impl<E, SinkT> SenderHandle<E, KafkaPublishResult, AggregatedKafkaErrors, SinkT> for KafkaSenderHandle<E, SinkT>
where E: NetworkEvent + Debug + Serialize + 'static, SinkT: JoinableConsumerSink + Send + Sync
{
    async fn send(&mut self, event: E, sink: Arc<SinkT>) -> Result<KafkaPublishResult, AggregatedKafkaErrors> {
        info!("Sending message");
        self.initialize().await;
        info!("Initialized!");
        let mut kafka_errs = AggregatedKafkaErrors::default();
        let mut kafka_publish_result = KafkaPublishResult::default();
        let mut join_handles: Vec<JoinHandle<(FutureProducer, Option<KafkaError>, Option<OwnedMessage>, Option<i32>, Option<i64>)>> = vec![];
        for (topic, event, key) in serde_json::to_string::<E>(&event)
            .into_iter()
            .flat_map(|event| E::publish_topics().iter()
                .map(|topic| (String::from(*topic), event.clone(), String::from(topic.clone())))
                .collect::<Vec<(String, String, String)>>()
            ).into_iter() {
            if self.producer.len() != 0 {
                self.spawn_from_producers(sink.clone(), &mut join_handles, &topic, &event, &key);
            } else if join_handles.len() != 0 {
                Self::spawn_from_join_handle(sink.clone(), &mut kafka_errs, &mut kafka_publish_result, &mut join_handles, &topic, &event, &key).await;
            } else {
                loop {
                    if self.producer.len() != 0 {
                        self.spawn_from_producers(sink.clone(), &mut join_handles, &topic, &event, &key);
                        break;
                    } else if join_handles.len() != 0 {
                        Self::spawn_from_join_handle(
                            sink.clone(), &mut kafka_errs, &mut kafka_publish_result,
                            &mut join_handles,
                            &topic, &event, &key).await;
                        break;
                    }
                }
            }
        }
        for jh in join_handles.into_iter() {
            let (producer_taken, err, msg, i, j) = jh.await.unwrap();
            Self::add_results(&mut kafka_errs, &mut kafka_publish_result, err, msg, i, j);
            self.producer.push(producer_taken);
        }
        if kafka_errs.errors.len() == 0 {
            Ok(kafka_publish_result)
        } else {
            std::mem::swap(&mut kafka_errs.successes, &mut kafka_publish_result.msg);
            Err(kafka_errs)
        }
    }
}


impl<E, SinkT> KafkaSenderHandle<E, SinkT> where E: 'static + Debug + NetworkEvent, SinkT: JoinableConsumerSink + Send + Sync {
    async fn spawn_from_join_handle(
        sink: Arc<SinkT>,
        kafka_errs: &mut AggregatedKafkaErrors,
        kafka_publish_result: &mut KafkaPublishResult,
        mut join_handles: &mut Vec<JoinHandle<(FutureProducer, Option<KafkaError>, Option<OwnedMessage>, Option<i32>, Option<i64>)>>,
        topic: &String,
        event: &String,
        key: &String
    )
    {
        let jh = join_handles.remove(0);
        let (producer_taken, err, msg, i, j) = jh.await.unwrap();
        Self::add_results(kafka_errs, kafka_publish_result, err, msg, i, j);
        Self::spawn_send_task(sink, &mut join_handles, topic, event, key,
                              producer_taken);
    }

    fn create_future_record<'a>(topic: &'a str, event: &'a String, key: &'a String) -> FutureRecord<'a, String, String> {
        FutureRecord::to(topic).key(key).payload(event)
    }

    fn add_results(kafka_errs: &mut AggregatedKafkaErrors,
                   kafka_publish_result: &mut KafkaPublishResult,
                   err: Option<KafkaError>, msg: Option<OwnedMessage>,
                   i: Option<i32>,
                   j: Option<i64>) {
        if err.is_some() {
            kafka_errs.errors.push((err.unwrap(), msg.unwrap()))
        } else {
            kafka_publish_result.msg.push((i.unwrap(), j.unwrap()));
        }
    }

    fn spawn_send_task(sink: Arc<SinkT>,
                       join_handles: &mut Vec<JoinHandle<(FutureProducer, Option<KafkaError>, Option<OwnedMessage>, Option<i32>, Option<i64>)>>,
                       topic: &String,
                       event: &String,
                       key: &String,
                       mut producer_taken: FutureProducer) {
        let topic = topic.clone();
        let event = event.clone();
        let key = key.clone();

        let x = async move {
            let res = producer_taken
                .send(FutureRecord::to(topic.as_str()).payload(event.as_str()).key(key.as_str()),
                      Timeout::After(Duration::from_secs(3)))
                .await;
            if res.is_err() {
                let (kafka_err, msg) = res.unwrap_err();
                return (producer_taken, Some(kafka_err), Some(msg), None, None)
            } else {
                let (i, j) = res.unwrap();
                return (producer_taken, None, None, Some(i), Some(j));
            }
        };
        let handle = sink.spawn::<>(x);
        join_handles.push(handle);
    }

    fn spawn_from_producers(
        &mut self,
        sink: Arc<SinkT>,
        join_handles: &mut Vec<JoinHandle<(FutureProducer, Option<KafkaError>, Option<OwnedMessage>, Option<i32>, Option<i64>)>>,
        topic: &String,
        event: &String,
        key: &String
    ) {
        let mut producer_taken = self.producer.remove(0);
        Self::spawn_send_task(sink, join_handles,
                              topic, event, key,
                              producer_taken);
    }
}

#[derive(Default)]
pub struct KafkaDataPublisher<E>
    where
        E: NetworkEvent + Debug + 'static + Send + Sync,
{
    phantom_1: E,
}

impl<E> DataPublisher<
    E, KafkaSenderHandle<E, TokioRuntime>, EventReceiver<E>,
    TokioRuntime, TokioRuntime,
    KafkaPublishResult, AggregatedKafkaErrors,
> for KafkaDataPublisher<E>
    where
        E: NetworkEvent + Debug + 'static + Send + Sync + Serialize {}

