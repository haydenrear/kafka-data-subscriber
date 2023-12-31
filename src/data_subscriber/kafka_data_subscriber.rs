use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use rdkafka::Message;
use rdkafka::consumer::StreamConsumer;
use rdkafka::error::KafkaError;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time;
use crate::data_subscriber::DataSubscriber;
use crate::{ConsumerSink, EventReceiver, NetworkEvent};
use knockoff_tokio_util::run_blocking;

use crate::config::{KafkaClientProvider, MessageClientProvider};
use crate::receiver::ReceiverHandler;


use std::sync::Mutex;
use knockoff_logging::*;
use crate::logger_lazy;

import_logger!("kafka_data_subscriber.rs");

pub struct KafkaMessageSubscriber<E,
    EventReceiverHandlerT, KafkaClientProviderT, ConsumerSinkT,
>
    where
        E: NetworkEvent + 'static + Debug,
        EventReceiverHandlerT: ReceiverHandler<E>,
        KafkaClientProviderT: MessageClientProvider<KafkaClientProvider>,
        ConsumerSinkT: ConsumerSink
{
    phantom: PhantomData<E>,
    phantom_1: PhantomData<EventReceiverHandlerT>,
    phantom_2: PhantomData<KafkaClientProviderT>,
    phantom_3: PhantomData<ConsumerSinkT>,
}


impl <E,
    EventReceiverHandlerT, KafkaClientProviderT, ConsumerSinkT,
>
DataSubscriber<E,
    KafkaClientProviderT, EventReceiverHandlerT, ConsumerSinkT, KafkaClientProvider,
>
for KafkaMessageSubscriber<E,
    EventReceiverHandlerT, KafkaClientProviderT, ConsumerSinkT
>
    where
        E: NetworkEvent + 'static + Debug,
        EventReceiverHandlerT: ReceiverHandler<E>,
        KafkaClientProviderT: MessageClientProvider<KafkaClientProvider>,
        ConsumerSinkT: ConsumerSink,
{
    fn subscribe(
        mut consumer_config: KafkaClientProviderT,
        mut receiver_handler: EventReceiverHandlerT,
        consumer_sink: ConsumerSinkT
    )
    {

        let topics = vec![E::topic_matcher()];
        let mut consumers = vec![];

        info!("Creating consumers for topic matcher: {:?}.", E::topic_matcher());
        Self::create_consumers(&mut consumer_config, &topics, &mut consumers);

        info!("Initializing receiver handler for topic matcher: {:?}.", E::topic_matcher());
        let tx = Self::initialize_receiver_handle(&mut receiver_handler);

        info!("Initializing kafka subscribe for topics: {:?}.", E::topic_matcher());
        consumers.into_iter().for_each(|mut consumer| {
            let tx = tx.clone();
            consumer_sink.spawn(async move {
                info!("Created task to subscribe to messages.");
                let tx = tx.clone();
                loop {
                    match consumer.recv().await {
                        Ok(message_set) => {
                            if let Some(payload) = message_set.payload() {
                                let event = match serde_json::from_slice::<E>(payload) {
                                    Ok(event) => event,
                                    Err(e) => {
                                        error!("Error deserializing event: {:?}.", e);
                                        continue;
                                    }
                                };
                                let _ = tx.send(event)
                                    .await
                                    .or_else(|e| {
                                        error!("Error sending event: {}.", e.to_string());
                                        Err(e)
                                    });
                            }
                        },
                        Err(kafka_error) => {
                            error!("Error receiving consumer message: {:?}.", kafka_error);
                        }
                    }
                }
            });
        });

    }
}

impl<E, EventReceiverHandlerT, KafkaClientProviderT, ConsumerSinkT>
KafkaMessageSubscriber<E, EventReceiverHandlerT, KafkaClientProviderT, ConsumerSinkT>
    where
        ConsumerSinkT: ConsumerSink,
        E: 'static + Debug + NetworkEvent,
        EventReceiverHandlerT: ReceiverHandler<E>,
        KafkaClientProviderT: MessageClientProvider<KafkaClientProvider>
{
    fn create_consumers(consumer_config: &mut KafkaClientProviderT, topics: &Vec<&str>, mut consumers: &mut Vec<Arc<StreamConsumer>>) {
        let mut consumer_client = consumer_config.create_get_client();
        for _ in 0..consumer_client.num_consumers_per_event {
            run_blocking(async {
                let _ = consumer_client.get_consumer(topics.clone())
                    .await
                    .map(|consumer| {
                        info!("Created consumer for {:?}.", &topics);
                        consumers.push(Arc::new(consumer));
                    })
                    .or_else(|e| {
                        error!("Failed to create Kafka consumer for topic {:?}: {:?}", &topics, e);
                        Ok::<(), KafkaError>(())
                    })
                    .ok();
            })
        }

        let _ = consumer_client.consumers.consumers
            .lock()
            .map(|mut c| {
                let consumer = consumers.iter()
                    .map(|c| (c.clone(), vec![E::topic_matcher().to_string()]))
                    .collect::<Vec<(Arc<StreamConsumer>, Vec<String>)>>();
                c.extend(consumer);
            })
            .or_else(|e| {
                error!("Error saving kafka consumers: {:?}", e);
                Err(e)
            });
    }

    fn initialize_receiver_handle(receiver_handler: &mut EventReceiverHandlerT) -> Arc<Sender<E>> {
        let (mut tx, mut rx) = tokio::sync::mpsc::channel::<E>(16);

        let mut rx: Receiver<E> = rx;

        receiver_handler.initialize(&mut Some(rx));

        let tx = Arc::new(tx);
        tx
    }
}

pub fn write_events<E>
(
    event_writer: &mut dyn FnMut(E),
    receiver_handler: &mut EventReceiver<E>
)
    where E: NetworkEvent + 'static + Debug
{
    info!("Checking events.");
    if receiver_handler.receiver.is_none() {
        error!("Received event but there was no receiver handler set.");
        return;
    }
    knockoff_tokio_util::run_blocking(async {
        match time::timeout(Duration::from_secs(3), async {
            if let Some(event) = receiver_handler.receiver.as_mut().unwrap().recv().await {
                info!("{:?} is event.", &event);
                event_writer(event);
            }
        }).await {
            Ok(_) => {}
            Err(_) => {}
        }
    });
}
