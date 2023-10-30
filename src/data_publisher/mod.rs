use std::error::Error;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use crate::config::MessageClientProvider;
use crate::{ConsumerSink, EventSender, JoinableConsumerSink, NetworkEvent};
use crate::receiver::ReceiverHandler;
use crate::sender::SenderHandle;

use tokio::sync::mpsc::error::SendError;
use crate::EventReceiver;
use std::sync::Mutex;
use knockoff_logging::*;
use crate::logger_lazy;

import_logger!("data_publisher.rs");

pub mod kafka_data_publisher;

pub use self::kafka_data_publisher::*;

pub trait DataPublisher<E,
    SenderHandlerT, EventReceiverHandlerT,
    ConsumerSinkT, JoinableConsumerSinkT, SenderResultT, SenderResultErrorT
>
    where
        E: NetworkEvent + Debug + 'static + Send + Sync,
        SenderHandlerT: SenderHandle<E, SenderResultT, SenderResultErrorT, JoinableConsumerSinkT> + Send + Sync + 'static,
        EventReceiverHandlerT: ReceiverHandler<E> + Send + Sync + 'static,
        ConsumerSinkT: ConsumerSink + Send + Sync + 'static,
        JoinableConsumerSinkT: JoinableConsumerSink + Send + Sync + 'static,
        SenderResultErrorT: Error + Debug + Send + Sync,
        SenderResultT: Send + Sync + 'static,
{
    fn publish(
        mut sender_handler: SenderHandlerT,
        mut consumer_sink: ConsumerSinkT,
        joinable_consumer_sink: JoinableConsumerSinkT,
        mut receiver_handler: EventReceiverHandlerT
    ) {
        assert!(receiver_handler.ready());
        let joinable_sink = Arc::new(joinable_consumer_sink);
        consumer_sink.spawn(async move {
            loop {
                match receiver_handler.recv().await {
                    Some(event) => {
                        let _ = sender_handler.send(event, joinable_sink.clone())
                            .await
                            .or_else(|e| {
                                error!("Error sending event: {:?}.", e);
                                Err(e)
                            });
                    },
                    None => {
                        todo!()
                    }
                }
            }
        });
    }
}

pub trait MessageSource<E>
    where
        E: NetworkEvent + Debug + 'static + Send + Sync,
{
    async fn send(&mut self, network_event: E) -> Result<(), SendError<E>> {
        self.sender().sender.as_mut().unwrap().send(network_event).await
    }

    fn initialize(&mut self) -> EventReceiver<E> {
        let (mut tx, mut rx) = tokio::sync::mpsc::channel::<E>(16);
        self.set_sender(EventSender{sender: Some(tx)});
        EventReceiver {receiver: Some(rx)}
    }

    fn new() -> Self;

    fn set_sender(&mut self, event_sender: EventSender<E>);

    fn sender(&mut self) -> &mut EventSender<E>;

}

pub struct MessageSourceImpl<E>
    where
        E: NetworkEvent + Debug + 'static + Send + Sync {
    event_sender: Option<EventSender<E>>
}

impl<E> MessageSource<E> for MessageSourceImpl<E>
    where E: NetworkEvent + Debug + 'static + Send + Sync
{
    fn new() -> Self {
        Self {
            event_sender: None
        }
    }

    fn set_sender(&mut self, event_sender: EventSender<E>) {
        std::mem::swap(&mut self.event_sender, &mut Some(event_sender));
    }

    fn sender(&mut self) -> &mut EventSender<E> {
        if self.event_sender.is_none() {
            panic!("Attempted to access event sender without setting it first.");
        }
        self.event_sender.as_mut().unwrap()
    }
}
