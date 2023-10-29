use crate::{ConsumerSink, NetworkEvent};
use crate::config::MessageClientProvider;
use crate::receiver::ReceiverHandler;

pub mod kafka_data_subscriber;

pub use kafka_data_subscriber::*;

pub trait DataSubscriber<E,
    MessageClientProviderT, EventReceiverHandlerT, ConsumerSinkT, ClientT,
>
    where
        E: NetworkEvent + 'static,
        MessageClientProviderT: MessageClientProvider<ClientT>,
        EventReceiverHandlerT: ReceiverHandler<E>,
        ConsumerSinkT: ConsumerSink,
        ClientT: 'static
{
    fn subscribe(
        consumer: MessageClientProviderT,
        receiver_handler: EventReceiverHandlerT,
        sink: ConsumerSinkT
    );
}
