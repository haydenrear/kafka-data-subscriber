use crate::{ConsumerSink, NetworkEvent};
use crate::kafka_client_provider::MessageClientProvider;
use crate::receiver::ReceiverHandler;

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