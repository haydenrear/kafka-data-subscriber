use std::error::Error;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use crate::{JoinableConsumerSink, NetworkEvent};

pub trait SenderHandle<E, R, Err, SinkT>: Send + Sync
    where
        E: NetworkEvent + Debug + 'static + Send + Sync,
        Err: Error + Send + Sync,
        SinkT: JoinableConsumerSink,
{
    fn send(
        &mut self,
        event: E, sink: Arc<SinkT>,
    ) -> impl Future<Output=Result<R, Err>> + Send;


}