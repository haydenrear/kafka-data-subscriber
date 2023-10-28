use std::future::Future;
use std::process::Output;
use tokio::sync::mpsc::Receiver;
use crate::EventReceiver;
use crate::NetworkEvent;

pub trait ReceiverHandler<T: Send + Sync + 'static>: Send + Sync + 'static {
    fn recv(&mut self) -> impl Future<Output = Option<T>> + Send;
    fn initialize(&mut self, t: &mut Option<Receiver<T>>);
    fn ready(&self) -> bool;
}

impl<T> ReceiverHandler<T> for EventReceiver<T>
    where
        T: NetworkEvent + Send + Sync + 'static
{
    async fn recv(&mut self) -> Option<T>
    {
        let mut_receiver = self.receiver.as_mut();
        if mut_receiver.is_none() {
            None
        } else {
            mut_receiver.unwrap().recv().await
        }
    }

    fn initialize(&mut self, t: &mut Option<Receiver<T>>) {
        std::mem::swap(&mut self.receiver, t)
    }

    fn ready(&self) -> bool {
        self.receiver.is_some()
    }
}
