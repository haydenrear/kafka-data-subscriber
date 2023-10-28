#![feature(return_position_impl_trait_in_trait)]

use std::fmt::Debug;
use std::future::Future;
use rdkafka::util::TokioRuntime;
use serde::Deserialize;
use tokio::task::JoinHandle;
use tokio::sync::mpsc::{Receiver, Sender};

pub mod kafka_data_subscriber;
pub mod data_subscriber;
pub mod config;
pub mod data_publisher;
pub mod kafka_data_publisher;
mod kafka_client_provider;
pub mod receiver;
pub mod sender;

mod test;


pub trait ConsumerSink {

    /// Spawns a static future onto the thread pool. The returned Task is a future. It can also be
    /// canceled and "detached" allowing it to continue running without having to be polled by the
    /// end-user.
    ///
    /// If the provided future is non-`Send`, [`TaskPool::spawn_local`] should be used instead.
    fn spawn<T: Send + 'static>(&self, future: impl Future<Output = T> + Send + 'static);
}


pub trait JoinableConsumerSink {

    /// Spawns a static future onto the thread pool. The returned Task is a future. It can also be
    /// canceled and "detached" allowing it to continue running without having to be polled by the
    /// end-user.
    ///
    /// If the provided future is non-`Send`, [`TaskPool::spawn_local`] should be used instead.
    fn spawn<T, E>(&self, task: T) -> JoinHandle<E>
        where
            T: Future<Output = E> + Send + 'static,
            E: Send + Sync + 'static ;
}

impl JoinableConsumerSink for TokioRuntime {
    fn spawn<T, E>(&self, future: T) -> JoinHandle<E>
        where
            T: Future<Output=E> + Send + 'static,
            E: Send + Sync + 'static {
        tokio::spawn(future)
    }
}


impl ConsumerSink for TokioRuntime {
    fn spawn<T: Send + 'static>(&self, future: impl Future<Output=T> + Send + 'static) {
        tokio::spawn(future);
    }
}

pub trait NetworkEvent: for<'a> Deserialize<'a> + Send + Sync {
    fn topic_matcher() -> &'static str;
    fn publish_topics() -> Vec<&'static str>;
}


#[derive(Default)]
pub struct EventReceiver<T>
where
    T: NetworkEvent {
    pub receiver: Option<Receiver<T>>
}


#[derive(Default)]
pub struct EventSender<T> where
    T: NetworkEvent
{
    pub sender: Option<Sender<T>>
}