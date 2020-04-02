use futures_util::{future::FutureExt, stream::StreamExt, task::LocalSpawnExt};
use lapin::{
    options::*, types::FieldTable, BasicProperties, Connection, ConnectionProperties,
};
use log::info;
use std::collections::HashMap;


#[derive(Clone, Debug)]
struct Queue {
    name: String,
    options: lapin::options::QueueDeclareOptions,
    arguments: lapin::types::FieldTable,
}

impl Queue {
    pub fn new(name: &str) -> Queue {
        Self::builder(name).build()
    }
    pub fn builder(name: &str) -> QueueBuilder {
        QueueBuilder::new(name)
    }
}

struct QueueBuilder {
    queue: Queue
}

impl QueueBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            queue: Queue {
                name: String::from(name),
                options: lapin::options::QueueDeclareOptions::default(),
                arguments: lapin::types::FieldTable::default(),
            }
        }
    }
    pub fn passive(&mut self, value: bool) -> &mut Self {
        self.queue.options.passive = value;
        self
    }
    pub fn durable(&mut self, value: bool) -> &mut Self {
        self.queue.options.durable = value;
        self
    }
    pub fn exclusive(&mut self, value: bool) -> &mut Self {
        self.queue.options.exclusive = value;
        self
    }
    pub fn auto_delete(&mut self, value: bool) -> &mut Self {
        self.queue.options.auto_delete = value;
        self
    }
    pub fn nowait(&mut self, value: bool) -> &mut Self {
        self.queue.options.nowait = value;
        self
    }
    pub fn argument(&mut self, key: &str, value: lapin::types::AMQPValue) -> &mut Self {
        self.queue.arguments.insert(lapin::types::ShortString::from(key), value);
        self
    }
    pub fn build(self) -> Queue {
        self.queue
    }
}

#[derive(Debug)]
struct Exchange {
    name: String,
    kind: lapin::ExchangeKind,
    options: lapin::options::ExchangeDeclareOptions,
    arguments: lapin::types::FieldTable,
}



struct ChannelWrapper {
    channel: lapin::Channel,
    //queues: HashMap<Queue, lapin::Queue>,
    //exchanges: HashMap<Exchange,
}

impl ChannelWrapper {
    pub fn new(channel: lapin::Channel) -> Self {
        Self {
            channel,
            //queues: HashMap::default(),
        }
    }
    pub async fn queue_declare(&self, queue: &Queue) -> Result<(), lapin::Error> {
        self.channel
            .queue_declare(
                queue.name.as_str(),
                queue.options.clone(),
                queue.arguments.clone()
            )
            .await?;
        info!("Declared queue {:?}", queue.name);
        Ok(())
    }
    pub async fn consume(&self, queue: &Queue) -> Result<ConsumerWrapper, lapin::Error> {
        self.queue_declare(&queue).await?;
        let consumer = self.channel.basic_consume(
            queue.name.as_str(),
            "my_consumer", // FIXME autogenerate this name somehow
            BasicConsumeOptions::default(),
            FieldTable::default()
        )
        .await?;
        Ok(ConsumerWrapper {
            channel: self.channel.clone(),
            consumer,
        })
    }
    pub async fn publish(&self, queue: &Queue) -> Result<PublisherWrapper, lapin::Error> {
        self.queue_declare(&queue).await?;
        Ok(PublisherWrapper {
            channel: self.channel.clone(),
            queue: queue.clone(),
        })
    }
}

struct ConsumerWrapper {
    channel: lapin::Channel,
    consumer: lapin::Consumer,
}

impl ConsumerWrapper {
    pub async fn next(&mut self) -> Option<Result<lapin::message::Delivery, lapin::Error>> {
        self.consumer.next().await
    }
    pub async fn next_ack(&mut self) -> Option<Result<lapin::message::Delivery, lapin::Error>> {
        let delivery = self.next().await?;
        if let Ok(delivery) = delivery {
            if let Err(e) = self.channel.basic_ack(delivery.delivery_tag, BasicAckOptions::default()).await {
                return Some(Err(e));
            }
            Some(Ok(delivery))
        } else {
            None
        }
    }
}

struct PublisherWrapper {
    channel: lapin::Channel,
    queue: Queue,
}

impl PublisherWrapper {
    pub async fn publish(&self, payload: Vec<u8>) -> Result<lapin::publisher_confirm::PublisherConfirm, lapin::Error> {
        self.channel.basic_publish(
            "",
            self.queue.name.as_str(),
            BasicPublishOptions::default(),
            payload.to_vec(),
            BasicProperties::default(),
        ).await
    }
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();

    let queue = Queue::new("hello");

    let addr = std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".into());

    let conn = Connection::connect(&addr, ConnectionProperties::default()).await?;

    info!("CONNECTED");

    let channel_b = conn.create_channel().await?;
    let channel_b = ChannelWrapper::new(channel_b);
    let mut consumer = channel_b.consume(&queue).await?;

    tokio::spawn(async move {
        info!("will consume");
        loop {
            match consumer.next_ack().await {
                Some(Ok(delivery)) => {
                    info!("consumed!");
                },
                Some(Err(e)) => {
                    panic!(e);
                }
                None => {
                    info!("end of stream");
                },
            }
        }
    });

    let channel_a = conn.create_channel().await?;
    let channel_a = ChannelWrapper::new(channel_a);
    let publisher = channel_a.publish(&queue).await?;
    let payload = b"Hello world!";

    loop {
        info!("publishing...");
        publisher.publish(payload.to_vec()).await?;
    }
}