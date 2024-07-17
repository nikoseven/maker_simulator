use std::{cell::Cell, rc::Rc, sync::Mutex, time::SystemTime};

use crossbeam::channel;
use upstair_type::{
    module::{
        CommsSystem, ModuleComms, ModuleCommsBuilder, ModuleId, ReadTopicHandle, TopicId,
        WriteTopicHandle,
    },
    time::{SimulationTime, TimeProvider},
    Message,
};

#[derive(Debug, Clone)]
struct SimulationTopicPublisher {
    destination: Vec<crossbeam::channel::Sender<Message>>,
    topic_updated_at: Rc<Cell<SystemTime>>,
}

pub struct SimulationModuleComms {
    time_priovider: SimulationTime,
    topic_readers: Vec<crossbeam::channel::Receiver<Message>>,
    topic_publisher: Vec<SimulationTopicPublisher>,
    is_world_running: Rc<Cell<bool>>,
}

impl ModuleComms for SimulationModuleComms {
    fn time(&self) -> SystemTime {
        self.time_priovider.time()
    }

    fn receive(&mut self, topic: &ReadTopicHandle) -> Option<Message> {
        let reader = &mut self.topic_readers[topic.slot];
        reader.try_recv().ok()
    }

    fn publish(&mut self, topic: &WriteTopicHandle, message: Message) {
        let writer = &mut self.topic_publisher[topic.slot];
        for writer in &writer.destination {
            writer.send(message.clone()).unwrap();
        }
        writer.topic_updated_at.replace(message.header.commit_at);
    }

    fn request_terminate(&mut self) {
        self.is_world_running.set(false);
    }
}

pub struct SimulationModuleCommsBuilder {
    module_id: ModuleId,
    system: Rc<Mutex<SimulationCommsSystemInner>>,

    topic_readers: Vec<crossbeam::channel::Receiver<Message>>,
}

impl ModuleCommsBuilder for SimulationModuleCommsBuilder {
    fn get_topic(&mut self, name: &str) -> TopicId {
        self.system.lock().unwrap().get_or_create_topic(name)
    }

    fn subscribe_topic(&mut self, topic: &TopicId) -> ReadTopicHandle {
        self.topic_readers.push(
            self.system
                .lock()
                .unwrap()
                .subscribe_topic(&self.module_id, topic),
        );
        ReadTopicHandle {
            slot: self.topic_readers.len() - 1,
        }
    }

    fn publish_topic(&mut self, topic: &TopicId) -> WriteTopicHandle {
        let publisher_slot = self
            .system
            .lock()
            .unwrap()
            .publish_topic(&self.module_id, topic);
        WriteTopicHandle {
            slot: publisher_slot,
        }
    }

    fn build(self) -> Box<dyn ModuleComms> {
        let inner = self.system.lock().unwrap();
        // build publisher
        let mut topic_publisher = Vec::new();
        for topic in &inner.modules[self.module_id.slot].write_topics {
            topic_publisher.push(inner.topics[topic.slot].publisher.clone());
        }
        Box::new(SimulationModuleComms {
            time_priovider: inner.time_provider.clone(),
            topic_readers: self.topic_readers,
            topic_publisher,
            is_world_running: inner.is_world_running.clone(),
        })
    }

    fn get_module_id(&self) -> &ModuleId {
        &self.module_id
    }
}

pub struct SimulationCommsSystem {
    pub inner: Rc<Mutex<SimulationCommsSystemInner>>,
    pub time_provider: SimulationTime,
    pub is_world_running: Rc<Cell<bool>>,
}

impl Default for SimulationCommsSystem {
    fn default() -> SimulationCommsSystem {
        let time_provider = SimulationTime::default();
        let is_world_running = Rc::new(Cell::new(true));
        SimulationCommsSystem {
            time_provider: time_provider.clone(),
            is_world_running: is_world_running.clone(),
            inner: Rc::new(Mutex::new(SimulationCommsSystemInner {
                topics: Vec::new(),
                modules: Vec::new(),
                time_provider,
                is_world_running,
            })),
        }
    }
}

impl SimulationCommsSystem {
    pub fn get_topic_reader(
        &mut self,
        topic_id: &TopicId,
    ) -> crossbeam::channel::Receiver<Message> {
        let mut inner = self.inner.lock().unwrap();
        let (tx, rx) = channel::unbounded();
        inner.topics[topic_id.slot].publisher.destination.push(tx);
        rx
    }

    pub fn get_all_topic_update_time(&self) -> Vec<Rc<Cell<SystemTime>>> {
        self.inner
            .lock()
            .unwrap()
            .topics
            .iter()
            .map(|x| x.publisher.topic_updated_at.clone())
            .collect()
    }

    pub fn get_module_subscribed_topics(&self) -> Vec<Vec<TopicId>> {
        self.inner
            .lock()
            .unwrap()
            .modules
            .iter()
            .map(|x| x.read_topics.clone())
            .collect()
    }

    pub fn get_topic_name(&self) -> Vec<String> {
        self.inner
            .lock()
            .unwrap()
            .topics
            .iter()
            .map(|x| x.name.clone())
            .collect()
    }
}

impl CommsSystem<SimulationModuleCommsBuilder> for SimulationCommsSystem {
    fn new_builder(&self, module_name: &str) -> SimulationModuleCommsBuilder {
        let mod_id = self
            .inner
            .lock()
            .unwrap()
            .create_module(module_name)
            .unwrap();
        SimulationModuleCommsBuilder {
            module_id: mod_id,
            system: self.inner.clone(),
            topic_readers: Vec::new(),
        }
    }

    fn num_modules(&self) -> usize {
        self.inner.lock().unwrap().modules.len()
    }

    fn num_topics(&self) -> usize {
        self.inner.lock().unwrap().topics.len()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct _InnerModuleInfo {
    name: String,
    read_topics: Vec<TopicId>,
    write_topics: Vec<TopicId>,
}
#[derive(Debug, Clone)]
pub(crate) struct _InnerTopicInfo {
    name: String,
    write_modules: Vec<ModuleId>,
    read_modules: Vec<ModuleId>,

    publisher: SimulationTopicPublisher,
}
#[derive(Debug, Clone)]
pub struct SimulationCommsSystemInner {
    pub(crate) topics: Vec<_InnerTopicInfo>,
    pub(crate) modules: Vec<_InnerModuleInfo>,
    time_provider: SimulationTime,
    is_world_running: Rc<Cell<bool>>,
}

impl SimulationCommsSystemInner {
    fn get_or_create_topic(&mut self, topic_name: &str) -> TopicId {
        match self.topics.iter().position(|x| x.name == topic_name) {
            Some(index) => TopicId { slot: index },
            None => {
                let next_id = TopicId {
                    slot: self.topics.len(),
                };
                self.topics.push(_InnerTopicInfo {
                    name: topic_name.into(),
                    write_modules: Vec::new(),
                    read_modules: Vec::new(),
                    publisher: SimulationTopicPublisher {
                        destination: Vec::new(),
                        topic_updated_at: Rc::new(Cell::new(SystemTime::UNIX_EPOCH)),
                    },
                });
                next_id
            }
        }
    }

    fn create_module(&mut self, module_name: &str) -> Option<ModuleId> {
        match self.modules.iter().position(|x| x.name == module_name) {
            Some(_) => None,
            None => {
                let next_id = ModuleId {
                    slot: self.modules.len(),
                };
                self.modules.push(_InnerModuleInfo {
                    name: module_name.into(),
                    read_topics: Vec::new(),
                    write_topics: Vec::new(),
                });
                Some(next_id)
            }
        }
    }

    fn subscribe_topic(
        &mut self,
        module_id: &ModuleId,
        topic_id: &TopicId,
    ) -> crossbeam::channel::Receiver<Message> {
        let topic = &mut self.topics[topic_id.slot];
        topic.read_modules.push(module_id.clone());

        let module = &mut self.modules[module_id.slot];
        module.read_topics.push(topic_id.clone());

        let (tx, rx) = channel::unbounded();
        topic.publisher.destination.push(tx);

        rx
    }

    fn publish_topic(&mut self, module_id: &ModuleId, topic_id: &TopicId) -> usize {
        let topic = &mut self.topics[topic_id.slot];
        topic.write_modules.push(module_id.clone());

        let module = &mut self.modules[module_id.slot];
        module.write_topics.push(topic_id.clone());

        module.write_topics.len() - 1
    }
}
