use std::time::SystemTime;

use crate::Message;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicId {
    pub slot: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModuleId {
    pub slot: usize,
}

#[derive(Debug, Clone)]
pub struct WriteTopicHandle {
    pub slot: usize,
}

#[derive(Debug, Clone)]
pub struct ReadTopicHandle {
    pub slot: usize,
}

// Each module has its own ModuleComms instance for communication with other modules.
pub trait ModuleComms {
    fn time(&self) -> SystemTime;
    fn receive(&mut self, topic: &ReadTopicHandle) -> Option<Message>;
    fn publish(&mut self, topic: &WriteTopicHandle, message: Message);
    fn request_terminate(&mut self);
}

pub trait ModuleCommsBuilder {
    fn get_module_id(&self) -> &ModuleId;
    fn get_topic(&mut self, name: &str) -> TopicId;
    fn subscribe_topic(&mut self, topic: &TopicId) -> ReadTopicHandle;
    fn publish_topic(&mut self, topic: &TopicId) -> WriteTopicHandle;

    fn build(self) -> Box<dyn ModuleComms>;
}

// CommsSystem maintains global communication channels between modules and topics
pub trait CommsSystem<ModuleBuilderT: ModuleCommsBuilder> {
    fn new_builder(&self, module_name: &str) -> ModuleBuilderT;
    fn num_modules(&self) -> usize;
    fn num_topics(&self) -> usize;
}

/*
       ┌───────────────┐
       │               │
       │    ┌──────────▼─────────┐
       │    │  wait until ready  │
       │    └──────────┬─────────┘
       │               │
       │    ┌──────────▼─────────┐
       │    │        sync        │
       │    └──────────┬─────────┘
       │               │
       │    ┌──────────▼─────────┐
       │    │    one iteration   │
       │    └──────────┬─────────┘
       │               │
       └───────────────┘
*/
pub trait Module {
    fn start(&mut self);
    fn sync(&mut self, comms: &mut dyn ModuleComms) -> bool;
    fn one_iteration(&mut self, comms: &mut dyn ModuleComms);
    fn next_iteration_start_at(&self) -> Option<SystemTime>;
    fn wake_on_message(&self) -> bool;
    fn terminate(&mut self) {}
}

pub trait ModuleBuilder {
    fn init_comm(&mut self, comms: &mut dyn ModuleCommsBuilder);
    fn build(self: Box<Self>) -> Box<dyn Module>;
    fn name(&self) -> &str;
}
