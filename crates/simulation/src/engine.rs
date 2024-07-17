use std::cmp::Reverse;
use std::collections::hash_map::DefaultHasher;
use std::collections::BinaryHeap;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::time::SystemTime;
use std::vec;

use crate::simulation::{SimulationCommsSystem, SimulationModuleCommsBuilder};
use upstair_type::module::{ModuleBuilder, ModuleComms, ModuleCommsBuilder, TopicId};
use upstair_type::time::TimeProvider;
use upstair_type::Message;
use upstair_type::{
    module::{CommsSystem, Module, ModuleId},
    time::SimulationTime,
};

use tracing::debug;

#[derive(Eq, PartialEq, Hash, Debug)]
pub enum EngineEvent {
    Run(ModuleId),
}

#[derive(Eq, PartialEq)]
struct TimedEvent {
    time: SystemTime,
    event: EngineEvent,
}

impl PartialOrd for TimedEvent {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimedEvent {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.time.cmp(&other.time) {
            std::cmp::Ordering::Equal => {
                // compare hash of event
                let mut h1 = DefaultHasher::new();
                let mut h2 = DefaultHasher::new();
                self.event.hash(&mut h1);
                other.event.hash(&mut h2);
                h1.finish().cmp(&h2.finish())
            }
            other => other,
        }
    }
}

struct SimulationModuleContext {
    pub(crate) id: ModuleId,
    pub(crate) module: Box<dyn Module>,
    pub(crate) comms: Box<dyn ModuleComms>,
    pub(crate) name: String,
}

impl Debug for SimulationModuleContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimulationModuleContext")
            .field("id", &self.id)
            .finish()
    }
}

// Engine managee the system time and schedule the modules to run
pub struct SimulationEngine {
    comms_system: SimulationCommsSystem,
    simulation_time: SimulationTime,
    module_contexts: Vec<SimulationModuleContext>,
    topic_readers: Vec<crossbeam::channel::Receiver<Message>>,
}

impl SimulationEngine {
    pub fn run(&mut self) {
        let mut q = BinaryHeap::new();
        // get module writing topics
        let mut module_last_sync_time = vec![SystemTime::UNIX_EPOCH; self.module_contexts.len()];
        let topic_last_update_time = self.comms_system.get_all_topic_update_time();
        let module_subscribed_topics = self.comms_system.get_module_subscribed_topics();
        let topic_name = self.comms_system.get_topic_name();
        let module_name = self
            .module_contexts
            .iter()
            .map(|ctx| ctx.name.to_string())
            .collect::<Vec<_>>();
        assert_eq!(module_last_sync_time.len(), self.module_contexts.len());
        assert_eq!(module_subscribed_topics.len(), self.module_contexts.len());
        assert_eq!(topic_last_update_time.len(), self.topic_readers.len());

        // print module subscribed topics
        for (module_slot, topics) in module_subscribed_topics.iter().enumerate() {
            let mut s: String = format!(
                "module({}) subscribed: ",
                self.module_contexts[module_slot].name
            );
            for (i, topic_id) in topics.iter().enumerate() {
                if i > 0 {
                    s.push(',');
                }
                s.push_str(topic_name[topic_id.slot].as_str());
            }
            debug!("{}", s);
        }

        // call start for each modules
        for ctx in &mut self.module_contexts {
            debug!("start module({})", ctx.name);
            ctx.module.start();
        }
        // run modules with next iteration start time
        for (module_slot, ctx) in self.module_contexts.iter().enumerate() {
            let module_id = ModuleId { slot: module_slot };
            if let Some(t) = ctx.module.next_iteration_start_at() {
                let event = EngineEvent::Run(module_id);
                let e = TimedEvent { time: t, event };
                q.push(Reverse(e));
            }
        }
        // start simulation
        while let Some(Reverse(TimedEvent { time, event })) = q.pop() {
            if !self.comms_system.is_world_running.get() {
                break;
            }
            self.simulation_time.set_time(time);
            match event {
                EngineEvent::Run(module_id) => {
                    let ctx = &mut self.module_contexts[module_id.slot];
                    debug!(
                        "run module({}) at {}",
                        ctx.name,
                        time.elapsed().unwrap().as_millis()
                    );
                    if ctx.module.sync(ctx.comms.as_mut()) {
                        ctx.module.one_iteration(ctx.comms.as_mut());
                    }
                    // check next wakeup time
                    if let Some(next_iter_t) = ctx.module.next_iteration_start_at() {
                        let event = EngineEvent::Run(module_id);
                        q.push(Reverse(TimedEvent {
                            time: next_iter_t,
                            event,
                        }));

                        debug!(
                            "module {:?} finished. next_iter in {} ms",
                            ctx.name,
                            next_iter_t.duration_since(time).unwrap().as_millis()
                        );
                    } else {
                        debug!("module {:?} finished", ctx.name)
                    }
                    // print topic update time
                    for (i, t) in topic_last_update_time.iter().enumerate() {
                        if t.get()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap()
                            .as_millis()
                            == 0
                        {
                            continue;
                        }
                        debug!(
                            "topic({}) updated at {} ms ago",
                            topic_name[i],
                            time.duration_since(t.get()).unwrap().as_millis()
                        );
                    }

                    // wakeup module if topic is newer than last sync time
                    for module_slot in 0..module_subscribed_topics.len() {
                        let has_update_since_last_sync = module_subscribed_topics[module_slot]
                            .iter()
                            .any(|topic_id| {
                                let topic_slot = topic_id.slot;
                                let topic_updated_at = &topic_last_update_time[topic_slot];
                                let module_last_sync_time = &module_last_sync_time[module_slot];
                                topic_updated_at.get() > *module_last_sync_time
                            });
                        debug!(
                            "module {} has update: {} wake_on_message: {}",
                            module_name[module_slot],
                            has_update_since_last_sync,
                            self.module_contexts[module_slot].module.wake_on_message()
                        );
                        if has_update_since_last_sync
                            && self.module_contexts[module_slot].module.wake_on_message()
                        {
                            let event = EngineEvent::Run(ModuleId { slot: module_slot });
                            let t = self.comms_system.time_provider.time();
                            q.push(Reverse(TimedEvent { time: t, event }));
                            module_last_sync_time[module_slot] = t;
                        }
                    }
                }
            }
        }
        // terminate modules
        for ctx in &mut self.module_contexts {
            ctx.module.terminate();
        }
    }
}

struct SimulationModuleBuilderContext {
    id: ModuleId,
    builder: Box<dyn ModuleBuilder>,
    comms_builder: SimulationModuleCommsBuilder,
}

#[derive(Default)]
pub struct SimulationEngineBuilder {
    comms_sys: SimulationCommsSystem,
    module_builder_contexts: Vec<SimulationModuleBuilderContext>,
}

impl SimulationEngineBuilder {
    pub fn add_module(mut self, module: impl ModuleBuilder + 'static) -> Self {
        self.add_module_dyn(Box::new(module));
        self
    }

    pub fn add_module_dyn(&mut self, mut module_builder: Box<dyn ModuleBuilder>) {
        let name = module_builder.name();

        let mut module_comm_builder = self.comms_sys.new_builder(name);
        module_builder.init_comm(&mut module_comm_builder);

        let module_id = module_comm_builder.get_module_id().clone();
        if module_id.slot != self.module_builder_contexts.len() {
            panic!("module id must be continuous");
        }

        self.module_builder_contexts
            .push(SimulationModuleBuilderContext {
                id: module_id,
                builder: module_builder,
                comms_builder: module_comm_builder,
            });
    }

    pub fn build(mut self) -> SimulationEngine {
        let mut ctxs = vec![];
        // listen to all topics
        let mut topic_readers = vec![];
        for i in 0..self.comms_sys.num_topics() {
            let topic_id = TopicId { slot: i };
            topic_readers.push(self.comms_sys.get_topic_reader(&topic_id));
        }

        // build all modules
        for SimulationModuleBuilderContext {
            id,
            builder,
            comms_builder,
        } in self.module_builder_contexts
        {
            let name: String = builder.name().into();
            let module = builder.build();
            let comms = comms_builder.build();
            ctxs.push(SimulationModuleContext {
                id,
                module,
                comms,
                name,
            });
        }

        let simulation_time = self.comms_sys.time_provider.clone();
        SimulationEngine {
            comms_system: self.comms_sys,
            simulation_time,
            module_contexts: ctxs,
            topic_readers,
        }
    }
}
