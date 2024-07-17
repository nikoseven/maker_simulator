use std::{
    sync::{atomic::AtomicU64, Arc},
    time::{SystemTime, UNIX_EPOCH},
};

pub trait TimeProvider {
    fn time(&self) -> SystemTime;
}

#[derive(Debug, Default)]
pub struct SystemTimeProvider {}

impl TimeProvider for SystemTimeProvider {
    fn time(&self) -> SystemTime {
        SystemTime::now()
    }
}

#[derive(Debug, Default, Clone)]
pub struct SimulationTime {
    nanos_since_epoch: Arc<AtomicU64>,
}

impl TimeProvider for SimulationTime {
    fn time(&self) -> SystemTime {
        let t = self
            .nanos_since_epoch
            .load(std::sync::atomic::Ordering::Acquire);
        UNIX_EPOCH + std::time::Duration::from_nanos(t)
    }
}

impl SimulationTime {
    pub fn set_time(&self, t: SystemTime) {
        let t = t.duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
        self.nanos_since_epoch
            .store(t, std::sync::atomic::Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    #[test]
    fn test_simulation_time() {
        use super::*;
        let t = SimulationTime::default();
        assert_eq!(t.time(), UNIX_EPOCH);
        t.set_time(UNIX_EPOCH + Duration::from_nanos(100));
        assert_eq!(t.time(), UNIX_EPOCH + Duration::from_nanos(100));
    }

    #[test]
    fn test_modify_mut_ref() {
        use super::*;
        let t = SimulationTime::default();
        let t1 = t.clone();
        let t2 = t.clone();

        t.set_time(UNIX_EPOCH + Duration::from_nanos(100));
        assert_eq!(t1.time(), UNIX_EPOCH + Duration::from_nanos(100));

        t1.set_time(UNIX_EPOCH + Duration::from_nanos(103));
        assert_eq!(t2.time(), UNIX_EPOCH + Duration::from_nanos(103));
    }

    #[test]
    fn test_sync_and_send() {
        use super::*;
        fn is_sync<T: Sync>() {}
        fn is_send<T: Send>() {}
        is_sync::<SimulationTime>();
        is_sync::<&SimulationTime>();
        is_send::<SimulationTime>();
        is_send::<&SimulationTime>();
    }
}
