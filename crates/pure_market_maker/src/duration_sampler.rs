#[derive(Debug, Clone)]
pub struct DurationSampler {
    duration_ms: u64,
    last_sample_slot: u64,
}

impl DurationSampler {
    pub fn new(duration_ms: u64, first_sampled_t: u64) -> Self {
        let first_slot = first_sampled_t / duration_ms;
        Self {
            duration_ms,
            last_sample_slot: first_slot,
        }
    }
    pub fn sampled(&mut self, t: u64) -> bool {
        let t_slot = t / self.duration_ms;
        if t_slot > self.last_sample_slot {
            self.last_sample_slot = t_slot;
            true
        } else {
            false
        }
    }
}
