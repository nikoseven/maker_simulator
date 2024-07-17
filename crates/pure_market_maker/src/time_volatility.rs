use yata::{
    core::{Error, Method, PeriodType, ValueType},
    helpers::Peekable,
    methods::StDev,
};

use crate::duration_sampler::DurationSampler;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct TimeVolatility {
    stdev: StDev,
    duration_sampler: DurationSampler,
    last_value: f64,
}

impl Method for TimeVolatility {
    type Params = (PeriodType, u64);
    type Input = (u64, ValueType);
    type Output = ValueType;

    fn new(params: Self::Params, value: &Self::Input) -> Result<Self, Error> {
        let (t, value) = value;
        match params {
            (0, _) => Err(Error::WrongMethodParameters),
            (_, 0) => Err(Error::WrongMethodParameters),
            (num_sample, duration_ms) => Ok(Self {
                stdev: StDev::new(num_sample, &0.0)?,
                duration_sampler: DurationSampler::new(duration_ms, *t),
                last_value: *value,
            }),
        }
    }

    #[inline]
    fn next(&mut self, value: &Self::Input) -> Self::Output {
        let (t, value) = value;
        if self.duration_sampler.sampled(*t) {
            let diff = value - self.last_value;
            self.last_value = *value;
            self.stdev.next(&diff)
        } else {
            self.stdev.peek()
        }
    }
}

impl Peekable<<Self as Method>::Output> for TimeVolatility {
    fn peek(&self) -> <Self as Method>::Output {
        self.stdev.peek()
    }
}
