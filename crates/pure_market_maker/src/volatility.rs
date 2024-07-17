use yata::{
    core::{Error, Method, PeriodType, ValueType, Window},
    helpers::Peekable,
};

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct InstantVolatility {
    window: Window<ValueType>,
    prev_value: ValueType,
    sum_sq: ValueType,
}

impl Method for InstantVolatility {
    type Params = PeriodType;
    type Input = ValueType;
    type Output = Self::Input;

    fn new(length: Self::Params, &value: &Self::Input) -> Result<Self, Error> {
        match length {
            0 => Err(Error::WrongMethodParameters),
            length => Ok(Self {
                window: Window::new(length, 0.),
                prev_value: value,
                sum_sq: 0.,
            }),
        }
    }

    #[inline]
    fn next(&mut self, &value: &Self::Input) -> Self::Output {
        let derivative = value - self.prev_value;
        let derivative_sq = derivative * derivative;

        self.prev_value = value;
        let past_derivative_sq = self.window.push(derivative_sq);

        self.sum_sq += derivative_sq - past_derivative_sq;
        self.sum_sq
    }
}

impl Peekable<<Self as Method>::Output> for InstantVolatility {
    fn peek(&self) -> <Self as Method>::Output {
        (self.sum_sq / self.window.len() as f64).sqrt()
    }
}
