pub struct OhlcvCandle {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

impl OhlcvCandle {
    pub fn from_ohlcv(open: f64, high: f64, low: f64, close: f64, volume: f64) -> Self {
        Self {
            open,
            high,
            low,
            close,
            volume,
        }
    }

    pub fn from_trade(price: f64, voi: f64) -> Self {
        Self {
            open: price,
            high: price,
            low: price,
            close: price,
            volume: voi,
        }
    }

    pub fn update_latest_trade(&mut self, price: f64, voi: f64) {
        self.close = price;
        self.volume += voi;

        if price > self.high {
            self.high = price;
        }

        if price < self.low {
            self.low = price;
        }
    }
}

// unit-test
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ohlc_candle() {
        let mut candle = OhlcvCandle::from_ohlcv(1.0, 2.0, 0.5, 1.5, 100.0);
        assert_eq!(candle.open, 1.0);
        assert_eq!(candle.high, 2.0);
        assert_eq!(candle.low, 0.5);
        assert_eq!(candle.close, 1.5);
        assert_eq!(candle.volume, 100.0);

        candle.update_latest_trade(2.0, 50.0);
        assert_eq!(candle.open, 1.0);
        assert_eq!(candle.high, 2.0);
        assert_eq!(candle.low, 0.5);
        assert_eq!(candle.close, 2.0);
        assert_eq!(candle.volume, 150.0);

        candle.update_latest_trade(0.5, 50.0);
        assert_eq!(candle.open, 1.0);
        assert_eq!(candle.high, 2.0);
        assert_eq!(candle.low, 0.5);
        assert_eq!(candle.close, 0.5);
        assert_eq!(candle.volume, 200.0);

        // a trade of higher price
        candle.update_latest_trade(3.0, 50.0);
        assert_eq!(candle.open, 1.0);
        assert_eq!(candle.high, 3.0);
        assert_eq!(candle.low, 0.5);
        assert_eq!(candle.close, 3.0);
        assert_eq!(candle.volume, 250.0);

        // a trdae of lower price
        candle.update_latest_trade(0.1, 50.0);
        assert_eq!(candle.open, 1.0);
        assert_eq!(candle.high, 3.0);
        assert_eq!(candle.low, 0.1);
        assert_eq!(candle.close, 0.1);
        assert_eq!(candle.volume, 300.0);
    }
}
