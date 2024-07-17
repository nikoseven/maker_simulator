use std::collections::HashMap;

#[derive(Default, Debug)]
pub(crate) struct MarketStats {
    total_order_num: u64,
    total_order_buy_quantity: f64,
    total_order_sell_quantity: f64,
    total_order_cancel_num: u64,
    total_filled_buy_quantity: f64,
    total_filled_sell_quantity: f64,
    total_filled_buy_vol: f64,
    total_filled_sell_vol: f64,

    event_count: HashMap<String, u64>,
}

impl MarketStats {
    pub(crate) fn on_order_cancel(&mut self) {
        self.total_order_cancel_num += 1;
    }

    pub(crate) fn on_order_submiited(&mut self, quantity: f64, is_buy: bool) {
        self.total_order_num += 1;
        if is_buy {
            self.total_order_buy_quantity += quantity;
        } else {
            self.total_order_sell_quantity += quantity;
        }
    }

    pub(crate) fn on_order_filled(&mut self, quantity: f64, vol: f64, is_buy: bool) {
        if is_buy {
            self.total_filled_buy_quantity += quantity;
            self.total_filled_buy_vol += vol;
        } else {
            self.total_filled_sell_quantity += quantity;
            self.total_filled_sell_vol += vol;
        }
    }

    pub(crate) fn on_event(&mut self, event: &str) {
        let count = self.event_count.entry(event.to_string()).or_insert(0);
        *count += 1;
    }

    pub(crate) fn summary(&self) -> String {
        let mut event_summary = String::new();
        for (event, count) in &self.event_count {
            event_summary.push_str(&format!("{}: {}\n", event, count));
        }

        format!(
            "Order Num: {}\n\
            Order Cancel Num: {}\n\
            Order Buy Quantity: {:.5}\n\
            Order Sell Quantity: {:.5}\n\
            Filled Buy Quantity/Vol: {:.5}/{:.2}\n\
            Filled Sell Quantity/Vol: {:.5}/{:.2}\n\
            {}",
            self.total_order_num,
            self.total_order_cancel_num,
            self.total_order_buy_quantity,
            self.total_order_sell_quantity,
            self.total_filled_buy_quantity,
            self.total_filled_buy_vol,
            self.total_filled_sell_quantity,
            self.total_filled_sell_vol,
            event_summary
        )
    }

    pub(crate) fn total_filled_sell_vol(&self) -> f64 {
        self.total_filled_sell_vol
    }

    pub(crate) fn total_filled_buy_vol(&self) -> f64 {
        self.total_filled_buy_vol
    }
}
