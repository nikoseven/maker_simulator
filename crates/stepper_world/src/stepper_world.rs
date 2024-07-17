use std::time::{SystemTime, UNIX_EPOCH};

use account::account::Account;
use upstair_type::data::market::BinanceTradeTick;

use crate::order_tracker::OrderTracker;

pub struct StepperWorld {
    pub now: SystemTime,
    pub latest_market_price: f64,
    pub order_tracker: OrderTracker,
    pub account: Account,
    pub best_bid_price: f64,
    pub best_bid_qty: f64,
    pub best_ask_price: f64,
    pub best_ask_qty: f64,
    pub booker_tick_updated_at: SystemTime,

    pub trade_buf: Vec<BinanceTradeTick>,
    pub wap_buf: Vec<(u64, f64)>,
    // (order_id, filled_amt)
    pub filled_event_buf: Vec<(String, f64)>,
}

impl Default for StepperWorld {
    fn default() -> Self {
        StepperWorld {
            now: SystemTime::now(),
            latest_market_price: 0.0,
            order_tracker: OrderTracker::default(),
            account: Account::default(),
            best_bid_price: 0.0,
            best_bid_qty: 0.0,
            best_ask_price: 0.0,
            best_ask_qty: 0.0,
            booker_tick_updated_at: UNIX_EPOCH,
            trade_buf: Vec::with_capacity(1024),
            wap_buf: Vec::with_capacity(1024),
            filled_event_buf: Vec::with_capacity(1024),
        }
    }
}
