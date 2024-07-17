pub mod account;
use std::time::SystemTime;

pub mod data;
pub mod module;
pub mod order;
pub mod time;

#[derive(Debug, Clone)]
pub enum Payload {
    BinanceTradeTick(data::market::BinanceTradeTick),
    OrderRequest(order::OrderRequest),
    CancelOrderRequest(order::CancelOrderRequest),
    OrderResult(order::OrderResult),
    AccountUpdate(account::AccountUpdate),
    BinanceBookTicker(data::market::BinanceBookTicker),
}

#[derive(Debug, Clone)]
pub struct MessageHeader {
    pub commit_at: SystemTime,
}

#[derive(Debug, Clone)]
pub struct Message {
    pub header: MessageHeader,
    pub payload: Payload,
}
