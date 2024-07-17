use std::sync::Arc;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone)]
pub enum TradeType {
    Limit,
    LimitMaker,
    Market,
}

#[derive(Debug, Clone)]
pub enum TimeInForce {
    GoodTilCancelled,
    ImmediateOrCancelled,
    FillOrKill,
}

#[derive(Debug, Clone)]
pub struct OrderRequest {
    pub symbol: &'static str,
    pub side: TradeSide,
    pub price: f64,
    pub quantity: f64,
    pub trade_type: TradeType,
    pub time_in_force: TimeInForce,
    pub client_order_id: Arc<str>,
    pub cancel_order_id: Option<Arc<str>>,
}

#[derive(Debug, Clone)]
pub struct CancelOrderRequest {
    pub symbol: &'static str,
    pub client_order_id: Arc<str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
    Expired,
    ExpiredInMatch,
}

#[derive(Debug, Clone)]
pub struct OrderResult {
    pub symbol: &'static str,
    pub at: std::time::SystemTime,
    pub client_order_id: Arc<str>,
    pub filled_quantity: f64,
    pub price: f64,
    pub is_buy: bool,
    pub status: OrderStatus,
}
