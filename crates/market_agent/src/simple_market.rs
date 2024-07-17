use std::sync::Arc;

use tracing::warn;
use upstair_type::order::TradeSide;

#[derive(Debug)]
pub(crate) struct LimitOrder {
    pub(crate) price: f64,
    pub(crate) quantity: f64,
    pub(crate) filled: f64,
    pub(crate) submit_at: std::time::SystemTime,
    pub(crate) side: TradeSide,
    pub(crate) order_id: Arc<str>,
}

#[derive(Debug)]
pub(crate) struct MarketTrade {
    pub(crate) price: f64,
    pub(crate) quantity: f64,
    pub(crate) trade_at: std::time::SystemTime,
    pub(crate) is_buyer_maker: bool,
}

pub(crate) struct SimpleMarket {
    pub(crate) open_orders: Vec<LimitOrder>,
    market_trade_buf: Vec<MarketTrade>,
    pub(crate) last_trade_price: f64,
}

#[derive(Debug)]
pub(crate) struct MarketEvent {
    pub(crate) side: TradeSide,
    pub(crate) price: f64,
    pub(crate) quantity: f64,
    pub(crate) reamin_qty_to_fill: f64,
    #[allow(dead_code)]
    pub(crate) event_at: std::time::SystemTime,
    pub(crate) order_id: Arc<str>,
}

impl SimpleMarket {
    pub(crate) fn new() -> Self {
        Self {
            open_orders: vec![],
            market_trade_buf: vec![],
            last_trade_price: 0.0,
        }
    }

    pub(crate) fn add_order(&mut self, order: LimitOrder) {
        if order.quantity <= 0.0 {
            warn!("order rejected due to quantity <= 0.0 : {:?}", order);
            return;
        }
        for o in self.open_orders.iter_mut() {
            if o.order_id == order.order_id {
                return;
            }
        }
        self.open_orders.push(order);
        self.open_orders.sort_by(|a, b| {
            if a.price == b.price {
                a.submit_at.cmp(&b.submit_at)
            } else {
                a.price.partial_cmp(&b.price).unwrap()
            }
        });
    }

    pub(crate) fn get_order(&self, order_id: &str) -> Option<&LimitOrder> {
        self.open_orders
            .iter()
            .find(|o| o.order_id.as_ref() == order_id)
    }

    pub(crate) fn cancel_order(&mut self, order_id: &str) {
        self.open_orders.retain(|o| o.order_id.as_ref() != order_id);
    }

    pub(crate) fn add_market_trade(&mut self, trade: MarketTrade) {
        self.last_trade_price = trade.price;
        self.market_trade_buf.push(trade);
    }

    pub(crate) fn try_match_market(&mut self) -> Vec<MarketEvent> {
        let mut events = vec![];
        for trade in self.market_trade_buf.drain(..) {
            let mut remain_quantity = trade.quantity;

            if trade.is_buyer_maker {
                // this is a active sell trade
                // from order with highest price to lowest price
                for order in self.open_orders.iter_mut().rev() {
                    if order.side == TradeSide::Buy && order.price >= trade.price {
                        let fill_quantity = (order.quantity - order.filled).min(remain_quantity);
                        order.filled += fill_quantity;
                        remain_quantity -= fill_quantity;
                        events.push(MarketEvent {
                            price: order.price,
                            quantity: fill_quantity,
                            event_at: trade.trade_at,
                            order_id: order.order_id.clone(),
                            side: order.side.clone(),
                            reamin_qty_to_fill: order.quantity - order.filled,
                        });
                        if remain_quantity <= 0.0 {
                            break;
                        }
                    }
                }
            } else {
                // this is active buy trade
                // from order with lowest price to highest price
                for order in self.open_orders.iter_mut() {
                    if order.side == TradeSide::Sell && order.price <= trade.price {
                        let fill_quantity = (order.quantity - order.filled).min(remain_quantity);
                        order.filled += fill_quantity;
                        remain_quantity -= fill_quantity;
                        events.push(MarketEvent {
                            price: order.price,
                            quantity: fill_quantity,
                            event_at: trade.trade_at,
                            order_id: order.order_id.clone(),
                            side: order.side.clone(),
                            reamin_qty_to_fill: order.quantity - order.filled,
                        });
                        if remain_quantity <= 0.0 {
                            break;
                        }
                    }
                }
            }
            // remove filled order
            self.open_orders.retain(|o| o.filled < o.quantity);
        }
        events
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use super::*;

    #[test]
    fn test_order_sorted_by_price_then_time() {
        let mut market = SimpleMarket::new();
        let order_id: Arc<str> = Arc::from("A");
        let order = LimitOrder {
            price: 100.0,
            quantity: 10.0,
            filled: 0.0,
            submit_at: std::time::SystemTime::now(),
            side: TradeSide::Buy,
            order_id: order_id.clone(),
        };
        market.add_order(order);
        let order_id: Arc<str> = Arc::from("B");
        let order = LimitOrder {
            price: 101.0,
            quantity: 10.0,
            filled: 0.0,
            submit_at: std::time::SystemTime::now(),
            side: TradeSide::Buy,
            order_id: order_id.clone(),
        };
        market.add_order(order);
        assert_eq!(market.open_orders.len(), 2);
        assert_eq!(market.open_orders[0].price, 100.0);
        assert_eq!(market.open_orders[1].price, 101.0);
    }

    #[test]
    fn test_dup_order_id() {
        let mut market = SimpleMarket::new();
        let order_id: Arc<str> = Arc::from("A");
        let order = LimitOrder {
            price: 100.0,
            quantity: 10.0,
            filled: 0.0,
            submit_at: std::time::SystemTime::now(),
            side: TradeSide::Buy,
            order_id: order_id.clone(),
        };
        market.add_order(order);
        let order = LimitOrder {
            price: 100.0,
            quantity: 10.0,
            filled: 0.0,
            submit_at: std::time::SystemTime::now(),
            side: TradeSide::Buy,
            order_id: order_id.clone(),
        };
        market.add_order(order);
        assert_eq!(market.open_orders.len(), 1);
    }

    #[test]
    fn test_remove_order() {
        let mut market = SimpleMarket::new();
        let order_id: Arc<str> = Arc::from("A");
        let order = LimitOrder {
            price: 100.0,
            quantity: 10.0,
            filled: 0.0,
            submit_at: std::time::SystemTime::now(),
            side: TradeSide::Buy,
            order_id: order_id.clone(),
        };
        market.add_order(order);
        market.cancel_order(&order_id);
        assert_eq!(market.open_orders.len(), 0);
    }

    #[test]
    fn test_add_market_trade() {
        let mut market = SimpleMarket::new();
        let trade = MarketTrade {
            price: 100.0,
            quantity: 10.0,
            trade_at: std::time::SystemTime::now(),
            is_buyer_maker: true,
        };
        market.add_market_trade(trade);
        assert_eq!(market.market_trade_buf.len(), 1);
    }

    #[test]
    fn test_try_match_market() {
        let mut market = SimpleMarket::new();
        let order_id: Arc<str> = Arc::from("A");
        let order = LimitOrder {
            price: 100.0,
            quantity: 10.0,
            filled: 0.0,
            submit_at: std::time::SystemTime::now(),
            side: TradeSide::Buy,
            order_id: order_id.clone(),
        };
        market.add_order(order);
        let trade = MarketTrade {
            price: 100.0,
            quantity: 5.0,
            trade_at: std::time::SystemTime::now(),
            is_buyer_maker: true,
        };
        market.add_market_trade(trade);
        let events = market.try_match_market();
        assert_eq!(events.len(), 1);
        assert_eq!(market.open_orders.len(), 1);
        assert_eq!(market.open_orders[0].filled, 5.0);
    }

    #[test]
    fn test_try_match_market_fill_more_than_one_order() {
        let mut market = SimpleMarket::new();
        let order_id: Arc<str> = Arc::from("A");
        let order = LimitOrder {
            price: 100.0,
            quantity: 10.0,
            filled: 0.0,
            submit_at: std::time::SystemTime::now(),
            side: TradeSide::Buy,
            order_id: order_id.clone(),
        };
        market.add_order(order);

        let order_id: Arc<str> = Arc::from("B");
        let order = LimitOrder {
            price: 101.0,
            quantity: 10.0,
            filled: 0.0,
            submit_at: std::time::SystemTime::now(),
            side: TradeSide::Buy,
            order_id: order_id.clone(),
        };
        market.add_order(order);

        let orde_id: Arc<str> = Arc::from("C");
        let order = LimitOrder {
            price: 105.0,
            quantity: 10.0,
            filled: 0.0,
            submit_at: std::time::SystemTime::now(),
            side: TradeSide::Sell,
            order_id: orde_id.clone(),
        };

        market.add_order(order);
        let trade = MarketTrade {
            price: 100.0,
            quantity: 15.0,
            trade_at: std::time::SystemTime::now(),
            is_buyer_maker: true,
        };
        market.add_market_trade(trade);
        let events = market.try_match_market();
        assert_eq!(events.len(), 2);
        assert_eq!(market.open_orders.len(), 2);
        // check events
        assert_eq!(events[0].price, 101.0);
        assert_eq!(events[0].quantity, 10.0);
        assert_eq!(events[1].price, 100.0);
        assert_eq!(events[1].quantity, 5.0);
    }

    #[test]
    fn test_push_zero_quantity_order() {
        let mut market = SimpleMarket::new();
        let order_id: Arc<str> = Arc::from("A");
        let order = LimitOrder {
            price: 100.0,
            quantity: 0.0,
            filled: 0.0,
            submit_at: std::time::SystemTime::now(),
            side: TradeSide::Buy,
            order_id: order_id.clone(),
        };
        market.add_order(order);
        assert_eq!(market.open_orders.len(), 0);
    }

    #[test]
    fn test_sort_order_by_price() {
        let mut market = SimpleMarket::new();
        let order_id: Arc<str> = Arc::from("A");
        let order = LimitOrder {
            price: 100.0,
            quantity: 10.0,
            filled: 0.0,
            submit_at: std::time::SystemTime::now(),
            side: TradeSide::Buy,
            order_id: order_id.clone(),
        };
        market.add_order(order);
        let order_id: Arc<str> = Arc::from("B");
        let order = LimitOrder {
            price: 100.0,
            quantity: 10.0,
            filled: 0.0,
            submit_at: std::time::SystemTime::now(),
            side: TradeSide::Buy,
            order_id: order_id.clone(),
        };
        market.add_order(order);
        let order_id: Arc<str> = Arc::from("C");
        let order = LimitOrder {
            price: 99.0,
            quantity: 10.0,
            filled: 0.0,
            submit_at: std::time::SystemTime::now(),
            side: TradeSide::Buy,
            order_id: order_id.clone(),
        };
        market.add_order(order);
        assert_eq!(market.open_orders.len(), 3);
        assert_eq!(market.open_orders[0].price, 99.0);
        assert_eq!(market.open_orders[1].price, 100.0);
        assert_eq!(market.open_orders[2].price, 100.0);
        assert_eq!(market.open_orders[2].order_id.deref(), "B");
    }
}
