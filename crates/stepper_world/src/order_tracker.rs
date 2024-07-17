use std::{
    collections::{HashMap, HashSet},
    time::SystemTime,
};
use upstair_type::order::TradeSide;

#[derive(Debug, Eq, PartialEq, Hash)]
pub enum OrderStatus {
    OpenRequested,
    Open,
    PartiallyFilled,
    Filled,
    CancelRequested,
    Canceled,
}

#[derive(Debug)]
pub struct Order {
    pub order_id: String,
    pub price: f64,
    pub side: TradeSide,
    pub quantity: f64,
    pub filled: f64,
    pub status: OrderStatus,
    pub created_at: SystemTime,
}

#[derive(Debug, Default)]
pub struct OrderTracker {
    orders: HashMap<String, Order>,
    proceed_unique_fill_report_id: HashSet<String>,
}

impl OrderTracker {
    // find order by order_id
    pub fn get_order(&self, order_id: &str) -> Option<&Order> {
        self.orders.get(order_id)
    }

    // upsert order
    // return if new order is inserted
    pub fn upsert_order(&mut self, order: Order) -> bool {
        let order_id = order.order_id.clone();
        let is_new_order = !self.orders.contains_key(&order_id);
        self.orders.insert(order_id, order);
        is_new_order
    }

    // fiil order
    pub fn fill_order(&mut self, order_id: &str, filled: f64, unique_fill_report_id: Option<&str>) {
        // skip if the fill report is already proceed
        if let Some(unique_fill_report_id) = unique_fill_report_id {
            if self
                .proceed_unique_fill_report_id
                .contains(unique_fill_report_id)
            {
                return;
            }
            self.proceed_unique_fill_report_id
                .insert(unique_fill_report_id.to_string());
        }

        if let Some(order) = self.orders.get_mut(order_id) {
            order.filled += filled;
        }
    }

    pub fn update_fill_quantity(&mut self, order_id: &str, filled: f64) {
        if let Some(order) = self.orders.get_mut(order_id) {
            order.filled = filled;
        }
    }

    pub fn update_status(&mut self, order_id: &str, status: OrderStatus) {
        if let Some(order) = self.orders.get_mut(order_id) {
            order.status = status;
        }
    }

    pub fn remove_terminated_orders(&mut self) {
        self.orders.retain(|_, order| {
            order.status != OrderStatus::Canceled && order.status != OrderStatus::Filled
        });
    }

    pub fn iter(&self) -> impl Iterator<Item = &Order> {
        self.orders.values()
    }

    pub fn cancel_order(&mut self, order_id: &str) {
        // remove the order
        self.orders.remove(order_id);
    }

    pub fn request_cancel_order(&mut self, order_id: &str) {
        if let Some(order) = self.orders.get_mut(order_id) {
            order.status = OrderStatus::CancelRequested;
        }
    }

    pub fn size(&self) -> usize {
        self.orders.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // test upsert_order new order
    #[test]
    fn upsert_new_order() {
        let mut order_tracker = OrderTracker::default();
        let order = Order {
            order_id: "test".into(),
            price: 0.0,
            side: TradeSide::Buy,
            quantity: 0.0,
            filled: 0.0,
            status: OrderStatus::Open,
            created_at: SystemTime::UNIX_EPOCH,
        };
        assert!(order_tracker.upsert_order(order));
    }

    // test upsert_order exisiting order
    #[test]
    fn upsert_existing_order() {
        let mut order_tracker = OrderTracker::default();
        let order = Order {
            order_id: "test".into(),
            price: 0.0,
            side: TradeSide::Buy,
            quantity: 2.0,
            filled: 0.0,
            status: OrderStatus::Open,
            created_at: SystemTime::UNIX_EPOCH,
        };
        order_tracker.upsert_order(order);
        let order = Order {
            order_id: "test".into(),
            price: 0.0,
            side: TradeSide::Sell,
            quantity: 1.0,
            filled: 0.0,
            status: OrderStatus::Open,
            created_at: SystemTime::UNIX_EPOCH,
        };
        assert!(!order_tracker.upsert_order(order));
        // the new upserted order should be the new one
        assert_eq!(
            order_tracker.orders.get("test").unwrap().side,
            TradeSide::Sell
        );
        assert_eq!(order_tracker.orders.get("test").unwrap().quantity, 1.0);
    }

    // test fill_order
    #[test]
    fn fill_order() {
        let mut order_tracker = OrderTracker::default();
        let order = Order {
            order_id: "test".into(),
            price: 0.0,
            side: TradeSide::Buy,
            quantity: 1.5,
            filled: 0.0,
            status: OrderStatus::Open,
            created_at: SystemTime::UNIX_EPOCH,
        };
        order_tracker.upsert_order(order);
        order_tracker.fill_order("test", 0.5, Some("report1"));
        order_tracker.fill_order("test", 0.5, Some("report1"));
        order_tracker.fill_order("test", 1.0, Some("report2"));
        assert_eq!(order_tracker.orders.get("test").unwrap().filled, 1.5);
    }

    #[test]
    fn test_cancel_order() {
        let mut order_tracker = OrderTracker::default();
        let order = Order {
            order_id: "test".into(),
            price: 0.0,
            side: TradeSide::Buy,
            quantity: 1.5,
            filled: 0.0,
            status: OrderStatus::Open,
            created_at: SystemTime::UNIX_EPOCH,
        };
        order_tracker.upsert_order(order);
        order_tracker.cancel_order("test");
        assert_eq!(order_tracker.orders.len(), 0);
    }
}
