use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use stepper_world::order_tracker::{self};
use symbol_info::SymbolInfoManager;
use upstair_type::module::{Module, ModuleBuilder, ReadTopicHandle, WriteTopicHandle};
use upstair_type::order::{CancelOrderRequest, TimeInForce};
use upstair_type::Payload::{self, BinanceTradeTick};
use upstair_type::{order, Message, MessageHeader};

use stepper_world;

pub struct Stepper {
    // Topics
    read_market_data_handle: ReadTopicHandle,
    read_order_result_handle: ReadTopicHandle,
    write_order_handle: WriteTopicHandle,
    read_account_handle: ReadTopicHandle,

    // Internal states
    world: stepper_world::StepperWorld,

    last_iteration_time: std::time::SystemTime,

    mm_strategy: pure_market_maker::AmmStrategy,

    #[allow(dead_code)]
    symbol_info: SymbolInfoManager,
}

impl Module for Stepper {
    fn sync(&mut self, comms: &mut dyn upstair_type::module::ModuleComms) -> bool {
        while let Some(msg) = comms.receive(&self.read_market_data_handle) {
            self.ingest_message(msg);
        }
        while let Some(msg) = comms.receive(&self.read_order_result_handle) {
            self.ingest_message(msg);
        }
        while let Some(msg) = comms.receive(&self.read_account_handle) {
            self.ingest_message(msg);
        }
        true
    }

    fn one_iteration(&mut self, comms: &mut dyn upstair_type::module::ModuleComms) {
        // at least 100ms from last iteration
        if comms
            .time()
            .duration_since(self.last_iteration_time)
            .unwrap()
            .as_millis()
            < 100
        {
            return;
        }
        self.last_iteration_time = comms.time();

        self.world.now = comms.time();
        self.world.order_tracker.remove_terminated_orders();

        self.mm_strategy.run(&mut self.world);
        self.world.trade_buf.clear();
        self.world.wap_buf.clear();
        self.world.filled_event_buf.clear();

        // run actions
        for action in self.mm_strategy.actions.iter() {
            match action {
                pure_market_maker::Action::CancelOrder(cancel_order) => {
                    self.world
                        .order_tracker
                        .request_cancel_order(&cancel_order.order_id);
                    comms.publish(
                        &self.write_order_handle,
                        Message {
                            header: MessageHeader {
                                commit_at: self.world.now,
                            },
                            payload: Payload::CancelOrderRequest(CancelOrderRequest {
                                symbol: cancel_order.symbol,
                                client_order_id: Arc::from(cancel_order.order_id.as_str()),
                            }),
                        },
                    )
                }
                pure_market_maker::Action::PlaceOrder(place_order) => {
                    let tracking_order = stepper_world::order_tracker::Order {
                        order_id: place_order.order_id.clone(),
                        price: place_order.price,
                        side: place_order.side.clone(),
                        quantity: place_order.quantity,
                        filled: 0.0,
                        status: stepper_world::order_tracker::OrderStatus::Open,
                        created_at: self.world.now,
                    };
                    self.world.order_tracker.upsert_order(tracking_order);
                    comms.publish(
                        &self.write_order_handle,
                        Message {
                            header: MessageHeader {
                                commit_at: self.world.now,
                            },
                            payload: Payload::OrderRequest(order::OrderRequest {
                                symbol: place_order.symbol,
                                side: place_order.side.clone(),
                                price: place_order.price,
                                quantity: place_order.quantity,
                                client_order_id: Arc::from(place_order.order_id.as_str()),
                                trade_type: order::TradeType::Limit,
                                time_in_force: TimeInForce::GoodTilCancelled,
                                cancel_order_id: None,
                            }),
                        },
                    );
                }
            }
        }
    }

    fn start(&mut self) {}

    fn next_iteration_start_at(&self) -> Option<std::time::SystemTime> {
        None
    }

    fn wake_on_message(&self) -> bool {
        true
    }

    fn terminate(&mut self) {
        self.mm_strategy.terminate();
    }
}

impl Stepper {
    fn ingest_message(&mut self, data: upstair_type::Message) {
        match data.payload {
            BinanceTradeTick(data) => {
                self.world.latest_market_price = data.price;
                self.world.trade_buf.push(data);
            }
            Payload::OrderRequest(_) => {}
            Payload::CancelOrderRequest(_) => {
                unimplemented!("cacnel rsp")
            }
            Payload::OrderResult(order_result) => {
                let order_tracking_status: order_tracker::OrderStatus = match order_result.status {
                    order::OrderStatus::New => order_tracker::OrderStatus::Open,
                    order::OrderStatus::PartiallyFilled => {
                        order_tracker::OrderStatus::PartiallyFilled
                    }
                    order::OrderStatus::Filled => order_tracker::OrderStatus::Filled,
                    order::OrderStatus::Canceled => order_tracker::OrderStatus::Canceled,
                    order::OrderStatus::Rejected => order_tracker::OrderStatus::Canceled,
                    order::OrderStatus::Expired => order_tracker::OrderStatus::Canceled,
                    order::OrderStatus::ExpiredInMatch => order_tracker::OrderStatus::Canceled,
                };
                self.world.order_tracker.update_fill_quantity(
                    &order_result.client_order_id,
                    order_result.filled_quantity,
                );
                self.world.filled_event_buf.push((
                    order_result.client_order_id.as_ref().into(),
                    order_result.filled_quantity,
                ));
                self.world
                    .order_tracker
                    .update_status(&order_result.client_order_id, order_tracking_status);
            }
            Payload::AccountUpdate(update) => {
                update.updates.iter().for_each(|(asset, updated_balance)| {
                    let entry = self
                        .world
                        .account
                        .asset_to_balance
                        .entry(asset)
                        .or_default();
                    entry.balance = updated_balance.balance;
                    entry.locked = updated_balance.locked;
                });
            }
            Payload::BinanceBookTicker(book_ticker) => {
                self.world.booker_tick_updated_at = self.world.now;
                self.world.best_ask_price = book_ticker.best_ask_price;
                self.world.best_ask_qty = book_ticker.best_ask_qty;
                self.world.best_bid_price = book_ticker.best_bid_price;
                self.world.best_bid_qty = book_ticker.best_bid_qty;

                let wap = (book_ticker.best_ask_price * book_ticker.best_bid_qty
                    + book_ticker.best_bid_price * book_ticker.best_ask_qty)
                    / (book_ticker.best_ask_qty + book_ticker.best_bid_qty);
                self.world.wap_buf.push((
                    data.header
                        .commit_at
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                    wap,
                ));
            }
        }
    }
}

pub struct StepperBuilder {
    market_data_topic: Option<ReadTopicHandle>,
    order_result_topic: Option<ReadTopicHandle>,
    order_topic: Option<WriteTopicHandle>,
    account_topic: Option<ReadTopicHandle>,
    symbol_info_manager: Option<SymbolInfoManager>,

    symbol: &'static str,
}

impl StepperBuilder {
    pub fn new(symbol: &'static str) -> StepperBuilder {
        StepperBuilder {
            market_data_topic: None,
            order_result_topic: None,
            order_topic: None,
            account_topic: None,
            symbol_info_manager: None,
            symbol,
        }
    }

    pub fn with_symbol_info_manager(mut self, symbol_info_manager: SymbolInfoManager) -> Self {
        self.symbol_info_manager = Some(symbol_info_manager);
        self
    }
}

impl ModuleBuilder for StepperBuilder {
    fn name(&self) -> &str {
        "stepper"
    }

    fn init_comm(&mut self, comms: &mut dyn upstair_type::module::ModuleCommsBuilder) {
        let market_data_topic = comms.get_topic("market_data");
        let order_result_topic = comms.get_topic("order_result");
        let order_topic = comms.get_topic("order");
        let account_topic = comms.get_topic("account");

        self.market_data_topic = comms.subscribe_topic(&market_data_topic).into();
        self.order_result_topic = comms.subscribe_topic(&order_result_topic).into();
        self.order_topic = comms.publish_topic(&order_topic).into();
        self.account_topic = comms.subscribe_topic(&account_topic).into();
    }

    fn build(self: Box<StepperBuilder>) -> Box<dyn Module> {
        Box::new(Stepper {
            read_market_data_handle: self.market_data_topic.unwrap(),
            read_order_result_handle: self.order_result_topic.unwrap(),
            write_order_handle: self.order_topic.unwrap(),
            read_account_handle: self.account_topic.unwrap(),
            world: stepper_world::StepperWorld::default(),
            last_iteration_time: SystemTime::UNIX_EPOCH,
            mm_strategy: pure_market_maker::AmmStrategy::new(
                self.symbol,
                self.symbol_info_manager.clone().unwrap(),
            ),
            symbol_info: self.symbol_info_manager.unwrap(),
        })
    }
}
