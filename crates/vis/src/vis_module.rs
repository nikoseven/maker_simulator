use std::{
    ops::Add,
    sync::mpsc::{self, Sender},
    thread::{self, JoinHandle},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use account::account::{Account, AssetBalance};
use eframe::{egui, EventLoopBuilderHook};
use symbol_info::SymbolInfoManager;
use upstair_type::module::{Module, ModuleBuilder, ReadTopicHandle};

use crate::vis_data::{self, DataState, TimeInMs, TradeBrief};
use crate::{vis_app::VisApp, vis_data::DataBuffer};

use tracing::{error, info};

use winit::platform::windows::EventLoopBuilderExtWindows;

pub struct VisModule {
    read_market_data: ReadTopicHandle,
    order_topic: ReadTopicHandle,
    order_result_topic: ReadTopicHandle,
    account_topic: ReadTopicHandle,

    wait_for_first_message: bool,
    next_iteration_time: SystemTime,

    #[allow(dead_code)]
    symbol_info_manager: SymbolInfoManager,

    buffer: vis_data::DataBuffer,

    vis_app_join_handle: Option<JoinHandle<()>>,

    app_tx: Option<Sender<DataBuffer>>,

    initial_account: Account,
}

impl Module for VisModule {
    fn start(&mut self) {
        let (tx, rx) = mpsc::channel::<DataBuffer>();
        let vis_app_join_handle = thread::spawn(move || {
            info!("Vis App Started");
            let event_loop_builder: Option<EventLoopBuilderHook> =
                Some(Box::new(|event_loop_builder| {
                    event_loop_builder.with_any_thread(true);
                }));
            let options = eframe::NativeOptions {
                event_loop_builder,
                viewport: egui::ViewportBuilder::default().with_inner_size([1200.0, 800.0]),
                default_theme: eframe::Theme::Dark,
                follow_system_theme: false,
                centered: true,
                ..Default::default()
            };

            let result = eframe::run_native(
                "Stepper Vis",
                options,
                Box::new(|cc| {
                    cc.egui_ctx.set_pixels_per_point(1.);
                    let app = VisApp::default().with_update_data_fn(Box::new(
                        move |state: &mut DataState| {
                            let mut updated = false;
                            while let Ok(buffer) = rx.try_recv() {
                                state.update(buffer);
                                updated = true;
                            }
                            updated
                        },
                    ));
                    Box::new(app)
                }),
            );
            if result.is_err() {
                error!("Error in running vis app: {:?}", result);
            }
            info!("Vis App Terminated");
        });
        self.vis_app_join_handle = Some(vis_app_join_handle);
        self.app_tx = tx.into();
    }

    fn terminate(&mut self) {
        self.vis_app_join_handle.take().map(|h| h.join());
    }

    fn sync(&mut self, comms: &mut dyn upstair_type::module::ModuleComms) -> bool {
        while let Some(msg) = comms.receive(&self.read_market_data) {
            self.ingest_message(msg);
        }
        while let Some(msg) = comms.receive(&self.order_topic) {
            self.ingest_message(msg);
        }
        while let Some(msg) = comms.receive(&self.order_result_topic) {
            self.ingest_message(msg);
        }
        while let Some(msg) = comms.receive(&self.account_topic) {
            self.ingest_message(msg);
        }
        if self.wait_for_first_message {
            self.wait_for_first_message = false;
            self.next_iteration_time = comms.time().add(Duration::from_millis(60 * 1000));
            return false;
        }
        true
    }

    fn one_iteration(&mut self, comms: &mut dyn upstair_type::module::ModuleComms) {
        if let Some(tx) = self.app_tx.as_ref() {
            self.buffer.commit_at =
                comms.time().duration_since(UNIX_EPOCH).unwrap().as_millis() as TimeInMs;
            let _ = tx.send(self.buffer.take());
        }
        self.next_iteration_time = comms.time().add(Duration::from_millis(1000));
    }

    fn next_iteration_start_at(&self) -> Option<std::time::SystemTime> {
        if self.wait_for_first_message {
            None
        } else {
            Some(self.next_iteration_time)
        }
    }

    fn wake_on_message(&self) -> bool {
        self.wait_for_first_message
    }
}

impl VisModule {
    fn ingest_message(&mut self, data: upstair_type::Message) {
        match data.payload {
            upstair_type::Payload::BinanceTradeTick(tick) => {
                *self
                    .buffer
                    .latest_market_price
                    .entry(
                        self.symbol_info_manager
                            .get(tick.symbol)
                            .unwrap()
                            .base_asset,
                    )
                    .or_default() = tick.price;
                self.buffer.last_price = tick.price;
                self.buffer.market_trades.push(tick);
            }
            upstair_type::Payload::OrderRequest(_) => self.buffer.order_count += 1,
            upstair_type::Payload::OrderResult(order_result) => {
                if order_result.status == upstair_type::order::OrderStatus::Filled
                    || order_result.status == upstair_type::order::OrderStatus::PartiallyFilled
                {
                    self.buffer.account_trades.push(TradeBrief {
                        time: order_result
                            .at
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as TimeInMs,
                        is_buy: order_result.is_buy,
                        price: order_result.price,
                        qty: order_result.filled_quantity,
                    })
                }
                self.buffer.order_updates.push(order_result);
            }
            upstair_type::Payload::CancelOrderRequest(_) => {
                self.buffer.order_cancel_count += 1;
            }
            upstair_type::Payload::AccountUpdate(account) => {
                for (asset, update) in account.updates.iter() {
                    let b = self
                        .buffer
                        .account
                        .asset_to_balance
                        .entry(asset)
                        .or_default();
                    b.balance = update.balance;
                    b.locked = update.locked;

                    let profit_balance = self
                        .buffer
                        .profit_account
                        .asset_to_balance
                        .entry(&asset)
                        .or_default();
                    let inital_balance = self
                        .initial_account
                        .asset_to_balance
                        .get(asset)
                        .map(|b| b.balance)
                        .unwrap_or(0.);
                    profit_balance.balance = b.balance - inital_balance;
                }
            }
            upstair_type::Payload::BinanceBookTicker(_) => {}
        }
    }
}

#[derive(Default)]
pub struct VisModuleBuilder {
    market_data_topic: Option<ReadTopicHandle>,
    order_topic: Option<ReadTopicHandle>,
    order_result_topic: Option<ReadTopicHandle>,
    symbol_info_manager: Option<SymbolInfoManager>,
    account_topic: Option<ReadTopicHandle>,
    initial_account: Account,
}

impl VisModuleBuilder {
    pub fn with_symbol_info_manager(mut self, manager: SymbolInfoManager) -> Self {
        self.symbol_info_manager = Some(manager);
        self
    }

    pub fn with_initial_balance(mut self, asset: &'static str, balance: f64) -> Self {
        self.initial_account.asset_to_balance.insert(
            asset,
            AssetBalance {
                balance,
                locked: 0.,
            },
        );
        self
    }
}

impl ModuleBuilder for VisModuleBuilder {
    fn name(&self) -> &str {
        "vis"
    }

    fn init_comm(&mut self, comms: &mut dyn upstair_type::module::ModuleCommsBuilder) {
        let market_data_topic = comms.get_topic("market_data");
        let order_topic = comms.get_topic("order");
        let order_result_topic = comms.get_topic("order_result");
        let account_topic = comms.get_topic("account");

        self.market_data_topic = comms.subscribe_topic(&market_data_topic).into();
        self.order_topic = comms.subscribe_topic(&order_topic).into();
        self.order_result_topic = comms.subscribe_topic(&order_result_topic).into();
        self.account_topic = comms.subscribe_topic(&account_topic).into();
    }

    fn build(self: Box<VisModuleBuilder>) -> Box<dyn Module> {
        Box::new(VisModule {
            read_market_data: self.market_data_topic.unwrap(),
            order_topic: self.order_topic.unwrap(),
            order_result_topic: self.order_result_topic.unwrap(),
            wait_for_first_message: true,
            next_iteration_time: SystemTime::UNIX_EPOCH,
            symbol_info_manager: self.symbol_info_manager.unwrap(),
            buffer: DataBuffer::default(),
            vis_app_join_handle: None,
            app_tx: None,
            account_topic: self.account_topic.unwrap(),
            initial_account: self.initial_account,
        })
    }
}
