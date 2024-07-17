use std::{
    collections::HashMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{market_stats::MarketStats, simple_market};
use account::account::{Account, AssetBalance};
use symbol_info::{calc_trade_result, SymbolInfoManager};
use tracing::{debug, error, trace};
use upstair_type::module::{Module, ModuleBuilder, ReadTopicHandle, WriteTopicHandle};

struct MarketAgent {
    market_data_topic: ReadTopicHandle,
    order_topic: ReadTopicHandle,
    order_result_topic: WriteTopicHandle,
    account_topic: WriteTopicHandle,

    market_by_symbol: std::collections::HashMap<&'static str, simple_market::SimpleMarket>,

    account: Account,
    fee_account: Account,
    symobl_info_manager: SymbolInfoManager,

    stats: MarketStats,

    initial_balance: Vec<(String, f64)>,

    last_account_summary_send_time: SystemTime,
}

impl Module for MarketAgent {
    fn start(&mut self) {
        // add initial balance
        for (asset, balance) in &self.initial_balance {
            let account = self.account.get_or_create(asset.clone().leak());
            account.add_balance(*balance);
        }
    }

    fn sync(&mut self, comms: &mut dyn upstair_type::module::ModuleComms) -> bool {
        while let Some(msg) = comms.receive(&self.market_data_topic) {
            self.ingest_market_trade_data(msg);
        }
        while let Some(msg) = comms.receive(&self.order_topic) {
            self.ingest_order_request(msg, comms);
        }
        true
    }

    fn one_iteration(&mut self, comms: &mut dyn upstair_type::module::ModuleComms) {
        for (symbol, market) in &mut self.market_by_symbol {
            for e in market.try_match_market().iter() {
                let is_buy = e.side == upstair_type::order::TradeSide::Buy;
                // update stats
                self.stats
                    .on_order_filled(e.quantity, e.quantity * e.price, is_buy);

                // deduce locked balance
                let symbol_info = self.symobl_info_manager.get(symbol).unwrap_or_else(|| {
                    panic!("symbol {} is not supported", symbol);
                });
                let r = calc_trade_result(symbol_info, e.price, e.quantity, is_buy);

                // deduct fees
                self.fee_account
                    .get_or_create(r.fee_asset)
                    .add_balance(r.fee_qty);
                self.account
                    .get_or_create(r.pay_asset)
                    .consume_locked(r.pay_qty);
                self.account
                    .get_or_create(r.recv_asset)
                    .add_balance(r.recv_qty);
                if e.quantity <= 0.0 {
                    panic!("quantity should be positive");
                }

                trace!(
                    "-----\nFill {:?} order_id={} price={} qty={}\n{}",
                    e.side,
                    e.order_id,
                    e.price,
                    e.quantity,
                    account_brief(&self.account)
                );

                let is_fully_filled = e.reamin_qty_to_fill <= 0.0;
                comms.publish(
                    &self.order_result_topic,
                    upstair_type::Message {
                        header: upstair_type::MessageHeader {
                            commit_at: comms.time(),
                        },
                        payload: upstair_type::Payload::OrderResult(
                            upstair_type::order::OrderResult {
                                symbol,
                                at: comms.time(),
                                client_order_id: e.order_id.clone(),
                                filled_quantity: e.quantity,
                                price: e.price,
                                is_buy,
                                status: if is_fully_filled {
                                    upstair_type::order::OrderStatus::Filled
                                } else {
                                    upstair_type::order::OrderStatus::PartiallyFilled
                                },
                            },
                        ),
                    },
                );
                // update touch asset
                comms.publish(
                    &self.account_topic,
                    upstair_type::Message {
                        header: upstair_type::MessageHeader {
                            commit_at: comms.time(),
                        },
                        payload: upstair_type::Payload::AccountUpdate(
                            Self::make_account_update_for_asset(
                                &self.account,
                                &[r.pay_asset, r.recv_asset],
                            ),
                        ),
                    },
                );
            }
        }

        // send account summary every 10 seconds
        let now = comms.time();
        if now
            .duration_since(self.last_account_summary_send_time)
            .unwrap_or_default()
            .as_secs()
            > 1000
        {
            self.last_account_summary_send_time = now;
            comms.publish(
                &self.account_topic,
                upstair_type::Message {
                    header: upstair_type::MessageHeader { commit_at: now },
                    payload: upstair_type::Payload::AccountUpdate(Self::make_account_update(
                        &self.account,
                    )),
                },
            );
        }
    }

    fn next_iteration_start_at(&self) -> Option<std::time::SystemTime> {
        None
    }

    fn wake_on_message(&self) -> bool {
        true
    }

    fn terminate(&mut self) {
        println!("--- Stats ---");
        println!("{}", self.stats.summary());

        // print all market price
        println!("--- Market Price ---");
        for (symbol, market) in &self.market_by_symbol {
            println!("{}: {}", symbol, market.last_trade_price);
        }

        // given account, compute total usdt value
        let calc_usdt_value_fn = |account: &Account| -> f64 {
            let mut total_usdt_value = 0.0;
            for (asset, balance) in &account.asset_to_balance {
                if asset == &"USDT" {
                    total_usdt_value += balance.balance;
                } else {
                    let symbol: String = format!("{}USDT", asset);
                    let market = self.market_by_symbol.get(&symbol.as_str());
                    if market.is_none() {
                        error!("symbol {} is not valued", symbol);
                        continue;
                    }
                    total_usdt_value += balance.balance * market.unwrap().last_trade_price;
                }
            }
            total_usdt_value
        };
        // print inital equity
        let mut total_inital_value = 0.0;
        println!("--- Initial Equity ---");
        for (asset, balance) in &self.initial_balance {
            let equity_price = if asset == "USDT" {
                1.0
            } else {
                let symbol: String = format!("{}USDT", asset);
                let market = self.market_by_symbol.get(&symbol.as_str());
                if market.is_none() {
                    error!("symbol {} is not valued", symbol);
                    continue;
                }
                market.unwrap().last_trade_price
            };
            total_inital_value += balance * equity_price;

            println!("{}: {} ({} usdt)", asset, balance, balance * equity_price);
        }
        println!("Total Usdt Value: {}", total_inital_value);

        // print all equity
        println!("--- Equity ---");
        for (asset, balance) in &self.account.asset_to_balance {
            println!("{}: {} ({} locked)", asset, balance.balance, balance.locked);
        }
        println!("Total Usdt Value: {}", calc_usdt_value_fn(&self.account));
        // print all fee balance
        println!("--- Fee ---");
        for (asset, balance) in &self.fee_account.asset_to_balance {
            println!("{}: {}", asset, balance.balance);
        }
        println!(
            "Total Usdt Value: {}",
            calc_usdt_value_fn(&self.fee_account)
        );
        // print all profilts
        println!("--- Profits ---");
        let mut total_profit_in_usdt = 0.0;
        for (asset, balance) in &self.account.asset_to_balance {
            let inital_balance = self
                .initial_balance
                .iter()
                .find(|(a, _)| a == asset)
                .map(|(_, b)| *b)
                .unwrap_or(0.0);
            let total_profit = balance.balance - inital_balance;
            println!("{}: {}", asset, total_profit);

            let equity_price = if asset == &"USDT" {
                1.0
            } else {
                let symbol: String = format!("{}USDT", asset);
                let market = self.market_by_symbol.get(&symbol.as_str());
                if market.is_none() {
                    error!("symbol {} is not valued", symbol);
                    continue;
                }
                market.unwrap().last_trade_price
            };
            total_profit_in_usdt += total_profit * equity_price;
        }
        println!("Total Usdt Value: {}", total_profit_in_usdt);
        println!(
            "Profit Rate: {:.2}%",
            total_profit_in_usdt / total_inital_value * 100.0
        );
        println!(
            "Profit/vol: {:.2} bps",
            total_profit_in_usdt
                / (self.stats.total_filled_buy_vol() + self.stats.total_filled_sell_vol())
                * 100.0
                * 100.0
        );
    }
}

fn account_brief(account: &Account) -> String {
    let usdt = account
        .asset_to_balance
        .get("USDT")
        .unwrap_or(&AssetBalance {
            balance: 0.0,
            locked: 0.0,
        });
    let btc = account
        .asset_to_balance
        .get("BTC")
        .unwrap_or(&AssetBalance {
            balance: 0.0,
            locked: 0.0,
        });
    format!(
        "usdt={}({} locked) btc={}({} locked)",
        usdt.balance, usdt.locked, btc.balance, btc.locked
    )
}

impl MarketAgent {
    fn ingest_market_trade_data(&mut self, data: upstair_type::Message) {
        match data.payload {
            upstair_type::Payload::BinanceTradeTick(tick) => {
                let market = self
                    .market_by_symbol
                    .entry(tick.symbol)
                    .or_insert_with(simple_market::SimpleMarket::new);
                market.add_market_trade(simple_market::MarketTrade {
                    price: tick.price,
                    quantity: tick.qty,
                    trade_at: SystemTime::UNIX_EPOCH + Duration::from_millis(tick.time),
                    is_buyer_maker: tick.is_buyer_maker,
                });
            }
            upstair_type::Payload::BinanceBookTicker(_) => {}
            _ => {
                error!("ingest_market_data: data is not expected");
            }
        }
    }

    fn ingest_order_request(
        &mut self,
        data: upstair_type::Message,
        comms: &mut dyn upstair_type::module::ModuleComms,
    ) {
        trace!("{:?}", data.payload);
        match data.payload {
            upstair_type::Payload::OrderRequest(req) => {
                if req.price <= 0.0 {
                    error!("price must be positive");
                    return;
                }
                let symbol = req.symbol;
                let side = req.side.clone();
                let client_order_id = req.client_order_id.clone();
                let price = req.price;
                match self.process_order_request(req, data.header) {
                    Ok(_) => {
                        comms.publish(
                            &self.order_result_topic,
                            upstair_type::Message {
                                header: upstair_type::MessageHeader {
                                    commit_at: comms.time(),
                                },
                                payload: upstair_type::Payload::OrderResult(
                                    upstair_type::order::OrderResult {
                                        symbol,
                                        at: comms.time(),
                                        client_order_id,
                                        filled_quantity: 0.0,
                                        price,
                                        is_buy: side == upstair_type::order::TradeSide::Buy,
                                        status: upstair_type::order::OrderStatus::New,
                                    },
                                ),
                            },
                        );
                    }
                    Err(_) => {
                        comms.publish(
                            &self.order_result_topic,
                            upstair_type::Message {
                                header: upstair_type::MessageHeader {
                                    commit_at: comms.time(),
                                },
                                payload: upstair_type::Payload::OrderResult(
                                    upstair_type::order::OrderResult {
                                        symbol,
                                        at: comms.time(),
                                        client_order_id,
                                        filled_quantity: 0.0,
                                        price,
                                        is_buy: side == upstair_type::order::TradeSide::Buy,
                                        status: upstair_type::order::OrderStatus::Rejected,
                                    },
                                ),
                            },
                        );
                        self.stats
                            .on_event(format!("order_fail_{:?}_{}", side, symbol).as_str());
                    }
                }
            }
            upstair_type::Payload::CancelOrderRequest(cancel_req) => {
                let symbol = cancel_req.symbol;
                let client_order_id = cancel_req.client_order_id.clone();

                match self.process_cancel_order_request(cancel_req) {
                    Ok(_) => {
                        comms.publish(
                            &self.order_result_topic,
                            upstair_type::Message {
                                header: upstair_type::MessageHeader {
                                    commit_at: comms.time(),
                                },
                                payload: upstair_type::Payload::OrderResult(
                                    upstair_type::order::OrderResult {
                                        symbol,
                                        at: comms.time(),
                                        client_order_id,
                                        status: upstair_type::order::OrderStatus::Canceled,
                                        filled_quantity: 0.0,
                                        price: 0.0,
                                        is_buy: false,
                                    },
                                ),
                            },
                        );
                    }
                    Err(e) => {
                        debug!("ingest_order_request: {}", e);
                        self.stats.on_event("cancel_order_fail");
                    }
                }
            }
            _ => {
                error!("ingest_market_data: data is not expected");
            }
        }
    }

    fn process_order_request(
        &mut self,
        req: upstair_type::order::OrderRequest,
        header: upstair_type::MessageHeader,
    ) -> anyhow::Result<()> {
        // update stats
        self.stats.on_order_submiited(
            req.quantity,
            req.side == upstair_type::order::TradeSide::Buy,
        );

        let symbol_info = self
            .symobl_info_manager
            .get(req.symbol)
            .ok_or_else(|| anyhow::anyhow!("symbol {} is not supported", req.symbol))?;
        // determine paying asset and amount
        let (pay_asset, pay_amt) = if req.side == upstair_type::order::TradeSide::Buy {
            (symbol_info.quote_asset, req.price * req.quantity)
        } else {
            (symbol_info.base_asset, req.quantity)
        };
        let pay_asset_balance = self.account.get_or_create(pay_asset);
        if !pay_asset_balance.try_lock_balance(pay_amt) {
            return Err(anyhow::anyhow!("insufficient balance"));
        }
        trace!(
            "-----\n{:?} client_id={} price={} qty={}\n{}",
            req.side,
            req.client_order_id,
            req.price,
            req.quantity,
            account_brief(&self.account)
        );
        let market = self
            .market_by_symbol
            .get_mut(req.symbol)
            .ok_or_else(|| anyhow::anyhow!("symbol {} has no market", req.symbol))?;
        market.add_order(simple_market::LimitOrder {
            submit_at: header.commit_at,
            side: req.side,
            order_id: req.client_order_id,
            price: req.price,
            quantity: req.quantity,
            filled: 0.0,
        });
        Ok(())
    }

    fn process_cancel_order_request(
        &mut self,
        cancel_req: upstair_type::order::CancelOrderRequest,
    ) -> anyhow::Result<()> {
        // update stats
        self.stats.on_order_cancel();

        let symbol_info = self
            .symobl_info_manager
            .get(cancel_req.symbol)
            .ok_or_else(|| anyhow::anyhow!("symbol {} is not supported", cancel_req.symbol))?;
        let market = self
            .market_by_symbol
            .get_mut(cancel_req.symbol)
            .ok_or_else(|| anyhow::anyhow!("symbol {} has no market", cancel_req.symbol))?;

        // determine paying asset and amount
        let order = market.get_order(&cancel_req.client_order_id);
        if order.is_none() {
            return Err(anyhow::anyhow!(
                "order {} not found",
                cancel_req.client_order_id
            ));
        };
        let order = order.unwrap();
        let (locked_asset, locked_amt) = if order.side == upstair_type::order::TradeSide::Buy {
            (
                symbol_info.quote_asset,
                order.price * (order.quantity - order.filled),
            )
        } else {
            (symbol_info.base_asset, order.quantity - order.filled)
        };
        self.account
            .get_or_create(locked_asset)
            .unlock_balance(locked_amt);
        trace!(
            "-----\nCancel {:?} client_id={} price={} qty={} filled={}\n{}",
            order.side,
            cancel_req.client_order_id,
            order.price,
            order.quantity,
            order.filled,
            account_brief(&self.account)
        );

        market.cancel_order(&cancel_req.client_order_id);
        Ok(())
    }

    fn make_account_update(account: &Account) -> upstair_type::account::AccountUpdate {
        upstair_type::account::AccountUpdate {
            updates: account
                .asset_to_balance
                .iter()
                .map(|(asset, balance)| {
                    (
                        *asset,
                        upstair_type::account::AccountAssetUpdate {
                            balance: balance.balance,
                            locked: balance.locked,
                        },
                    )
                })
                .collect(),
        }
    }
    fn make_account_update_for_asset(
        account: &Account,
        asset: &[&'static str],
    ) -> upstair_type::account::AccountUpdate {
        upstair_type::account::AccountUpdate {
            updates: asset
                .iter()
                .map(|asset| {
                    let balance = account
                        .asset_to_balance
                        .get(asset)
                        .unwrap_or(&AssetBalance {
                            balance: 0.0,
                            locked: 0.0,
                        });
                    (
                        *asset,
                        upstair_type::account::AccountAssetUpdate {
                            balance: balance.balance,
                            locked: balance.locked,
                        },
                    )
                })
                .collect(),
        }
    }
}

#[derive(Default)]
pub struct MarketAgentBuilder {
    market_data_topic: Option<ReadTopicHandle>,
    order_topic: Option<ReadTopicHandle>,
    order_result_topic: Option<WriteTopicHandle>,
    account_topic: Option<WriteTopicHandle>,

    symobl_info_manager: Option<SymbolInfoManager>,
    intial_balance: HashMap<String, f64>,
}

impl MarketAgentBuilder {
    // add balanace
    pub fn with_initial_balance(mut self, asset: impl Into<String>, balance: f64) -> Self {
        self.intial_balance.insert(asset.into(), balance);
        self
    }

    // set symbol info manager
    pub fn with_symbol_info_manager(mut self, manager: SymbolInfoManager) -> Self {
        self.symobl_info_manager = Some(manager);
        self
    }
}

impl ModuleBuilder for MarketAgentBuilder {
    fn init_comm(&mut self, comms: &mut dyn upstair_type::module::ModuleCommsBuilder) {
        let market_data_topic = comms.get_topic("market_data");
        let order_topic = comms.get_topic("order");
        let order_result_topic = comms.get_topic("order_result");
        let account_topic = comms.get_topic("account");

        self.market_data_topic = comms.subscribe_topic(&market_data_topic).into();
        self.order_topic = comms.subscribe_topic(&order_topic).into();
        self.order_result_topic = comms.publish_topic(&order_result_topic).into();
        self.account_topic = comms.publish_topic(&account_topic).into();
    }

    fn name(&self) -> &str {
        "market_agent"
    }

    fn build(self: Box<Self>) -> Box<dyn Module> {
        Box::new(MarketAgent {
            market_data_topic: self.market_data_topic.unwrap(),
            order_topic: self.order_topic.unwrap(),
            order_result_topic: self.order_result_topic.unwrap(),
            account_topic: self.account_topic.unwrap(),
            market_by_symbol: std::collections::HashMap::new(),
            account: Account::default(),
            symobl_info_manager: self.symobl_info_manager.unwrap(),
            fee_account: Account::default(),
            stats: MarketStats::default(),
            initial_balance: self.intial_balance.into_iter().collect(),
            last_account_summary_send_time: UNIX_EPOCH,
        })
    }
}
