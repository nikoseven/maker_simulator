mod duration_sampler;
mod time_volatility;
mod volatility;
use std::time::{SystemTime, UNIX_EPOCH};

use polars::{df, io::parquet::ParquetWriter};
use time_volatility::TimeVolatility;
use tracing::info;
use upstair_type::order::TradeSide;
use yata::{core::Method, helpers::Peekable};

use stepper_world::{
    order_tracker::{Order, OrderStatus},
    StepperWorld,
};

use symbol_info::SymbolInfoManager;

#[derive(Debug)]
pub struct CancelOrder {
    pub symbol: &'static str,
    pub order_id: String,
}

#[derive(Debug)]
pub struct PlaceOrderData {
    pub symbol: &'static str,
    pub order_id: String,
    pub price: f64,
    pub side: TradeSide,
    pub quantity: f64,
}

#[derive(Debug)]
pub enum Action {
    CancelOrder(CancelOrder),
    PlaceOrder(PlaceOrderData),
}

macro_rules! struct_to_dataframe {
    ($input:expr, [$($field:ident),+]) => {
        {
            let len = $input.len().to_owned();
            // Extract the field values into separate vectors
            $(let mut $field = Vec::with_capacity(len);)*
            for e in $input.into_iter() {
                $($field.push(e.$field);)*
            }
            df! {
                $(stringify!($field) => $field,)*
            }
        }
    };
}

struct QuoteDebugLog {
    time: i64,
    price: f64,
    qty: f64,
    fair_price: f64,
    is_bid: bool,
    best_bid_price: f64,
    best_bid_qty: f64,
    best_ask_price: f64,
    best_ask_qty: f64,
    id: String,
}

pub struct AmmStrategy {
    pub intial_position: f64,
    pub target_ratio: f64,
    pub actions: Vec<Action>,
    pub symbol_info_manager: SymbolInfoManager,

    pub symbol: &'static str,
    pub base_asset: &'static str,
    pub quote_asset: &'static str,

    pub vol_tracker: Option<TimeVolatility>,

    pub gamma: f64,

    pub ts_seq: Vec<i64>,
    pub vol_seq: Vec<f64>,
    quote_seq: Vec<QuoteDebugLog>,
    fill_seq_order_id: Vec<String>,
    fill_seq_qty: Vec<f64>,

    pub uniq_quote_round: u64,
}

fn convert_order_to_action(symbol: &'static str, order: Order) -> Action {
    Action::PlaceOrder(PlaceOrderData {
        symbol,
        order_id: order.order_id,
        price: order.price,
        side: order.side,
        quantity: order.quantity,
    })
}

fn inverse_lerp(v: f64, a: f64, b: f64) -> f64 {
    (v - a) / (b - a)
}

fn inverse_lerp_with_clamp(v: f64, a: f64, b: f64) -> f64 {
    inverse_lerp(v.clamp(a, b), a, b)
}

const ENABLE_VOL_DEBUG: bool = true;

impl AmmStrategy {
    pub fn new(symbol: &'static str, symbol_info_manager: SymbolInfoManager) -> AmmStrategy {
        let symbol_info = symbol_info_manager
            .get(symbol)
            .expect("symbol in symbol info manager");
        let base_asset = symbol_info.base_asset;
        let quote_asset = symbol_info.quote_asset;
        AmmStrategy {
            symbol,
            actions: Vec::new(),
            intial_position: 0.0,
            target_ratio: 0.5,
            symbol_info_manager,
            base_asset,
            quote_asset,
            vol_tracker: None,
            gamma: 1.0,
            ts_seq: vec![],
            vol_seq: vec![],
            quote_seq: vec![],
            fill_seq_order_id: vec![],
            fill_seq_qty: vec![],
            uniq_quote_round: 0,
        }
    }

    fn mid_price(&self, world: &StepperWorld) -> f64 {
        (world.best_ask_price + world.best_bid_price) / 2.0
    }

    fn wap_price(&self, world: &StepperWorld) -> f64 {
        (world.best_ask_price * world.best_bid_qty + world.best_bid_price * world.best_ask_qty)
            / (world.best_ask_qty + world.best_bid_qty)
    }

    fn calc_q(&self, world: &StepperWorld) -> f64 {
        let base_asset_amt = world
            .account
            .asset_to_balance
            .get(self.base_asset)
            .map(|x| x.balance)
            .unwrap_or(0.0);
        let quote_asset_amt = world
            .account
            .asset_to_balance
            .get(self.quote_asset)
            .map(|x| x.balance)
            .unwrap_or(0.0);
        let price = self.mid_price(world);
        let base_value = base_asset_amt * price;
        let inventory_value = base_value + quote_asset_amt;
        let inventory_value_base = inventory_value / price;

        let target_base_asset_amt = inventory_value_base * self.target_ratio;
        (base_asset_amt - target_base_asset_amt) / inventory_value_base
    }

    fn calc_inventory_base(&self, world: &StepperWorld) -> f64 {
        let base_asset_amt = world
            .account
            .asset_to_balance
            .get(self.base_asset)
            .map(|x| x.balance)
            .unwrap_or(0.0);
        let quote_asset_amt = world
            .account
            .asset_to_balance
            .get(self.quote_asset)
            .map(|x| x.balance)
            .unwrap_or(0.0);
        let price = self.mid_price(world);
        let base_value = base_asset_amt * price;
        let inventory_value = base_value + quote_asset_amt;
        inventory_value / price
    }

    fn update_vol(&mut self, world: &StepperWorld) {
        const USE_WAP: bool = true;
        if self.vol_tracker.is_none() {
            if USE_WAP {
                if world.wap_buf.is_empty() {
                    return;
                }
                self.vol_tracker = Some(
                    TimeVolatility::new((60, 1000), &(world.wap_buf[0].0, world.wap_buf[0].1))
                        .unwrap(),
                );
            } else {
                if world.trade_buf.is_empty() {
                    return;
                }
                self.vol_tracker = Some(
                    TimeVolatility::new(
                        (60, 1000),
                        &(world.trade_buf[0].time, world.trade_buf[0].price),
                    )
                    .unwrap(),
                );
            }
        }
        if USE_WAP {
            world.trade_buf.iter().for_each(|trade| {
                self.vol_tracker
                    .as_mut()
                    .unwrap()
                    .next(&(trade.time, trade.price));
            });
        } else {
            world.wap_buf.iter().for_each(|(time, price)| {
                self.vol_tracker.as_mut().unwrap().next(&(*time, *price));
            });
        }

        if ENABLE_VOL_DEBUG {
            self.ts_seq
                .push(world.now.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64);
            self.vol_seq.push(self.vol_tracker.as_ref().unwrap().peek())
        }
    }

    fn vol(&self) -> f64 {
        self.vol_tracker.as_ref().unwrap().peek()
    }

    // make_decision take world as input
    pub fn run(&mut self, world: &mut StepperWorld) {
        self.actions.clear();
        self.update_vol(world);

        if ENABLE_VOL_DEBUG {
            let filled_event_buf = std::mem::take(&mut world.filled_event_buf);
            filled_event_buf.into_iter().for_each(|(order_id, filled)| {
                self.fill_seq_order_id.push(order_id);
                self.fill_seq_qty.push(filled);
            });
        }

        if self.intial_position == 0.0 {
            if world
                .account
                .asset_to_balance
                .get(self.base_asset)
                .is_none()
            {
                info!("Wait for asset information to be available.");
                return;
            } else {
                self.intial_position = world
                    .account
                    .asset_to_balance
                    .get(self.base_asset)
                    .unwrap()
                    .balance;
                self.target_ratio = self.intial_position / self.calc_inventory_base(world);
                tracing::trace!(
                    "Setup AMM Strategy Params : inital_pos={}{btc} invetory={}{btc} target_ratio={}",
                    self.intial_position,
                    self.calc_inventory_base(world),
                    self.target_ratio,
                    btc=self.base_asset
                );
            }
        }
        if world.best_ask_price == 0.0
            || world.best_bid_price == 0.0
            || world.latest_market_price == 0.0
            || self.vol_tracker.is_none()
        {
            info!("Wait for market data to be available.");
            return;
        }

        const USE_WAP_AS_FAIR_PRICE: bool = true;
        let fair_price = if USE_WAP_AS_FAIR_PRICE {
            self.wap_price(world)
        } else {
            self.mid_price(world)
        };
        let q = self.calc_q(world);
        let vol = self.vol();
        let reservation_price = fair_price - (q * self.gamma * vol);
        let optimal_spread = self.gamma * vol;
        tracing::trace!(
            "price={:.3} q={:.3} vol={:.3} res_price={:.3} spread={:.3} opt_spread={:.3}",
            fair_price,
            q,
            vol,
            reservation_price,
            world.best_ask_price - world.best_bid_price,
            optimal_spread
        );

        let base_asset_balance = world.account.asset_to_balance.get(self.base_asset).unwrap();
        let (low_water_level, high_water_level) =
            (self.intial_position * 0.5, self.intial_position * 1.5);
        let skew = inverse_lerp_with_clamp(
            base_asset_balance.balance,
            low_water_level,
            high_water_level,
        );
        info!(
            "Skew: {skew} {base_asset_balance:?} {}",
            self.intial_position
        );

        // const MM_PRICE_SPREAD: f64 = 15.0;
        const MM_QUANTITY: f64 = 0.01;
        const MM_ORDER_EXPIRE_MILLSECONDS: u64 = 100;
        let now = world.now;
        let t_since_epoch = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let uniq_token = self.uniq_quote_round;
        self.uniq_quote_round += 1;
        // make orders around latest price
        let (buy, sell) = (
            Order {
                order_id: format!("B{}", uniq_token),
                price: (reservation_price - optimal_spread * 0.5).min(world.best_bid_price),
                side: TradeSide::Buy,
                quantity: MM_QUANTITY,
                filled: 0.0,
                status: OrderStatus::Open,
                created_at: now,
            },
            Order {
                order_id: format!("S{}", uniq_token),
                price: (reservation_price + optimal_spread * 0.5).max(world.best_ask_price),
                side: TradeSide::Sell,
                quantity: MM_QUANTITY,
                filled: 0.0,
                status: OrderStatus::Open,
                created_at: now,
            },
        );

        if ENABLE_VOL_DEBUG {
            self.quote_seq.push(QuoteDebugLog {
                time: t_since_epoch as i64,
                price: buy.price,
                qty: buy.quantity,
                fair_price: self.mid_price(&world),
                is_bid: true,
                id: buy.order_id.clone(),
                best_bid_price: world.best_bid_price,
                best_bid_qty: world.best_bid_qty,
                best_ask_price: world.best_ask_price,
                best_ask_qty: world.best_ask_qty,
            });
            self.quote_seq.push(QuoteDebugLog {
                time: t_since_epoch as i64,
                price: sell.price,
                qty: sell.quantity,
                fair_price: self.mid_price(&world),
                is_bid: false,
                id: sell.order_id.clone(),
                best_bid_price: world.best_bid_price,
                best_bid_qty: world.best_bid_qty,
                best_ask_price: world.best_ask_price,
                best_ask_qty: world.best_ask_qty,
            });
        }

        tracing::trace!(
            "bid={:.3} ask={:.3} quote_bid={:.3} quote_ask={:.3}",
            world.best_bid_price,
            world.best_ask_price,
            world.best_bid_price - buy.price,
            sell.price - world.best_ask_price
        );

        // put order
        self.actions.push(convert_order_to_action(self.symbol, buy));
        self.actions
            .push(convert_order_to_action(self.symbol, sell));

        // clear expired orders
        for order in world.order_tracker.iter() {
            if order.status == OrderStatus::CancelRequested {
                continue;
            }
            let order_exist_duration = now.duration_since(order.created_at);
            if order_exist_duration.is_err() {
                continue;
            }
            let order_exist_duration = order_exist_duration.unwrap();
            if order_exist_duration.as_millis() as u64 > MM_ORDER_EXPIRE_MILLSECONDS {
                self.actions.push(Action::CancelOrder(CancelOrder {
                    symbol: self.symbol,
                    order_id: order.order_id.clone(),
                }));
            }
        }
    }

    pub fn terminate(&mut self) {
        if ENABLE_VOL_DEBUG {
            let debug_vol_file_path = "data/vol.parquet";
            println!("DebugVol write to {debug_vol_file_path}");
            let mut vol_df = df!(
                "time" => std::mem::take(&mut self.ts_seq),
                "vol" => std::mem::take(&mut self.vol_seq)
            )
            .unwrap();
            let mut parquet_file = std::fs::File::create(debug_vol_file_path).unwrap();
            ParquetWriter::new(&mut parquet_file)
                .finish(&mut vol_df)
                .unwrap();

            let debug_quote_file_path = "data/quote.parquet";
            let quote_seq = std::mem::take(&mut self.quote_seq);
            let mut quote_seq_df = struct_to_dataframe!(
                quote_seq,
                [
                    time,
                    price,
                    qty,
                    fair_price,
                    is_bid,
                    id,
                    best_bid_price,
                    best_bid_qty,
                    best_ask_price,
                    best_ask_qty
                ]
            )
            .unwrap();
            let mut parquet_file = std::fs::File::create(debug_quote_file_path).unwrap();
            ParquetWriter::new(&mut parquet_file)
                .finish(&mut quote_seq_df)
                .unwrap();

            let trade_file_path = "data/trade.parquet";
            let mut trade_df = df!(
                "order_id" => std::mem::take(&mut self.fill_seq_order_id),
                "filled" => std::mem::take(&mut self.fill_seq_qty),
            )
            .unwrap();
            let mut parquet_file = std::fs::File::create(trade_file_path).unwrap();
            ParquetWriter::new(&mut parquet_file)
                .finish(&mut trade_df)
                .unwrap();
        }
    }
}
