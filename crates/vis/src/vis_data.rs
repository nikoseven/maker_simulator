use std::{collections::HashMap, sync::Arc, time::UNIX_EPOCH};

use account::account::Account;

use upstair_type::{
    data::market::BinanceTradeTick,
    order::{OrderResult, OrderStatus},
};

use crate::candle::OhlcvCandle;

#[derive(Default, Debug)]
pub struct TradeBrief {
    pub time: TimeInMs,
    pub is_buy: bool,
    pub price: f64,
    pub qty: f64,
}

#[derive(Default, Debug)]
pub struct MakerOrderBrief {
    pub price: f64,
    pub created_at: TimeInMs, // 0 for TBD
    pub ended_at: TimeInMs,   // 0 for TBD
    pub is_buy: bool,
}

#[derive(Default, Debug)]
pub struct DataBuffer {
    pub last_price: f64,
    pub order_count: i64,
    pub order_cancel_count: i64,
    pub order_result_count: i64,
    pub account: Account,
    pub profit_account: Account,

    pub latest_market_price: HashMap<&'static str, f64>,
    pub market_trades: Vec<BinanceTradeTick>,
    pub account_trades: Vec<TradeBrief>,

    pub order_updates: Vec<OrderResult>,

    pub commit_at: TimeInMs,
}

impl DataBuffer {
    pub fn take(&mut self) -> Self {
        Self {
            last_price: self.last_price,
            order_count: self.order_count,
            order_cancel_count: self.order_cancel_count,
            order_result_count: self.order_result_count,
            account: self.account.clone(),
            market_trades: std::mem::take(&mut self.market_trades),
            commit_at: self.commit_at,
            account_trades: std::mem::take(&mut self.account_trades),
            order_updates: std::mem::take(&mut self.order_updates),
            latest_market_price: self.latest_market_price.clone(),
            profit_account: self.profit_account.clone(),
        }
    }
}

#[derive(Default, Debug)]
pub struct DataState {
    pub market_trades: Vec<BinanceTradeTick>,
    pub account_trades: Vec<TradeBrief>,
    pub account_asset_history: HashMap<&'static str, Vec<(TimeInMs, f64)>>,
    pub order_briefs: HashMap<Arc<str>, MakerOrderBrief>,
}

impl DataState {
    pub fn update(&mut self, buffer: DataBuffer) {
        let mut buffer = buffer;
        self.market_trades.append(&mut buffer.market_trades);
        self.account_trades.append(&mut buffer.account_trades);

        let mut total_usdt_value = 0.0;
        for (asset, account) in buffer.account.asset_to_balance.iter() {
            self.account_asset_history
                .entry(asset)
                .or_default()
                .push((buffer.commit_at, account.balance));
            if let Some(asset_price) = buffer.latest_market_price.get(asset) {
                total_usdt_value += account.balance * asset_price;
            } else if asset == &"USDT" {
                total_usdt_value += account.balance;
            }
        }
        if total_usdt_value != 0.0 {
            self.account_asset_history
                .entry("EquityUSDT")
                .or_default()
                .push((buffer.commit_at, total_usdt_value));
        }

        let mut total_profit_usdt = 0.0;
        for (asset, account) in buffer.profit_account.asset_to_balance.iter() {
            if let Some(asset_price) = buffer.latest_market_price.get(asset) {
                total_profit_usdt += account.balance * asset_price;
            } else if asset == &"USDT" {
                total_profit_usdt += account.balance;
            }
        }
        if total_profit_usdt != 0.0 {
            self.account_asset_history
                .entry("ProfitUSDT")
                .or_default()
                .push((buffer.commit_at, total_profit_usdt));
        }

        for order_result in buffer.order_updates.drain(..) {
            let brief = self
                .order_briefs
                .entry(order_result.client_order_id.clone())
                .or_default();
            let order_result_t_in_ms = order_result
                .at
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as TimeInMs;
            match order_result.status {
                OrderStatus::New => {
                    brief.is_buy = order_result.is_buy;
                    brief.price = order_result.price;
                    brief.created_at = order_result_t_in_ms;
                }
                OrderStatus::PartiallyFilled => {}
                OrderStatus::Filled => {
                    brief.ended_at = order_result_t_in_ms;
                }
                OrderStatus::Canceled => {
                    brief.ended_at = order_result_t_in_ms;
                }
                OrderStatus::Rejected => {
                    brief.ended_at = order_result_t_in_ms;
                }
                _ => {}
            }
        }
    }
}

pub type TimeInMs = u64;
pub fn compute_candles_from_market_trades(
    trades: &[BinanceTradeTick],
    first_time_ms: TimeInMs,
    period_ms: TimeInMs,
) -> impl Iterator<Item = (TimeInMs, OhlcvCandle)> {
    let mut candles = vec![];

    // skip trades before first_time_ms
    let trade_after_first_time = trades.iter().skip_while(|t| t.time < first_time_ms);

    let mut current_candle_ts = first_time_ms;
    let mut current_candle: Option<OhlcvCandle> = None;
    for trade in trade_after_first_time {
        if trade.time >= current_candle_ts + period_ms {
            // the candle ends now
            if let Some(candle) = current_candle.take() {
                candles.push((current_candle_ts, candle));
            }
            // locate the latest candle start time in O(1)
            while current_candle_ts + period_ms <= trade.time {
                current_candle_ts += period_ms;
            }
        }
        // init or update the candle
        if let Some(candle) = current_candle.as_mut() {
            candle.update_latest_trade(trade.price, trade.qty);
        } else {
            current_candle = Some(OhlcvCandle::from_trade(trade.price, trade.qty));
        }
    }
    if let Some(candle) = current_candle.take() {
        candles.push((current_candle_ts, candle));
    }
    candles.into_iter()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_candles_from_trades() {
        let trades = vec![
            BinanceTradeTick {
                id: 1,
                price: 1.0,
                qty: 1.0,
                base_qty: 1.0,
                time: 0,
                is_buyer_maker: true,
                symbol: "",
            },
            BinanceTradeTick {
                id: 2,
                price: 2.0,
                qty: 2.0,
                base_qty: 2.0,
                time: 1,
                is_buyer_maker: true,
                symbol: "",
            },
            BinanceTradeTick {
                id: 3,
                price: 3.0,
                qty: 3.0,
                base_qty: 3.0,
                time: 2,
                is_buyer_maker: true,
                symbol: "",
            },
            BinanceTradeTick {
                id: 4,
                price: 4.0,
                qty: 4.0,
                base_qty: 4.0,
                time: 3,
                is_buyer_maker: true,
                symbol: "",
            },
            BinanceTradeTick {
                id: 5,
                price: 5.0,
                qty: 5.0,
                base_qty: 5.0,
                time: 4,
                is_buyer_maker: true,
                symbol: "",
            },
        ];
        let candles = compute_candles_from_market_trades(&trades, 1, 1);
        let candles: Vec<(TimeInMs, OhlcvCandle)> = candles.collect();
        assert_eq!(candles.len(), 4);
        assert_eq!(candles[0].1.close, 2.0);
        assert_eq!(candles[1].1.close, 3.0);
        assert_eq!(candles[2].1.close, 4.0);
        assert_eq!(candles[3].1.close, 5.0);

        let candles = compute_candles_from_market_trades(&trades, 0, 3);
        let candles: Vec<(TimeInMs, OhlcvCandle)> = candles.collect();
        assert_eq!(candles.len(), 2);
        assert_eq!(candles[0].1.close, 3.0);
        assert_eq!(candles[1].1.close, 5.0);
        assert_eq!(candles[0].1.volume, 6.0);
        assert_eq!(candles[1].1.volume, 9.0);

        let candles = compute_candles_from_market_trades(&trades, 4, 3);
        let candles: Vec<(TimeInMs, OhlcvCandle)> = candles.collect();
        assert_eq!(candles.len(), 1);

        let candles = compute_candles_from_market_trades(&trades, 100, 3);
        let candles: Vec<(TimeInMs, OhlcvCandle)> = candles.collect();
        assert_eq!(candles.len(), 0);
    }
}
