use crate::symbol_info::SymbolInfo;

#[derive(Debug)]
pub struct SymbolTradeResult {
    pub pay_asset: &'static str,
    pub recv_asset: &'static str,
    pub fee_asset: &'static str,
    pub pay_qty: f64,
    pub recv_qty: f64,
    pub fee_qty: f64,
}

pub fn calc_trade_result(
    symbol_info: &SymbolInfo,
    price: f64,
    qty: f64,
    is_buy: bool,
) -> SymbolTradeResult {
    let (pay_qty, pay_asset, recv_qty, recv_asset) = if is_buy {
        (
            qty * price,
            symbol_info.quote_asset,
            qty,
            symbol_info.base_asset,
        )
    } else {
        (
            qty,
            symbol_info.base_asset,
            qty * price,
            symbol_info.quote_asset,
        )
    };
    let fee_asset = recv_asset;
    let fee_qty = recv_qty * symbol_info.fee_rate;
    let recv_qty = recv_qty - fee_qty;
    SymbolTradeResult {
        pay_asset,
        recv_asset,
        pay_qty,
        recv_qty,
        fee_asset,
        fee_qty,
    }
}
