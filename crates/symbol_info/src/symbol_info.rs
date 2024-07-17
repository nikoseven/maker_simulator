#[derive(Default, Debug, Clone)]

pub struct SymbolInfo {
    pub base_asset: &'static str,
    pub quote_asset: &'static str,
    pub fee_rate: f64,
}

#[derive(Default, Debug, Clone)]
pub struct SymbolInfoManager {
    pub symbol_info: std::collections::HashMap<&'static str, SymbolInfo>,
}

impl SymbolInfoManager {
    pub fn get(&self, symbol: &'static str) -> Option<&SymbolInfo> {
        self.symbol_info.get(&symbol)
    }

    // add symbol config
    pub fn with_symbol_config(
        mut self,
        symbol: &'static str,
        base_asset: &'static str,
        quote_asset: &'static str,
        fee_rate: f64,
    ) -> Self {
        self.symbol_info.insert(
            symbol,
            SymbolInfo {
                base_asset,
                quote_asset,
                fee_rate,
            },
        );
        self
    }
}
