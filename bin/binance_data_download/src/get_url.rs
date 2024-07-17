#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub enum BinanceBizType {
    #[allow(dead_code)]
    Spot,
    FutureUm,
}

impl BinanceBizType {
    pub fn base_url(&self) -> &'static str {
        match self {
            BinanceBizType::Spot => "https://data.binance.vision/data/spot/daily",
            BinanceBizType::FutureUm => "https://data.binance.vision/data/futures/um/daily",
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
pub enum DataProductName {
    Trades,
    BookTicker,
}

impl Default for DataProductName {
    fn default() -> Self {
        Self::Trades
    }
}

impl DataProductName {
    fn to_str(&self) -> &str {
        match self {
            DataProductName::Trades => "trades",
            DataProductName::BookTicker => "bookTicker",
        }
    }
}

pub fn get_data_url(
    symbol: &str,
    biz_type: BinanceBizType,
    product_name: DataProductName,
    date_str: &str,
) -> String {
    let base_url = biz_type.base_url();
    let product_name_str = product_name.to_str();
    let file_name = format!("{}-{}-{}.zip", symbol, product_name_str, date_str);

    format!("{}/{}/{}/{}", base_url, product_name_str, symbol, file_name)
}
