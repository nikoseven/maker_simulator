#[derive(Debug, Clone)]
pub struct AccountAssetUpdate {
    pub balance: f64,
    pub locked: f64,
}

#[derive(Debug, Clone)]
pub struct AccountUpdate {
    pub updates: Vec<(&'static str, AccountAssetUpdate)>,
}
