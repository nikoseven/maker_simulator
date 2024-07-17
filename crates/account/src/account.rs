use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct AssetBalance {
    pub balance: f64,
    pub locked: f64,
}

impl AssetBalance {
    pub fn try_lock_balance(&mut self, amount: f64) -> bool {
        if self.balance >= self.locked + amount {
            self.locked += amount;
            true
        } else {
            false
        }
    }

    pub fn unlock_balance(&mut self, amount: f64) {
        assert!(self.locked + 1e-8 >= amount);
        self.locked -= amount;
    }

    pub fn consume_locked(&mut self, amount: f64) {
        assert!(self.locked - amount > -0.1);
        self.locked -= amount;
        self.balance -= amount;
    }

    pub fn deduce_balance(&mut self, amount: f64) {
        self.balance -= amount;
    }

    pub fn add_balance(&mut self, amount: f64) {
        self.balance += amount;
    }
}

#[derive(Debug, Clone, Default)]
pub struct Account {
    pub asset_to_balance: HashMap<&'static str, AssetBalance>,
}

impl Account {
    pub fn get_or_create(&mut self, asset: &'static str) -> &mut AssetBalance {
        self.asset_to_balance.entry(asset).or_default()
    }
}
