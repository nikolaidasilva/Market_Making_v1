use std::time::{Duration, Instant};

use crate::config::Config;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskState {
    Normal,
    Cautious,
    Halted,
}

pub struct RiskManager {
    pub state: RiskState,
    pub inventory: i64,
    pub max_inventory: i64,
    pub hard_inventory_cap: i64,
    pub daily_pnl_cents: i64,
    pub daily_loss_cap_cents: i64,
    pub ws_connected: bool,
    pub last_book_update: Instant,
    pub max_book_staleness: Duration,

    // ── Pair tracking ──
    open_yes_legs: Vec<i64>,
    open_no_legs: Vec<i64>,
    pub realized_pair_profit_cents: i64,
    pub pairs_completed: u64,
    pub open_leg_cost_cents: i64,
}

impl RiskManager {
    pub fn new(config: &Config) -> Self {
        Self {
            state: RiskState::Normal,
            inventory: 0,
            max_inventory: config.max_inventory,
            hard_inventory_cap: config.hard_inventory_cap,
            daily_pnl_cents: 0,
            daily_loss_cap_cents: config.daily_loss_cap_cents,
            ws_connected: false,
            last_book_update: Instant::now(),
            max_book_staleness: Duration::from_secs(5),
            open_yes_legs: Vec::new(),
            open_no_legs: Vec::new(),
            realized_pair_profit_cents: 0,
            pairs_completed: 0,
            open_leg_cost_cents: 0,
        }
    }

    pub fn carry_daily_pnl(&mut self, pnl: i64) {
        self.daily_pnl_cents = pnl;
    }

    fn evaluate_state(&mut self) {
        if self.inventory.abs() >= self.hard_inventory_cap {
            tracing::error!(
                inventory = self.inventory,
                cap = self.hard_inventory_cap,
                "HARD INVENTORY CAP HIT — HALTING"
            );
            self.state = RiskState::Halted;
            return;
        }

        if self.daily_pnl_cents <= -self.daily_loss_cap_cents {
            tracing::error!(
                pnl = self.daily_pnl_cents,
                cap = self.daily_loss_cap_cents,
                "DAILY LOSS CAP HIT — HALTING"
            );
            self.state = RiskState::Halted;
            return;
        }

        if !self.ws_connected {
            tracing::warn!("WS disconnected — HALTING");
            self.state = RiskState::Halted;
            return;
        }

        if self.last_book_update.elapsed() > self.max_book_staleness {
            tracing::warn!("Book stale for > {:?} — HALTING", self.max_book_staleness);
            self.state = RiskState::Halted;
            return;
        }

        if self.inventory.abs() >= self.max_inventory {
            self.state = RiskState::Cautious;
            return;
        }

        self.state = RiskState::Normal;
    }

    pub fn tick(&mut self) -> RiskState {
        self.evaluate_state();
        self.state
    }

    /// Record a fill and track PnL correctly.
    pub fn record_fill(
        &mut self,
        side: &str,
        _action: &str,
        yes_price: i64,
        count: i64,
        post_position: i64,
    ) {
        self.inventory = post_position;

        for _ in 0..count {
            if side == "yes" {
                if let Some(no_yes_price) = self.open_no_legs.pop() {
                    let no_cost = 100 - no_yes_price;
                    let pair_cost = yes_price + no_cost;
                    let profit = 100 - pair_cost;
                    self.realized_pair_profit_cents += profit;
                    self.daily_pnl_cents += profit;
                    self.open_leg_cost_cents -= no_cost;
                    self.pairs_completed += 1;
                    tracing::info!(
                        yes_price,
                        no_cost,
                        pair_cost,
                        profit,
                        total_realized = self.realized_pair_profit_cents,
                        pairs = self.pairs_completed,
                        "PAIR COMPLETED"
                    );
                } else {
                    self.open_yes_legs.push(yes_price);
                    self.open_leg_cost_cents += yes_price;
                }
            } else {
                let no_cost = 100 - yes_price;
                if let Some(yes_cost) = self.open_yes_legs.pop() {
                    let pair_cost = yes_cost + no_cost;
                    let profit = 100 - pair_cost;
                    self.realized_pair_profit_cents += profit;
                    self.daily_pnl_cents += profit;
                    self.open_leg_cost_cents -= yes_cost;
                    self.pairs_completed += 1;
                    tracing::info!(
                        yes_cost,
                        no_cost,
                        pair_cost,
                        profit,
                        total_realized = self.realized_pair_profit_cents,
                        pairs = self.pairs_completed,
                        "PAIR COMPLETED"
                    );
                } else {
                    self.open_no_legs.push(yes_price);
                    self.open_leg_cost_cents += no_cost;
                }
            }
        }

        tracing::info!(
            side,
            yes_price,
            count,
            inventory = self.inventory,
            daily_pnl = self.daily_pnl_cents,
            realized = self.realized_pair_profit_cents,
            open_yes = self.open_yes_legs.len(),
            open_no = self.open_no_legs.len(),
            open_cost = self.open_leg_cost_cents,
            "Fill recorded"
        );
    }

    pub fn set_ws_connected(&mut self, connected: bool) {
        self.ws_connected = connected;
        if !connected {
            self.state = RiskState::Halted;
        }
    }

    pub fn book_updated(&mut self) {
        self.last_book_update = Instant::now();
    }

    pub fn can_bid(&self) -> bool {
        self.state != RiskState::Halted && self.inventory < self.max_inventory
    }

    pub fn can_ask(&self) -> bool {
        self.state != RiskState::Halted && self.inventory > -self.max_inventory
    }

    pub fn should_quote(&self) -> bool {
        self.state != RiskState::Halted
    }

    pub fn halt(&mut self, reason: &str) {
        tracing::error!(reason, "MANUAL HALT");
        self.state = RiskState::Halted;
    }

    pub fn reset(&mut self) {
        tracing::warn!("Risk state reset to Normal");
        self.state = RiskState::Normal;
    }
}