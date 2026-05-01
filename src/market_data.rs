use std::time::Instant;

const MAX_PRICE: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Yes,
    No,
}

#[derive(Debug, Clone)]
pub struct BookSide {
    pub quantities: [i64; MAX_PRICE],
    /// Bitmask: bit N is set if quantities[N] > 0. Prices 1-99 use bits 1-99.
    /// Finding best bid = 127 - mask.leading_zeros() which is O(1).
    pub occupied: u128,
    pub best_bid: i64,
    pub total_qty: i64,
}

impl BookSide {
    pub fn new() -> Self {
        Self {
            quantities: [0; MAX_PRICE],
            occupied: 0,
            best_bid: 0,
            total_qty: 0,
        }
    }

    #[inline(always)]
    fn recompute_best_bid(&mut self) {
        if self.occupied == 0 {
            self.best_bid = 0;
        } else {
            self.best_bid = (127 - self.occupied.leading_zeros()) as i64;
        }
    }

    pub fn apply_snapshot(&mut self, levels: &[(i64, i64)]) {
        self.quantities = [0; MAX_PRICE];
        self.occupied = 0;
        self.total_qty = 0;
        for &(price, qty) in levels {
            if price >= 1 && price <= 99 {
                self.quantities[price as usize] = qty;
                self.total_qty += qty;
                if qty > 0 {
                    self.occupied |= 1u128 << price;
                }
            }
        }
        self.recompute_best_bid();
    }

    #[inline]
    pub fn apply_delta(&mut self, price: i64, delta: i64) {
        if price < 1 || price > 99 {
            return;
        }
        let idx = price as usize;
        let old_qty = self.quantities[idx];
        let new_qty = (old_qty + delta).max(0);
        self.quantities[idx] = new_qty;
        self.total_qty += new_qty - old_qty;

        if new_qty > 0 {
            self.occupied |= 1u128 << idx;
        } else {
            self.occupied &= !(1u128 << idx);
        }

        if new_qty == 0 && price == self.best_bid {
            self.recompute_best_bid();
        } else if new_qty > 0 && price > self.best_bid {
            self.best_bid = price;
        }
    }

    #[inline]
    pub fn best_bid_qty(&self) -> i64 {
        if self.best_bid >= 1 && self.best_bid <= 99 {
            self.quantities[self.best_bid as usize]
        } else {
            0
        }
    }

    #[inline]
    pub fn best_bid_below(&self, below_price: i64) -> i64 {
        if below_price <= 1 {
            return 0;
        }
        let mask = self.occupied & ((1u128 << below_price) - 1);
        if mask == 0 {
            0
        } else {
            (127 - mask.leading_zeros()) as i64
        }
    }

    pub fn top_n(&self, n: usize) -> Vec<(i64, i64)> {
        let mut result = Vec::with_capacity(n);
        let mut remaining = self.occupied;
        for _ in 0..n {
            if remaining == 0 {
                break;
            }
            let p = (127 - remaining.leading_zeros()) as usize;
            result.push((p as i64, self.quantities[p]));
            remaining &= !(1u128 << p);
        }
        result
    }
}

#[derive(Debug, Clone)]
pub struct OrderBook {
    pub yes_bids: BookSide,
    pub no_bids: BookSide,
    pub last_update: Instant,
    pub seq: u64,
    pub initialized: bool,
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            yes_bids: BookSide::new(),
            no_bids: BookSide::new(),
            last_update: Instant::now(),
            seq: 0,
            initialized: false,
        }
    }

    pub fn apply_snapshot(
        &mut self,
        yes_levels: &[(i64, i64)],
        no_levels: &[(i64, i64)],
        seq: u64,
    ) {
        self.yes_bids.apply_snapshot(yes_levels);
        self.no_bids.apply_snapshot(no_levels);
        self.seq = seq;
        self.last_update = Instant::now();
        self.initialized = true;
    }

    #[inline]
    pub fn apply_delta(&mut self, side: Side, price: i64, delta: i64, seq: u64) -> bool {
        if seq != self.seq + 1 {
            tracing::warn!(
                expected = self.seq + 1,
                got = seq,
                "Sequence gap detected — book state unreliable"
            );
            return false;
        }
        match side {
            Side::Yes => self.yes_bids.apply_delta(price, delta),
            Side::No => self.no_bids.apply_delta(price, delta),
        }
        self.seq = seq;
        self.last_update = Instant::now();
        true
    }

    #[inline]
    pub fn best_yes_bid(&self) -> i64 {
        self.yes_bids.best_bid
    }

    #[inline]
    pub fn best_yes_ask(&self) -> i64 {
        if self.no_bids.best_bid > 0 {
            100 - self.no_bids.best_bid
        } else {
            0
        }
    }

    pub fn yes_spread(&self) -> i64 {
        let ask = self.best_yes_ask();
        let bid = self.best_yes_bid();
        if ask > 0 && bid > 0 {
            ask - bid
        } else {
            i64::MAX
        }
    }

    pub fn midprice(&self) -> Option<f64> {
        let bid = self.best_yes_bid();
        let ask = self.best_yes_ask();
        if bid > 0 && ask > 0 && ask > bid {
            Some((bid as f64 + ask as f64) / 2.0)
        } else {
            None
        }
    }

    pub fn microprice(&self) -> Option<f64> {
        let bid = self.best_yes_bid();
        let ask = self.best_yes_ask();
        if bid <= 0 || ask <= 0 || ask <= bid {
            return None;
        }
        let bid_qty = self.yes_bids.best_bid_qty() as f64;
        let ask_qty = self.no_bids.best_bid_qty() as f64;
        if bid_qty + ask_qty == 0.0 {
            return self.midprice();
        }
        Some((bid as f64 * ask_qty + ask as f64 * bid_qty) / (bid_qty + ask_qty))
    }

    pub fn imbalance_ratio(&self) -> Option<f64> {
        let bid_qty = self.yes_bids.best_bid_qty() as f64;
        let ask_qty = self.no_bids.best_bid_qty() as f64;
        if ask_qty > 0.0 {
            Some(bid_qty / ask_qty)
        } else if bid_qty > 0.0 {
            Some(f64::MAX)
        } else {
            None
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Trend Tracker
//
// Tracks short-term midprice direction using an EWMA of midprice deltas.
// The strategy uses this to fade quotes away from a trending market:
//   - Market trending UP (YES getting more expensive):
//     Our YES bid is the dangerous side (we buy high, it may come back down)
//     → Lag our YES bid by 1¢ (don't chase the rising price)
//     Our NO bid (YES ask) is the safe side (selling YES high = good)
//     → Keep aggressive
//   - Market trending DOWN (YES getting cheaper):
//     Our NO bid is the dangerous side (we buy NO high, it may come back)
//     → Lag our NO bid (raise our YES ask) by 1¢
//     Our YES bid is the safe side (buying YES cheap = good)
//     → Keep aggressive
//
// This is NOT adverse selection (which pulls all quotes). We stay in the
// market on both sides — we just quote smarter during directional moves.
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrendDirection {
    Up,    // YES price trending up (midprice rising)
    Down,  // YES price trending down (midprice falling)
    Flat,  // No significant trend
}

pub struct TrendTracker {
    /// EWMA of midprice changes. Positive = trending up, negative = trending down.
    ewma: f64,
    /// Previous midprice for computing deltas.
    prev_mid: f64,
    /// EWMA decay factor. Lower = more responsive, higher = smoother.
    /// 0.3 means ~70% weight on new delta + 30% on history. Good for fast markets.
    alpha: f64,
    /// Threshold in cents for declaring a trend. Below this = Flat.
    /// 0.4 means the EWMA of deltas needs to average > 0.4¢ per update to trigger.
    threshold: f64,
    /// Whether we've seen at least one midprice (need 2 to compute delta).
    warmed_up: bool,
}

impl TrendTracker {
    pub fn new(alpha: f64, threshold: f64) -> Self {
        Self {
            ewma: 0.0,
            prev_mid: 0.0,
            alpha,
            threshold,
            warmed_up: false,
        }
    }

    /// Update with the current midprice. Call this on every book change.
    #[inline]
    pub fn update(&mut self, midprice: f64) {
        if !self.warmed_up {
            self.prev_mid = midprice;
            self.warmed_up = true;
            return;
        }

        let delta = midprice - self.prev_mid;
        self.prev_mid = midprice;

        // Skip zero deltas — they're noise from qty changes at same price
        if delta == 0.0 {
            // Still decay the EWMA slightly toward zero (mean-reversion)
            self.ewma *= 1.0 - self.alpha;
            return;
        }

        // EWMA update: ewma = alpha * delta + (1 - alpha) * ewma
        self.ewma = self.alpha * delta + (1.0 - self.alpha) * self.ewma;
    }

    /// Get the current trend direction.
    #[inline]
    pub fn direction(&self) -> TrendDirection {
        if self.ewma > self.threshold {
            TrendDirection::Up
        } else if self.ewma < -self.threshold {
            TrendDirection::Down
        } else {
            TrendDirection::Flat
        }
    }

    /// Get the raw EWMA value for logging.
    #[inline]
    pub fn ewma_value(&self) -> f64 {
        self.ewma
    }

    /// Reset the tracker (e.g. on snapshot/reconnect).
    pub fn reset(&mut self) {
        self.ewma = 0.0;
        self.prev_mid = 0.0;
        self.warmed_up = false;
    }
}