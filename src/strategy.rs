use crate::config::Config;
use crate::execution::OrderTracker;
use crate::market_data::{OrderBook, TrendDirection};
use crate::risk::RiskManager;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DesiredQuote {
    pub bid_price: i64,
    pub ask_price: i64,
}

#[derive(Debug)]
pub enum StrategyAction {
    PlaceBid { price_cents: i64, count: i64 },
    PlaceAsk { price_cents: i64, count: i64 },
    CancelOrder { order_id: String },
    AmendBid { order_id: String, client_order_id: String, new_price_cents: i64, count: i64 },
    AmendAsk { order_id: String, client_order_id: String, new_price_cents: i64, count: i64 },
    CancelAll,
}

pub struct Strategy {
    pub config: Config,
}

impl Strategy {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Compute where we want to quote.
    ///
    /// Three layers of quote adjustment:
    ///
    /// 1. BASE PLACEMENT (spread-based):
    ///    1-2¢ spread: Join both sides at BBO. Pair profit = spread¢.
    ///    3¢+ spread: Improve by 1¢ both sides for queue priority.
    ///
    /// 2. TREND FADE (this is the new part):
    ///    If market is trending, lag the DANGEROUS side by 1¢.
    ///    - Trending UP: lag the YES bid (don't chase rising price), keep ask aggressive
    ///    - Trending DOWN: lag the YES ask (don't sell into falling price), keep bid aggressive
    ///    This prevents the "buy at 30, sell at 29, buy at 29, sell at 28" death spiral.
    ///
    /// 3. INVENTORY SKEW (asymmetric):
    ///    Only adjusts the side that reduces inventory. Additive with trend fade.
    pub fn compute_desired_quotes(
        &self,
        book: &OrderBook,
        tracker: &OrderTracker,
        ticker: &str,
        risk: &RiskManager,
        trend: TrendDirection,
    ) -> DesiredQuote {
        if !risk.should_quote() || !book.initialized {
            return DesiredQuote { bid_price: 0, ask_price: 0 };
        }

        // ── Self-exclusion: find the true best bid/ask ignoring our own orders ──
        let mut true_bid = book.best_yes_bid();
        if let Some(my_bid) = tracker.resting_bid(ticker) {
            if true_bid == my_bid.price_cents
                && book.yes_bids.quantities[true_bid as usize] <= my_bid.remaining
            {
                true_bid = book.yes_bids.best_bid_below(my_bid.price_cents);
            }
        }

        let mut true_no_bid = book.no_bids.best_bid;
        if let Some(my_ask) = tracker.resting_ask(ticker) {
            if true_no_bid == my_ask.price_cents
                && book.no_bids.quantities[true_no_bid as usize] <= my_ask.remaining
            {
                true_no_bid = book.no_bids.best_bid_below(my_ask.price_cents);
            }
        }

        let true_ask = if true_no_bid > 0 { 100 - true_no_bid } else { 0 };

        if true_bid <= 0 || true_ask <= 0 || true_ask <= true_bid {
            return DesiredQuote { bid_price: 0, ask_price: 0 };
        }

        let spread = true_ask - true_bid;

        // ── Layer 1: Base quote placement by spread width ──
        let (mut bid, mut ask) = if spread <= 2 {
            (true_bid, true_ask)
        } else {
            (true_bid + 1, true_ask - 1)
        };

        // When market is trending, the side we're buying INTO the trend is
        // dangerous because by the time the other leg fills, the price has
        // moved further and the pair is a loser.
        //
        // Solution: lag the dangerous side by 1¢. We either:
        //   a) Don't get filled (good — avoided a losing pair), or
        //   b) Get filled at a 1¢ better price that compensates for the move.
        //
        // The "safe" side (selling into the trend) stays aggressive because
        // those fills are profitable — the trend is moving the price our way.
        match trend {
            TrendDirection::Up => {
                // YES price rising → our YES bid is dangerous (buying expensive YES)
                // Lag bid by 1¢, keep ask aggressive
                bid -= 1;
            }
            TrendDirection::Down => {
                // YES price falling → our YES ask is dangerous (selling cheap YES)
                // Which means our NO bid is dangerous (buying expensive NO)
                // Lag ask by 1¢ (raise YES ask = lower NO bid aggressiveness)
                ask += 1;
            }
            TrendDirection::Flat => {
                // No trend — quote normally
            }
        }

        // ── Layer 3: Asymmetric inventory skew ──
        let inv = risk.inventory;
        let skew_step = self.config.inventory_skew_threshold;

        if inv < 0 {
            // Short → want to buy → raise bid to attract fills
            bid += ((-inv) / skew_step).min(2) as i64;
        }
        if inv > 0 {
            // Long → want to sell → lower ask to attract fills
            ask -= (inv / skew_step).min(2) as i64;
        }

        // ── Clamp and safety ──
        bid = bid.clamp(1, 98);
        ask = ask.clamp(2, 99);

        // Ensure bid < ask
        if bid >= ask {
            if inv > 0 {
                ask = bid + 1;
            } else if inv < 0 {
                bid = ask - 1;
            } else {
                let mid = (bid + ask) / 2;
                bid = mid;
                ask = mid + 1;
            }
        }

        bid = bid.clamp(1, 98);
        ask = ask.clamp(2, 99);

        // Don't cross the true book
        if bid >= true_ask {
            bid = true_ask - 1;
        }
        if ask <= true_bid {
            ask = true_bid + 1;
        }

        if bid < 1 { bid = 0; }
        if ask > 99 { ask = 0; }

        // ── Pair profit guard ──
        if bid > 0 && ask > 0 {
            let pair_profit = ask - bid;
            if pair_profit < self.config.min_pair_profit_cents {
                tracing::debug!(
                    bid, ask, pair_profit, spread,
                    min = self.config.min_pair_profit_cents,
                    ?trend,
                    "Pair profit too low — suppressing quotes"
                );
                return DesiredQuote { bid_price: 0, ask_price: 0 };
            }
        }

        // ── Risk-based side suppression ──
        let final_bid = if risk.can_bid() { bid } else { 0 };
        let final_ask = if risk.can_ask() { ask } else { 0 };

        DesiredQuote {
            bid_price: final_bid,
            ask_price: final_ask,
        }
    }

    /// Reconcile desired quotes with current resting orders.
    pub fn reconcile(
        &self,
        desired: &DesiredQuote,
        tracker: &OrderTracker,
        ticker: &str,
    ) -> Vec<StrategyAction> {
        let mut actions = Vec::with_capacity(2);

        // ── Bid side ──
        let current_bid = tracker.resting_bid(ticker);
        match (desired.bid_price, current_bid) {
            (0, Some(order)) => {
                actions.push(StrategyAction::CancelOrder {
                    order_id: order.order_id.clone(),
                });
            }
            (price, None) if price > 0 => {
                actions.push(StrategyAction::PlaceBid {
                    price_cents: price,
                    count: 1,
                });
            }
            (price, Some(order)) if price > 0 && price != order.price_cents => {
                actions.push(StrategyAction::AmendBid {
                    order_id: order.order_id.clone(),
                    client_order_id: order.client_order_id.clone(),
                    new_price_cents: price,
                    count: 1,
                });
            }
            _ => {}
        }

        // ── Ask side ──
        let current_ask = tracker.resting_ask(ticker);
        match (desired.ask_price, current_ask) {
            (0, Some(order)) => {
                actions.push(StrategyAction::CancelOrder {
                    order_id: order.order_id.clone(),
                });
            }
            (ask_price, None) if ask_price > 0 => {
                actions.push(StrategyAction::PlaceAsk {
                    price_cents: ask_price,
                    count: 1,
                });
            }
            (ask_price, Some(order)) if ask_price > 0 => {
                let desired_no_price = 100 - ask_price;
                if desired_no_price != order.price_cents {
                    actions.push(StrategyAction::AmendAsk {
                        order_id: order.order_id.clone(),
                        client_order_id: order.client_order_id.clone(),
                        new_price_cents: desired_no_price,
                        count: 1,
                    });
                }
            }
            _ => {}
        }

        actions
    }
}