#![allow(dead_code)]
#![allow(unused_variables)]

mod auth;
mod config;
mod execution;
mod market_data;
mod market_finder;
mod risk;
mod strategy;
mod ws;

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use auth::Credentials;
use config::Config;
use execution::{ExecutionClient, OrderStatus, OrderTracker, TrackedOrder};
use market_data::{OrderBook, TrendTracker};
use risk::{RiskManager, RiskState};
use strategy::{Strategy, StrategyAction};
use ws::{WsManager, WsMessage};

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "kalshi_mm=info".into()),
        )
        .json()
        .init();

    let config = load_config_from_env();

    let creds = Arc::new(Credentials::load(
        config.api_key_id.clone(),
        &config.private_key_pem,
    ));

    tracing::info!(
        series = %config.series_ticker,
        max_inventory = config.max_inventory,
        min_pair_profit = config.min_pair_profit_cents,
        quote_refresh_ms = config.quote_refresh_interval.as_millis() as u64,
        skew_threshold = config.inventory_skew_threshold,
        trend_alpha = config.trend_alpha,
        trend_threshold = config.trend_threshold,
        "Starting Kalshi passive market maker"
    );

    let mut daily_pnl_cents: i64 = 0;

    loop {
        let http = reqwest::Client::builder()
            .timeout(config.rest_timeout)
            .tcp_nodelay(true)
            .pool_max_idle_per_host(32)
            .build()?;

        let market = match market_finder::find_current_market(&creds, &http, &config.series_ticker)
            .await
        {
            Ok(m) => m,
            Err(e) => {
                tracing::error!(error = %e, "Failed to find market — retrying in 10s");
                tokio::time::sleep(Duration::from_secs(10)).await;
                continue;
            }
        };

        let ticker = market.ticker.clone();
        let close_time = market
            .close_time
            .as_ref()
            .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
            .map(|dt: chrono::DateTime<chrono::FixedOffset>| dt.with_timezone(&chrono::Utc));

        tracing::info!(
            ticker = %ticker,
            title = %market.title,
            close_time = ?market.close_time,
            carry_pnl = daily_pnl_cents,
            "Trading market"
        );

        let result =
            run_market_session(&config, &creds, &ticker, close_time, daily_pnl_cents).await;

        match result {
            MarketSessionExit::Rotate { session_pnl } => {
                daily_pnl_cents = session_pnl;
                tracing::info!(ticker = %ticker, daily_pnl = daily_pnl_cents, "Market expiring — rotating");
            }
            MarketSessionExit::NoMarkets => {
                tracing::warn!("No suitable markets — waiting 30s");
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            MarketSessionExit::Fatal(e) => {
                tracing::error!(error = %e, "Fatal error — exiting");
                return Err(e);
            }
        }
    }
}

enum MarketSessionExit {
    Rotate { session_pnl: i64 },
    NoMarkets,
    Fatal(anyhow::Error),
}

async fn run_market_session(
    config: &Config,
    creds: &Arc<Credentials>,
    ticker: &str,
    close_time: Option<chrono::DateTime<chrono::Utc>>,
    carry_pnl: i64,
) -> MarketSessionExit {
    let mut book = OrderBook::new();
    let mut trend = TrendTracker::new(config.trend_alpha, config.trend_threshold);
    let mut risk = RiskManager::new(config);
    risk.carry_daily_pnl(carry_pnl);
    let strategy = Strategy::new(config.clone());
    let mut tracker = OrderTracker::new();

    let rate_sem = execution::spawn_rate_limiter(config.rate_limit_writes_per_sec);
    let exec = Arc::new(ExecutionClient::new(config, creds.clone(), rate_sem));

    match exec.get_position(ticker).await {
        Ok(pos) => {
            risk.inventory = pos;
            tracing::info!(position = pos, "Initial position loaded");
        }
        Err(e) => {
            tracing::error!(error = %e, "Failed to load initial position — starting at 0");
        }
    }

    let (ws_tx, mut ws_rx) = mpsc::unbounded_channel::<WsMessage>();

    let ws_creds = creds.clone();
    let ws_url = config.ws_url.clone();
    let ws_ticker = ticker.to_string();
    tokio::spawn(async move {
        let ws = WsManager::new(ws_url, ws_creds, ws_ticker);
        if let Err(e) = ws.run(ws_tx).await {
            tracing::error!(error = %e, "WS manager fatal error");
        }
    });

    let mut ws_connected_signaled = false;
    let mut last_gc = Instant::now();
    let mut last_trend_log = Instant::now();

    let mut last_desired = strategy::DesiredQuote {
        bid_price: 0,
        ask_price: 0,
    };

    tracing::info!(ticker, "Entering event loop");

    loop {
        // ── Market close check ──
        if let Some(close) = close_time {
            let now = chrono::Utc::now();
            let secs_until_close = (close - now).num_seconds();
            if secs_until_close <= config.market_rotate_before_close_secs {
                tracing::info!(secs_until_close, "Market closing soon — rotating");
                cancel_all_orders(&exec, &mut tracker, ticker).await;
                return MarketSessionExit::Rotate {
                    session_pnl: risk.daily_pnl_cents,
                };
            }
        }

        // ── Drain all pending WS messages ──
        let mut messages = Vec::new();
        match tokio::time::timeout(config.quote_refresh_interval, ws_rx.recv()).await {
            Ok(Some(msg)) => {
                messages.push(msg);
                while let Ok(msg) = ws_rx.try_recv() {
                    messages.push(msg);
                }
            }
            Ok(None) => {
                tracing::error!("WS channel closed — halting");
                risk.halt("ws_channel_closed");
                cancel_all_orders(&exec, &mut tracker, ticker).await;
                return MarketSessionExit::Rotate {
                    session_pnl: risk.daily_pnl_cents,
                };
            }
            Err(_) => {
                if last_gc.elapsed() > Duration::from_secs(60) {
                    tracker.gc(Duration::from_secs(300));
                    last_gc = Instant::now();
                }
                continue;
            }
        }

        // ── Process all WS messages ──
        let mut book_changed = false;
        let mut got_fill = false;

        for ws_msg in messages {
            if !ws_connected_signaled {
                risk.set_ws_connected(true);
                ws_connected_signaled = true;
            }

            match ws_msg {
                WsMessage::OrderbookSnapshot {
                    market_ticker,
                    yes_levels,
                    no_levels,
                    seq,
                } => {
                    if market_ticker == ticker {
                        book.apply_snapshot(&yes_levels, &no_levels, seq);
                        risk.book_updated();
                        trend.reset(); // Reset trend on snapshot — stale state
                        if let Some(mid) = book.midprice() {
                            trend.update(mid);
                        }
                        book_changed = true;
                    }
                }

                WsMessage::OrderbookDelta {
                    market_ticker,
                    side,
                    price,
                    delta,
                    seq,
                } => {
                    if market_ticker == ticker {
                        if !book.apply_delta(side, price, delta, seq) {
                            tracing::error!("Sequence gap — rotating to resubscribe");
                            risk.halt("sequence_gap");
                            cancel_all_orders(&exec, &mut tracker, ticker).await;
                            return MarketSessionExit::Rotate {
                                session_pnl: risk.daily_pnl_cents,
                            };
                        }
                        risk.book_updated();

                        // Update trend tracker on every book change
                        if let Some(mid) = book.midprice() {
                            trend.update(mid);
                        }

                        book_changed = true;
                    }
                }

                WsMessage::Fill {
                    order_id,
                    side,
                    yes_price,
                    count,
                    action,
                    is_taker,
                    post_position,
                    ..
                } => {
                    risk.record_fill(&side, &action, yes_price, count, post_position);

                    if let Some(order) = tracker.get_mut(&order_id) {
                        order.remaining -= count;
                        if order.remaining <= 0 {
                            order.status = OrderStatus::Executed;
                        }
                    }

                    got_fill = true;

                    tracing::info!(
                        order_id, side, action, yes_price, count, is_taker,
                        inventory = risk.inventory,
                        daily_pnl = risk.daily_pnl_cents,
                        pairs = risk.pairs_completed,
                        trend_ewma = format!("{:.3}", trend.ewma_value()),
                        trend_dir = ?trend.direction(),
                        "FILL"
                    );
                }

                WsMessage::UserOrder {
                    order_id, status, ..
                } => {
                    let new_status = match status.as_str() {
                        "resting" => OrderStatus::Resting,
                        "canceled" => OrderStatus::Canceled,
                        "executed" => OrderStatus::Executed,
                        _ => continue,
                    };
                    tracker.update_status(&order_id, new_status);
                    if matches!(new_status, OrderStatus::Canceled | OrderStatus::Executed) {
                        tracker.remove(&order_id);
                    }
                }

                WsMessage::Error { code, message } => {
                    tracing::error!(code, message, "WS error");
                    if message.starts_with("WS disconnect") {
                        ws_connected_signaled = false;
                        risk.set_ws_connected(false);
                        book.initialized = false;
                        trend.reset();
                        cancel_all_orders(&exec, &mut tracker, ticker).await;
                    }
                }
                _ => {}
            }
        }

        // ── Periodic trend logging (every 5s, not every tick) ──
        if last_trend_log.elapsed() > Duration::from_secs(5) && book.initialized {
            tracing::info!(
                trend_ewma = format!("{:.3}", trend.ewma_value()),
                trend_dir = ?trend.direction(),
                yes_bid = book.best_yes_bid(),
                yes_ask = book.best_yes_ask(),
                spread = book.yes_spread(),
                inventory = risk.inventory,
                daily_pnl = risk.daily_pnl_cents,
                pairs = risk.pairs_completed,
                "Status"
            );
            last_trend_log = Instant::now();
        }

        // ── Skip if book not initialized ──
        if !book.initialized {
            continue;
        }

        // ── Risk check ──
        let risk_state = risk.tick();

        if risk_state == RiskState::Halted {
            if !tracker.resting_order_ids(ticker).is_empty() {
                cancel_all_orders(&exec, &mut tracker, ticker).await;
            }
            last_desired = strategy::DesiredQuote { bid_price: 0, ask_price: 0 };
            continue;
        }

        // ── Only recompute if something changed ──
        if !book_changed && !got_fill {
            let desired = strategy.compute_desired_quotes(&book, &tracker, ticker, &risk, trend.direction());
            if desired == last_desired {
                continue;
            }
        }

        // ── Strategy: compute desired quotes and reconcile ──
        let desired = strategy.compute_desired_quotes(&book, &tracker, ticker, &risk, trend.direction());
        let actions = strategy.reconcile(&desired, &tracker, ticker);

        last_desired = desired;

        if actions.is_empty() {
            if last_gc.elapsed() > Duration::from_secs(60) {
                tracker.gc(Duration::from_secs(300));
                last_gc = Instant::now();
            }
            continue;
        }

        // ── Execute ──
        execute_actions(&exec, &mut tracker, ticker, actions).await;

        if last_gc.elapsed() > Duration::from_secs(60) {
            tracker.gc(Duration::from_secs(300));
            last_gc = Instant::now();
        }
    }
}

/// Execute actions with maximum concurrency.
async fn execute_actions(
    exec: &Arc<ExecutionClient>,
    tracker: &mut OrderTracker,
    ticker: &str,
    actions: Vec<StrategyAction>,
) {
    let (cancels, rest): (Vec<_>, Vec<_>) = actions.into_iter().partition(|a| {
        matches!(
            a,
            StrategyAction::CancelOrder { .. } | StrategyAction::CancelAll
        )
    });

    // Phase 1: cancels (sequential)
    for action in cancels {
        execute_single(exec, tracker, ticker, action).await;
    }

    if rest.is_empty() {
        return;
    }

    // Phase 2: Mark amends as PendingAmend
    for action in &rest {
        match action {
            StrategyAction::AmendBid { order_id, .. }
            | StrategyAction::AmendAsk { order_id, .. } => {
                tracker.update_status(order_id, OrderStatus::PendingAmend);
            }
            _ => {}
        }
    }

    if rest.len() == 1 {
        let action = rest.into_iter().next().unwrap();
        execute_single(exec, tracker, ticker, action).await;
    } else {
        let mut handles = Vec::with_capacity(rest.len());
        for action in rest {
            let exec_clone = Arc::clone(exec);
            let ticker_owned = ticker.to_string();
            handles.push(tokio::spawn(async move {
                execute_fire(exec_clone, &ticker_owned, action).await
            }));
        }
        for handle in handles {
            match handle.await {
                Ok(Some((old_id, tracked))) => {
                    if let Some(ref id) = old_id {
                        tracker.remove(id);
                    }
                    tracker.track(tracked);
                }
                Ok(None) => {}
                Err(e) => tracing::error!(error = %e, "Spawned action panicked"),
            }
        }
    }
}

async fn execute_single(
    exec: &Arc<ExecutionClient>,
    tracker: &mut OrderTracker,
    ticker: &str,
    action: StrategyAction,
) {
    match action {
        StrategyAction::PlaceBid { price_cents, count } => {
            match exec.place_order(ticker, "yes", "buy", price_cents, count).await {
                Ok(resp) => {
                    tracker.track(TrackedOrder {
                        order_id: resp.order_id.clone(),
                        client_order_id: resp.client_order_id,
                        ticker: ticker.to_string(),
                        side: "yes".into(),
                        action: "buy".into(),
                        price_cents,
                        count,
                        remaining: count,
                        status: OrderStatus::PendingNew,
                        created_at: Instant::now(),
                    });
                    tracing::info!(order_id = %resp.order_id, price = price_cents, "BID placed");
                }
                Err(e) => tracing::error!(error = %e, price = price_cents, "Bid failed"),
            }
        }

        StrategyAction::PlaceAsk { price_cents, count } => {
            let no_price = 100 - price_cents;
            match exec.place_order(ticker, "no", "buy", no_price, count).await {
                Ok(resp) => {
                    tracker.track(TrackedOrder {
                        order_id: resp.order_id.clone(),
                        client_order_id: resp.client_order_id,
                        ticker: ticker.to_string(),
                        side: "no".into(),
                        action: "buy".into(),
                        price_cents: no_price,
                        count,
                        remaining: count,
                        status: OrderStatus::PendingNew,
                        created_at: Instant::now(),
                    });
                    tracing::info!(order_id = %resp.order_id, yes_ask = price_cents, no_buy = no_price, "ASK placed");
                }
                Err(e) => tracing::error!(error = %e, price = price_cents, "Ask failed"),
            }
        }

        StrategyAction::CancelOrder { order_id } => {
            if let Err(e) = exec.cancel_order(&order_id).await {
                tracing::warn!(error = %e, order_id, "Cancel failed");
            } else {
                tracker.update_status(&order_id, OrderStatus::PendingCancel);
            }
        }

        StrategyAction::AmendBid {
            order_id, client_order_id, new_price_cents, count,
        } => {
            match exec.amend_order(&order_id, &client_order_id, ticker, "yes", "buy", new_price_cents, count).await {
                Ok(resp) => {
                    tracker.remove(&order_id);
                    tracker.track(TrackedOrder {
                        order_id: resp.order_id.clone(),
                        client_order_id: resp.new_client_order_id,
                        ticker: ticker.to_string(),
                        side: "yes".into(),
                        action: "buy".into(),
                        price_cents: new_price_cents,
                        count,
                        remaining: count,
                        status: OrderStatus::PendingNew,
                        created_at: Instant::now(),
                    });
                }
                Err(e) => {
                    tracker.update_status(&order_id, OrderStatus::Resting);
                    tracing::warn!(error = %e, "Amend bid failed — reverted to Resting");
                }
            }
        }

        StrategyAction::AmendAsk {
            order_id, client_order_id, new_price_cents, count,
        } => {
            match exec.amend_order(&order_id, &client_order_id, ticker, "no", "buy", new_price_cents, count).await {
                Ok(resp) => {
                    tracker.remove(&order_id);
                    tracker.track(TrackedOrder {
                        order_id: resp.order_id.clone(),
                        client_order_id: resp.new_client_order_id,
                        ticker: ticker.to_string(),
                        side: "no".into(),
                        action: "buy".into(),
                        price_cents: new_price_cents,
                        count,
                        remaining: count,
                        status: OrderStatus::PendingNew,
                        created_at: Instant::now(),
                    });
                }
                Err(e) => {
                    tracker.update_status(&order_id, OrderStatus::Resting);
                    tracing::warn!(error = %e, "Amend ask failed — reverted to Resting");
                }
            }
        }

        StrategyAction::CancelAll => {
            let ids = tracker.resting_order_ids(ticker);
            if !ids.is_empty() {
                if let Err(e) = exec.batch_cancel(&ids).await {
                    tracing::error!(error = %e, "Batch cancel failed");
                    for id in &ids {
                        let _ = exec.cancel_order(id).await;
                    }
                }
                for id in &ids {
                    tracker.remove(id);
                }
            }
        }
    }
}

async fn execute_fire(
    exec: Arc<ExecutionClient>,
    ticker: &str,
    action: StrategyAction,
) -> Option<(Option<String>, TrackedOrder)> {
    match action {
        StrategyAction::PlaceBid { price_cents, count } => {
            match exec.place_order(ticker, "yes", "buy", price_cents, count).await {
                Ok(resp) => {
                    tracing::info!(order_id = %resp.order_id, price = price_cents, "BID placed ∥");
                    Some((None, TrackedOrder {
                        order_id: resp.order_id,
                        client_order_id: resp.client_order_id,
                        ticker: ticker.to_string(),
                        side: "yes".into(),
                        action: "buy".into(),
                        price_cents,
                        count,
                        remaining: count,
                        status: OrderStatus::PendingNew,
                        created_at: Instant::now(),
                    }))
                }
                Err(e) => { tracing::error!(error = %e, "Bid failed ∥"); None }
            }
        }

        StrategyAction::PlaceAsk { price_cents, count } => {
            let no_price = 100 - price_cents;
            match exec.place_order(ticker, "no", "buy", no_price, count).await {
                Ok(resp) => {
                    tracing::info!(order_id = %resp.order_id, yes_ask = price_cents, "ASK placed ∥");
                    Some((None, TrackedOrder {
                        order_id: resp.order_id,
                        client_order_id: resp.client_order_id,
                        ticker: ticker.to_string(),
                        side: "no".into(),
                        action: "buy".into(),
                        price_cents: no_price,
                        count,
                        remaining: count,
                        status: OrderStatus::PendingNew,
                        created_at: Instant::now(),
                    }))
                }
                Err(e) => { tracing::error!(error = %e, "Ask failed ∥"); None }
            }
        }

        StrategyAction::AmendBid {
            order_id, client_order_id, new_price_cents, count,
        } => {
            match exec.amend_order(&order_id, &client_order_id, ticker, "yes", "buy", new_price_cents, count).await {
                Ok(resp) => Some((Some(order_id), TrackedOrder {
                    order_id: resp.order_id,
                    client_order_id: resp.new_client_order_id,
                    ticker: ticker.to_string(),
                    side: "yes".into(),
                    action: "buy".into(),
                    price_cents: new_price_cents,
                    count,
                    remaining: count,
                    status: OrderStatus::PendingNew,
                    created_at: Instant::now(),
                })),
                Err(e) => {
                    tracing::warn!(error = %e, "Amend bid failed ∥");
                    None
                }
            }
        }

        StrategyAction::AmendAsk {
            order_id, client_order_id, new_price_cents, count,
        } => {
            match exec.amend_order(&order_id, &client_order_id, ticker, "no", "buy", new_price_cents, count).await {
                Ok(resp) => Some((Some(order_id), TrackedOrder {
                    order_id: resp.order_id,
                    client_order_id: resp.new_client_order_id,
                    ticker: ticker.to_string(),
                    side: "no".into(),
                    action: "buy".into(),
                    price_cents: new_price_cents,
                    count,
                    remaining: count,
                    status: OrderStatus::PendingNew,
                    created_at: Instant::now(),
                })),
                Err(e) => {
                    tracing::warn!(error = %e, "Amend ask failed ∥");
                    None
                }
            }
        }

        StrategyAction::CancelOrder { order_id } => {
            let _ = exec.cancel_order(&order_id).await;
            None
        }
        StrategyAction::CancelAll => None,
    }
}

async fn cancel_all_orders(
    exec: &Arc<ExecutionClient>,
    tracker: &mut OrderTracker,
    ticker: &str,
) {
    let ids = tracker.resting_order_ids(ticker);
    if ids.is_empty() {
        return;
    }
    tracing::info!(count = ids.len(), "Canceling all resting orders");

    if let Err(e) = exec.batch_cancel(&ids).await {
        tracing::error!(error = %e, "Batch cancel failed — individual cancels");
        let mut handles = Vec::new();
        for id in &ids {
            let exec_clone = Arc::clone(exec);
            let id_owned = id.clone();
            handles.push(tokio::spawn(async move {
                let _ = exec_clone.cancel_order(&id_owned).await;
            }));
        }
        for h in handles {
            let _ = h.await;
        }
    }

    for id in &ids {
        tracker.remove(id);
    }
}

fn load_config_from_env() -> Config {
    if let Ok(contents) = std::fs::read_to_string(".env") {
        for line in contents.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((key, val)) = line.split_once('=') {
                let key = key.trim();
                let val = val.trim().trim_matches('"');
                if std::env::var(key).is_err() {
                    unsafe { std::env::set_var(key, val) };
                }
            }
        }
    }

    let mut config = Config::from_env();

    config.api_key_id =
        std::env::var("KALSHI_KEY_ID").expect("KALSHI_KEY_ID required in .env");

    let key_path =
        std::env::var("KALSHI_PRIVATE_KEY_PATH").expect("KALSHI_PRIVATE_KEY_PATH required in .env");

    config.private_key_pem = std::fs::read_to_string(&key_path)
        .unwrap_or_else(|e| panic!("Failed to read private key from {}: {}", key_path, e));

    tracing::info!(
        key_id = %config.api_key_id,
        key_path = %key_path,
        series = %config.series_ticker,
        "Config loaded"
    );

    config
}