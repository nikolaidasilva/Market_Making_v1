use reqwest::Client;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

use crate::auth::Credentials;
use crate::config::Config;

// ═══════════════════════════════════════════════════════════════════════════
// Rate limiter: Semaphore-based, non-blocking.
// ═══════════════════════════════════════════════════════════════════════════

pub fn spawn_rate_limiter(rate_per_sec: u32) -> Arc<tokio::sync::Semaphore> {
    let sem = Arc::new(tokio::sync::Semaphore::new(rate_per_sec as usize));
    let sem_clone = sem.clone();

    let refill_interval_us = 1_000_000u64 / rate_per_sec as u64;

    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(Duration::from_micros(refill_interval_us));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if sem_clone.available_permits() < rate_per_sec as usize {
                sem_clone.add_permits(1);
            }
        }
    });

    sem
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    PendingNew,
    Resting,
    Executed,
    Canceled,
    PendingCancel,
    PendingAmend,
}

#[derive(Debug, Clone)]
pub struct TrackedOrder {
    pub order_id: String,
    pub client_order_id: String,
    pub ticker: String,
    pub side: String,
    pub action: String,
    pub price_cents: i64,
    pub count: i64,
    pub remaining: i64,
    pub status: OrderStatus,
    pub created_at: Instant,
}

pub struct OrderTracker {
    pub orders: HashMap<String, TrackedOrder>,
    pub client_id_map: HashMap<String, String>,
    pub cached_bid_id: Option<String>,
    pub cached_ask_id: Option<String>,
}

impl OrderTracker {
    pub fn new() -> Self {
        Self {
            orders: HashMap::new(),
            client_id_map: HashMap::new(),
            cached_bid_id: None,
            cached_ask_id: None,
        }
    }

    pub fn track(&mut self, order: TrackedOrder) {
        if order.side == "yes" && order.action == "buy"
            && matches!(order.status, OrderStatus::Resting | OrderStatus::PendingNew)
        {
            self.cached_bid_id = Some(order.order_id.clone());
        } else if order.side == "no" && order.action == "buy"
            && matches!(order.status, OrderStatus::Resting | OrderStatus::PendingNew)
        {
            self.cached_ask_id = Some(order.order_id.clone());
        }

        self.client_id_map
            .insert(order.client_order_id.clone(), order.order_id.clone());
        self.orders.insert(order.order_id.clone(), order);
    }

    pub fn get(&self, order_id: &str) -> Option<&TrackedOrder> {
        self.orders.get(order_id)
    }

    pub fn get_mut(&mut self, order_id: &str) -> Option<&mut TrackedOrder> {
        self.orders.get_mut(order_id)
    }

    pub fn update_status(&mut self, order_id: &str, status: OrderStatus) {
        if let Some(order) = self.orders.get_mut(order_id) {
            order.status = status;
        }
    }

    pub fn remove(&mut self, order_id: &str) {
        if let Some(order) = self.orders.remove(order_id) {
            self.client_id_map.remove(&order.client_order_id);
            if self.cached_bid_id.as_deref() == Some(order_id) {
                self.cached_bid_id = None;
            }
            if self.cached_ask_id.as_deref() == Some(order_id) {
                self.cached_ask_id = None;
            }
        }
    }

    /// O(1) cached lookup for resting bid.
    /// Excludes orders that are mid-amend (PendingAmend) so strategy
    /// doesn't try to amend them again or place a duplicate.
    pub fn resting_bid(&self, ticker: &str) -> Option<&TrackedOrder> {
        if let Some(ref id) = self.cached_bid_id {
            if let Some(o) = self.orders.get(id.as_str()) {
                if o.ticker == ticker
                    && o.side == "yes"
                    && o.action == "buy"
                    && matches!(o.status, OrderStatus::Resting | OrderStatus::PendingNew | OrderStatus::PendingAmend)
                {
                    return Some(o);
                }
            }
        }
        self.orders.values().find(|o| {
            o.ticker == ticker
                && o.side == "yes"
                && o.action == "buy"
                && matches!(o.status, OrderStatus::Resting | OrderStatus::PendingNew | OrderStatus::PendingAmend)
        })
    }

    /// O(1) cached lookup for resting ask.
    pub fn resting_ask(&self, ticker: &str) -> Option<&TrackedOrder> {
        if let Some(ref id) = self.cached_ask_id {
            if let Some(o) = self.orders.get(id.as_str()) {
                if o.ticker == ticker
                    && o.side == "no"
                    && o.action == "buy"
                    && matches!(o.status, OrderStatus::Resting | OrderStatus::PendingNew | OrderStatus::PendingAmend)
                {
                    return Some(o);
                }
            }
        }
        self.orders.values().find(|o| {
            o.ticker == ticker
                && o.side == "no"
                && o.action == "buy"
                && matches!(o.status, OrderStatus::Resting | OrderStatus::PendingNew | OrderStatus::PendingAmend)
        })
    }

    pub fn resting_order_ids(&self, ticker: &str) -> Vec<String> {
        self.orders
            .values()
            .filter(|o| {
                o.ticker == ticker
                    && matches!(o.status, OrderStatus::Resting | OrderStatus::PendingNew | OrderStatus::PendingAmend)
            })
            .map(|o| o.order_id.clone())
            .collect()
    }

    pub fn gc(&mut self, max_age: Duration) {
        let now = Instant::now();
        self.orders.retain(|_, o| {
            if matches!(o.status, OrderStatus::Executed | OrderStatus::Canceled) {
                now.duration_since(o.created_at) < max_age
            } else {
                true
            }
        });
    }
}

pub struct ExecutionClient {
    pub http: Client,
    base_url: String,
    creds: Arc<Credentials>,
    rate_sem: Arc<tokio::sync::Semaphore>,
}

impl ExecutionClient {
    pub fn new(config: &Config, creds: Arc<Credentials>, rate_sem: Arc<tokio::sync::Semaphore>) -> Self {
        let http = Client::builder()
            .timeout(config.rest_timeout)
            .tcp_nodelay(true)
            .pool_max_idle_per_host(32)
            .build()
            .expect("Failed to build HTTP client");

        Self {
            http,
            base_url: config.rest_base_url.clone(),
            creds,
            rate_sem,
        }
    }

    #[inline]
    async fn acquire_rate_limit(&self) -> anyhow::Result<()> {
        // Increased from 200ms to 500ms — 200ms was too tight and caused
        // cascading failures when bid+ask amends competed for permits.
        match tokio::time::timeout(
            Duration::from_millis(500),
            self.rate_sem.acquire(),
        )
        .await
        {
            Ok(Ok(permit)) => {
                permit.forget();
                Ok(())
            }
            Ok(Err(_)) => anyhow::bail!("Rate limiter closed"),
            Err(_) => anyhow::bail!("Rate limit exhausted (500ms timeout)"),
        }
    }

    fn auth_headers(&self, method: &str, path: &str) -> [(&'static str, String); 3] {
        let (key, sig, ts) = self.creds.sign(method, path);
        [
            ("KALSHI-ACCESS-KEY", key),
            ("KALSHI-ACCESS-SIGNATURE", sig),
            ("KALSHI-ACCESS-TIMESTAMP", ts),
        ]
    }

    pub async fn place_order(
        &self,
        ticker: &str,
        side: &str,
        action: &str,
        price_cents: i64,
        count: i64,
    ) -> anyhow::Result<PlaceOrderResponse> {
        self.acquire_rate_limit().await?;

        let client_order_id = Uuid::new_v4().to_string();
        let path = "/trade-api/v2/portfolio/orders";
        let url = format!("{}/portfolio/orders", self.base_url);
        let headers = self.auth_headers("POST", path);

        let price_dollars = format!("{:.4}", price_cents as f64 / 100.0);
        let count_fp = format!("{}.00", count);

        let mut body = serde_json::json!({
            "ticker": ticker,
            "side": side,
            "action": action,
            "client_order_id": client_order_id,
            "count_fp": count_fp,
            "type": "limit",
            "time_in_force": "good_till_canceled",
            "post_only": true,
            "cancel_order_on_pause": true,
        });

        if side == "yes" {
            body["yes_price_dollars"] = serde_json::json!(price_dollars);
        } else {
            body["no_price_dollars"] = serde_json::json!(price_dollars);
        }

        let mut req = self.http.post(&url).json(&body);
        for (k, v) in &headers {
            req = req.header(*k, v.as_str());
        }

        let resp = req.send().await?;
        let status = resp.status();

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            tracing::warn!("429 rate limited");
            anyhow::bail!("Rate limited (429)");
        }
        if status == reqwest::StatusCode::CONFLICT {
            tracing::warn!("409 duplicate client_order_id");
            anyhow::bail!("Duplicate order (409)");
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            if body.contains("post only cross") {
                tracing::debug!("Post-only cross — book moved, skipping");
                anyhow::bail!("Post-only cross");
            }
            tracing::error!(%status, %body, "Order placement failed");
            anyhow::bail!("Order failed: {} {}", status, body);
        }

        let resp_body: serde_json::Value = resp.json().await?;
        let order = &resp_body["order"];

        Ok(PlaceOrderResponse {
            order_id: order["order_id"].as_str().unwrap_or("").to_string(),
            client_order_id,
            status: order["status"].as_str().unwrap_or("").to_string(),
        })
    }

    pub async fn cancel_order(&self, order_id: &str) -> anyhow::Result<()> {
        self.acquire_rate_limit().await?;

        let path = format!("/trade-api/v2/portfolio/orders/{}", order_id);
        let url = format!("{}/portfolio/orders/{}", self.base_url, order_id);
        let headers = self.auth_headers("DELETE", &path);

        let mut req = self.http.delete(&url);
        for (k, v) in &headers {
            req = req.header(*k, v.as_str());
        }

        let resp = req.send().await?;
        let status = resp.status();

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            anyhow::bail!("Rate limited (429)");
        }
        if status == reqwest::StatusCode::NOT_FOUND {
            tracing::debug!(order_id, "Cancel 404 — order already gone");
            return Ok(());
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            tracing::error!(%status, %body, "Cancel failed");
            anyhow::bail!("Cancel failed: {} {}", status, body);
        }

        Ok(())
    }

    pub async fn amend_order(
        &self,
        order_id: &str,
        client_order_id: &str,
        ticker: &str,
        side: &str,
        action: &str,
        new_price_cents: i64,
        count: i64,
    ) -> anyhow::Result<AmendOrderResponse> {
        self.acquire_rate_limit().await?;

        let path = format!("/trade-api/v2/portfolio/orders/{}/amend", order_id);
        let url = format!("{}/portfolio/orders/{}/amend", self.base_url, order_id);
        let headers = self.auth_headers("POST", &path);

        let new_client_id = Uuid::new_v4().to_string();
        let price_dollars = format!("{:.4}", new_price_cents as f64 / 100.0);
        let count_fp = format!("{}.00", count);

        let mut body = serde_json::json!({
            "ticker": ticker,
            "side": side,
            "action": action,
            "count_fp": count_fp,
            "client_order_id": client_order_id,
            "updated_client_order_id": new_client_id,
        });

        if side == "yes" {
            body["yes_price_dollars"] = serde_json::json!(price_dollars);
        } else {
            body["no_price_dollars"] = serde_json::json!(price_dollars);
        }

        let mut req = self.http.post(&url).json(&body);
        for (k, v) in &headers {
            req = req.header(*k, v.as_str());
        }

        let resp = req.send().await?;
        let status = resp.status();

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            anyhow::bail!("Rate limited (429)");
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Amend failed: {} {}", status, body);
        }

        let resp_body: serde_json::Value = resp.json().await?;
        let order = &resp_body["order"];

        Ok(AmendOrderResponse {
            order_id: order["order_id"].as_str().unwrap_or("").to_string(),
            new_client_order_id: new_client_id,
            status: order["status"].as_str().unwrap_or("").to_string(),
        })
    }

    pub async fn batch_cancel(&self, order_ids: &[String]) -> anyhow::Result<()> {
        for chunk in order_ids.chunks(20) {
            for _ in chunk {
                self.acquire_rate_limit().await?;
            }

            let path = "/trade-api/v2/portfolio/orders/batched";
            let url = format!("{}/portfolio/orders/batched", self.base_url);
            let headers = self.auth_headers("DELETE", path);

            let orders: Vec<serde_json::Value> = chunk
                .iter()
                .map(|id| serde_json::json!({ "order_id": id }))
                .collect();
            let body = serde_json::json!({ "orders": orders });

            let mut req = self.http.delete(&url).json(&body);
            for (k, v) in &headers {
                req = req.header(*k, v.as_str());
            }

            let resp = req.send().await?;
            if !resp.status().is_success() {
                let body = resp.text().await.unwrap_or_default();
                tracing::error!(%body, "Batch cancel failed");
            }
        }
        Ok(())
    }

    pub async fn get_balance(&self) -> anyhow::Result<i64> {
        let path = "/trade-api/v2/portfolio/balance";
        let url = format!("{}/portfolio/balance", self.base_url);
        let headers = self.auth_headers("GET", path);

        let mut req = self.http.get(&url);
        for (k, v) in &headers {
            req = req.header(*k, v.as_str());
        }

        let resp = req.send().await?;
        let body: serde_json::Value = resp.json().await?;
        Ok(body["balance"].as_i64().unwrap_or(0))
    }

    pub async fn get_position(&self, ticker: &str) -> anyhow::Result<i64> {
        let path = "/trade-api/v2/portfolio/positions";
        let url = format!("{}/portfolio/positions?ticker={}", self.base_url, ticker);
        let headers = self.auth_headers("GET", path);

        let mut req = self.http.get(&url);
        for (k, v) in &headers {
            req = req.header(*k, v.as_str());
        }

        let resp = req.send().await?;
        let body: serde_json::Value = resp.json().await?;

        if let Some(positions) = body["market_positions"].as_array() {
            for pos in positions {
                if pos["ticker"].as_str() == Some(ticker) {
                    return Ok(pos["position"].as_i64().unwrap_or(0));
                }
            }
        }
        Ok(0)
    }
}

#[derive(Debug)]
pub struct PlaceOrderResponse {
    pub order_id: String,
    pub client_order_id: String,
    pub status: String,
}

#[derive(Debug)]
pub struct AmendOrderResponse {
    pub order_id: String,
    pub new_client_order_id: String,
    pub status: String,
}