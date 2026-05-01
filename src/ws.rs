use std::sync::Arc;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite;

use crate::auth::Credentials;
use crate::market_data::Side;

#[derive(Debug, Clone)]
pub enum WsMessage {
    OrderbookSnapshot {
        market_ticker: String,
        yes_levels: Vec<(i64, i64)>,
        no_levels: Vec<(i64, i64)>,
        seq: u64,
    },
    OrderbookDelta {
        market_ticker: String,
        side: Side,
        price: i64,
        delta: i64,
        seq: u64,
    },
    Fill {
        trade_id: String,
        order_id: String,
        market_ticker: String,
        side: String,
        yes_price: i64,
        count: i64,
        action: String,
        is_taker: bool,
        post_position: i64,
    },
    UserOrder {
        order_id: String,
        ticker: String,
        status: String,
        side: String,
        remaining_count_fp: String,
        fill_count_fp: String,
    },
    Ticker {
        market_ticker: String,
        yes_bid: i64,
        yes_ask: i64,
        price: i64,
        volume: i64,
    },
    Trade {
        market_ticker: String,
        yes_price: i64,
        count: i64,
        taker_side: String,
    },
    Subscribed {
        channel: String,
        sid: u64,
    },
    Error {
        code: u64,
        message: String,
    },
    Unknown(String),
}

fn get_cents(msg: &serde_json::Value, dollars_key: &str, cents_key: &str) -> i64 {
    msg.get(dollars_key)
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .map(|f| (f * 100.0).round() as i64)
        .unwrap_or_else(|| msg[cents_key].as_i64().unwrap_or(0))
}

fn get_count(msg: &serde_json::Value, fp_key: &str, int_key: &str) -> i64 {
    msg.get(fp_key)
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .map(|f| f.round() as i64)
        .unwrap_or_else(|| msg[int_key].as_i64().unwrap_or(0))
}

fn parse_snapshot_levels(msg: &serde_json::Value, dollars_fp_key: &str, legacy_key: &str) -> Vec<(i64, i64)> {
    if let Some(arr) = msg.get(dollars_fp_key).and_then(|v| v.as_array()) {
        arr.iter().filter_map(|level| {
            let pair = level.as_array()?;
            let price_cents = (pair.first()?.as_str()?.parse::<f64>().ok()? * 100.0).round() as i64;
            let qty = pair.get(1)?.as_str()?.parse::<f64>().ok()?.round() as i64;
            Some((price_cents, qty))
        }).collect()
    } else {
        match msg.get(legacy_key).and_then(|v| v.as_array()) {
            Some(arr) => arr
                .iter()
                .filter_map(|level| {
                    let pair = level.as_array()?;
                    Some((pair.first()?.as_i64()?, pair.get(1)?.as_i64()?))
                })
                .collect(),
            None => Vec::new(),
        }
    }
}

pub fn parse_ws_message(text: &str) -> WsMessage {
    let v: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return WsMessage::Unknown(text.to_string()),
    };

    let msg_type = v.get("type").and_then(|t| t.as_str()).unwrap_or("");

    match msg_type {
        "orderbook_snapshot" => {
            let msg = &v["msg"];
            WsMessage::OrderbookSnapshot {
                market_ticker: msg["market_ticker"].as_str().unwrap_or("").to_string(),
                yes_levels: parse_snapshot_levels(msg, "yes_dollars_fp", "yes"),
                no_levels: parse_snapshot_levels(msg, "no_dollars_fp", "no"),
                seq: v["seq"].as_u64().unwrap_or(0),
            }
        }
        "orderbook_delta" => {
            let msg = &v["msg"];
            WsMessage::OrderbookDelta {
                market_ticker: msg["market_ticker"].as_str().unwrap_or("").to_string(),
                side: match msg["side"].as_str().unwrap_or("") {
                    "yes" => Side::Yes,
                    _ => Side::No,
                },
                price: get_cents(msg, "price_dollars", "price"),
                delta: get_count(msg, "delta_fp", "delta"),
                seq: v["seq"].as_u64().unwrap_or(0),
            }
        }
        "fill" => {
            let msg = &v["msg"];
            WsMessage::Fill {
                trade_id: msg["trade_id"].as_str().unwrap_or("").to_string(),
                order_id: msg["order_id"].as_str().unwrap_or("").to_string(),
                market_ticker: msg["market_ticker"].as_str().unwrap_or("").to_string(),
                side: msg["side"].as_str().unwrap_or("").to_string(),
                yes_price: get_cents(msg, "yes_price_dollars", "yes_price"),
                count: get_count(msg, "count_fp", "count"),
                action: msg["action"].as_str().unwrap_or("").to_string(),
                is_taker: msg["is_taker"].as_bool().unwrap_or(false),
                post_position: msg["post_position"].as_i64().unwrap_or(0),
            }
        }
        "user_order" => {
            let msg = &v["msg"];
            WsMessage::UserOrder {
                order_id: msg["order_id"].as_str().unwrap_or("").to_string(),
                ticker: msg["ticker"].as_str().unwrap_or("").to_string(),
                status: msg["status"].as_str().unwrap_or("").to_string(),
                side: msg["side"].as_str().unwrap_or("").to_string(),
                remaining_count_fp: msg["remaining_count_fp"]
                    .as_str()
                    .unwrap_or("0.00")
                    .to_string(),
                fill_count_fp: msg["fill_count_fp"]
                    .as_str()
                    .unwrap_or("0.00")
                    .to_string(),
            }
        }
        "ticker" => {
            let msg = &v["msg"];
            WsMessage::Ticker {
                market_ticker: msg["market_ticker"].as_str().unwrap_or("").to_string(),
                yes_bid: get_cents(msg, "yes_bid_dollars", "yes_bid"),
                yes_ask: get_cents(msg, "yes_ask_dollars", "yes_ask"),
                price: get_cents(msg, "price_dollars", "price"),
                volume: get_count(msg, "volume_fp", "volume"),
            }
        }
        "trade" => {
            let msg = &v["msg"];
            WsMessage::Trade {
                market_ticker: msg["market_ticker"].as_str().unwrap_or("").to_string(),
                yes_price: get_cents(msg, "yes_price_dollars", "yes_price"),
                count: get_count(msg, "count_fp", "count"),
                taker_side: msg["taker_side"].as_str().unwrap_or("").to_string(),
            }
        }
        "subscribed" => {
            let msg = &v["msg"];
            WsMessage::Subscribed {
                channel: msg["channel"].as_str().unwrap_or("").to_string(),
                sid: msg["sid"].as_u64().unwrap_or(0),
            }
        }
        "error" => WsMessage::Error {
            code: v["code"].as_u64().unwrap_or(0),
            message: v["msg"].as_str().unwrap_or("").to_string(),
        },
        _ => WsMessage::Unknown(text.to_string()),
    }
}

pub fn subscribe_cmd(id: u64, channels: &[&str], market_ticker: &str) -> String {
    serde_json::json!({
        "id": id,
        "cmd": "subscribe",
        "params": {
            "channels": channels,
            "market_ticker": market_ticker
        }
    })
    .to_string()
}

pub fn unsubscribe_cmd(id: u64, sids: &[u64]) -> String {
    serde_json::json!({
        "id": id,
        "cmd": "unsubscribe",
        "params": { "sids": sids }
    })
    .to_string()
}

pub struct WsManager {
    pub ws_url: String,
    pub creds: Arc<Credentials>,
    pub ticker: String,
}

impl WsManager {
    pub fn new(ws_url: String, creds: Arc<Credentials>, ticker: String) -> Self {
        Self {
            ws_url,
            creds,
            ticker,
        }
    }

    pub async fn run(&self, tx: mpsc::UnboundedSender<WsMessage>) -> anyhow::Result<()> {
        loop {
            match self.connect_and_run(&tx).await {
                Ok(()) => {
                    tracing::info!("WebSocket closed cleanly, reconnecting...");
                }
                Err(e) => {
                    tracing::error!(error = %e, "WebSocket error, reconnecting in 1s...");
                    let _ = tx.send(WsMessage::Error {
                        code: 0,
                        message: format!("WS disconnect: {}", e),
                    });
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    }

    async fn connect_and_run(
        &self,
        tx: &mpsc::UnboundedSender<WsMessage>,
    ) -> anyhow::Result<()> {
        let (key, sig, ts) = self.creds.sign("GET", "/trade-api/ws/v2");
        let url = url::Url::parse(&self.ws_url)?;

        let request = tungstenite::http::Request::builder()
            .uri(self.ws_url.as_str())
            .header("KALSHI-ACCESS-KEY", &key)
            .header("KALSHI-ACCESS-SIGNATURE", &sig)
            .header("KALSHI-ACCESS-TIMESTAMP", &ts)
            .header("Host", url.host_str().unwrap_or(""))
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header(
                "Sec-WebSocket-Key",
                tungstenite::handshake::client::generate_key(),
            )
            .body(())?;

        let (ws_stream, _) = tokio_tungstenite::connect_async(request).await?;
        tracing::info!(ticker = %self.ticker, "WebSocket connected");

        let (mut write, mut read) = ws_stream.split();

        for (_id, cmd) in [
            (1, subscribe_cmd(1, &["orderbook_delta"], &self.ticker)),
            (2, subscribe_cmd(2, &["fill"], &self.ticker)),
            (3, subscribe_cmd(3, &["user_orders"], &self.ticker)),
            (4, subscribe_cmd(4, &["trade"], &self.ticker)),
            (5, subscribe_cmd(5, &["ticker"], &self.ticker)),
        ] {
            write
                .send(tungstenite::Message::Text(cmd.into()))
                .await?;
        }

        while let Some(msg) = read.next().await {
            match msg {
                Ok(tungstenite::Message::Text(text)) => {
                    let parsed = parse_ws_message(&text);
                    if tx.send(parsed).is_err() {
                        tracing::info!("Receiver dropped, shutting down WS");
                        return Ok(());
                    }
                }
                Ok(tungstenite::Message::Ping(_)) => {}
                Ok(tungstenite::Message::Close(_)) => {
                    tracing::info!("WS received close frame");
                    return Ok(());
                }
                Err(e) => return Err(e.into()),
                _ => {}
            }
        }

        Ok(())
    }
}