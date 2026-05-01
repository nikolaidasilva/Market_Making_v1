use std::sync::Arc;
use serde::Deserialize;

use crate::auth::Credentials;

const BASE_URL: &str = "https://api.elections.kalshi.com/trade-api/v2";

#[derive(Deserialize, Debug)]
struct MarketsResponse {
    markets: Vec<Market>,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Market {
    pub ticker: String,
    pub title: String,
    pub status: String,
    #[serde(default)]
    pub yes_price: Option<i64>,
    #[serde(default)]
    pub volume: Option<i64>,
    #[serde(default)]
    pub volume_fp: Option<String>,
    #[serde(default)]
    pub event_ticker: Option<String>,
    #[serde(default)]
    pub close_time: Option<String>,
    #[serde(default)]
    pub expiration_time: Option<String>,
}

pub async fn find_active_markets(
    creds: &Arc<Credentials>,
    http: &reqwest::Client,
    series: &str,
) -> anyhow::Result<Vec<Market>> {
    let path = format!(
        "/trade-api/v2/markets?series_ticker={}&status=open&limit=100",
        series
    );
    let url = format!(
        "{}/markets?series_ticker={}&status=open&limit=100",
        BASE_URL, series
    );

    let (key, sig, ts) = creds.sign("GET", &path);

    let resp = http
        .get(&url)
        .header("KALSHI-ACCESS-KEY", &key)
        .header("KALSHI-ACCESS-SIGNATURE", &sig)
        .header("KALSHI-ACCESS-TIMESTAMP", &ts)
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Kalshi API error {}: {}", status, body);
    }

    let data: MarketsResponse = resp.json().await?;
    let mut markets = data.markets;

    markets.sort_by(|a, b| {
        let a_time = a.close_time.as_deref().unwrap_or("9999");
        let b_time = b.close_time.as_deref().unwrap_or("9999");
        a_time.cmp(b_time)
    });

    Ok(markets)
}

pub async fn find_current_market(
    creds: &Arc<Credentials>,
    http: &reqwest::Client,
    series: &str,
) -> anyhow::Result<Market> {
    // Hint the Vec type here
    let markets: Vec<Market> = find_active_markets(creds, http, series).await?;

    if markets.is_empty() {
        anyhow::bail!("No open {} markets found", series);
    }

    let now = chrono::Utc::now();

    let candidates: Vec<&Market> = markets
        .iter()
        .filter(|m| {
            let legacy_vol = m.volume.unwrap_or(0);
            // Explicit type for turbofish parse
            let fp_vol = m.volume_fp.as_ref()
                .and_then(|s| s.parse::<f64>().ok())
                .map(|f| f.round() as i64)
                .unwrap_or(0);
            
            let has_volume = legacy_vol > 0 || fp_vol > 0;

            let not_expiring = m
                .close_time
                .as_ref()
                .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                // Explicit type for the closure parameter
                .map(|close: chrono::DateTime<chrono::FixedOffset>| (close.with_timezone(&chrono::Utc) - now).num_seconds() > 60)
                .unwrap_or(true);

            has_volume && not_expiring
        })
        .collect();
    
    // ... rest of the function

    let market = candidates
        .first()
        .cloned()
        .cloned()
        .or_else(|| {
            markets
                .iter()
                .find(|m| {
                    m.close_time
                        .as_ref()
                        .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                        .map(|close| (close.with_timezone(&chrono::Utc) - now).num_seconds() > 60)
                        .unwrap_or(true)
                })
                .cloned()
        })
        .ok_or_else(|| anyhow::anyhow!("No suitable {} market found (all expiring)", series))?;

    tracing::info!(
        ticker = %market.ticker,
        title = %market.title,
        close_time = ?market.close_time,
        "Selected market"
    );

    Ok(market)
}