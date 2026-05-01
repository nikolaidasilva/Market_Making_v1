use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub api_key_id: String,
    pub private_key_pem: String,

    pub rest_base_url: String,
    pub ws_url: String,

    pub series_ticker: String,

    /// Minimum profit in cents per YES+NO pair.
    pub min_pair_profit_cents: i64,

    pub max_inventory: i64,
    pub hard_inventory_cap: i64,

    pub daily_loss_cap_cents: i64,

    pub rate_limit_writes_per_sec: u32,

    /// How many contracts of inventory before skew kicks in (per 1¢ adjustment).
    pub inventory_skew_threshold: i64,

    /// EWMA alpha for trend tracker. Higher = more responsive.
    /// 0.3 works well for KXBTC15M (updates every few ms, ~20-50 book changes/sec).
    pub trend_alpha: f64,

    /// Trend EWMA threshold in cents to declare a trend.
    /// 0.4 means the average midprice delta per update must exceed 0.4¢.
    /// Lower = more sensitive (fades more often, less volume).
    /// Higher = less sensitive (fades only on strong moves, more volume but more trend losses).
    pub trend_threshold: f64,

    pub quote_refresh_interval: Duration,
    pub rest_timeout: Duration,

    pub market_check_interval: Duration,
    pub market_rotate_before_close_secs: i64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            api_key_id: String::new(),
            private_key_pem: String::new(),
            rest_base_url: "https://api.elections.kalshi.com/trade-api/v2".into(),
            ws_url: "wss://api.elections.kalshi.com/trade-api/ws/v2".into(),
            series_ticker: "KXBTC15M".into(),
            min_pair_profit_cents: 1,
            max_inventory: 5,
            hard_inventory_cap: 10,
            daily_loss_cap_cents: 500,
            rate_limit_writes_per_sec: 28,
            inventory_skew_threshold: 2,
            trend_alpha: 0.4,
            trend_threshold: 0.15,
            quote_refresh_interval: Duration::from_millis(5),
            rest_timeout: Duration::from_millis(2000),
            market_check_interval: Duration::from_secs(30),
            market_rotate_before_close_secs: 90,
        }
    }
}

impl Config {
    pub fn from_env() -> Self {
        let mut c = Self::default();

        if let Ok(v) = std::env::var("SERIES_TICKER") { c.series_ticker = v; }
        if let Ok(v) = std::env::var("MIN_PAIR_PROFIT") {
            if let Ok(n) = v.parse() { c.min_pair_profit_cents = n; }
        }
        if let Ok(v) = std::env::var("MAX_INVENTORY") {
            if let Ok(n) = v.parse() { c.max_inventory = n; }
        }
        if let Ok(v) = std::env::var("HARD_INVENTORY_CAP") {
            if let Ok(n) = v.parse() { c.hard_inventory_cap = n; }
        }
        if let Ok(v) = std::env::var("DAILY_LOSS_CAP") {
            if let Ok(n) = v.parse() { c.daily_loss_cap_cents = n; }
        }
        if let Ok(v) = std::env::var("RATE_LIMIT_WPS") {
            if let Ok(n) = v.parse() { c.rate_limit_writes_per_sec = n; }
        }
        if let Ok(v) = std::env::var("INVENTORY_SKEW_THRESHOLD") {
            if let Ok(n) = v.parse() { c.inventory_skew_threshold = n; }
        }
        if let Ok(v) = std::env::var("TREND_ALPHA") {
            if let Ok(n) = v.parse() { c.trend_alpha = n; }
        }
        if let Ok(v) = std::env::var("TREND_THRESHOLD") {
            if let Ok(n) = v.parse() { c.trend_threshold = n; }
        }
        if let Ok(v) = std::env::var("QUOTE_REFRESH_MS") {
            if let Ok(n) = v.parse::<u64>() { c.quote_refresh_interval = Duration::from_millis(n); }
        }
        if let Ok(v) = std::env::var("REST_TIMEOUT_MS") {
            if let Ok(n) = v.parse::<u64>() { c.rest_timeout = Duration::from_millis(n); }
        }
        if let Ok(v) = std::env::var("ROTATE_BEFORE_CLOSE_SECS") {
            if let Ok(n) = v.parse() { c.market_rotate_before_close_secs = n; }
        }
        if let Ok(v) = std::env::var("REST_BASE_URL") { c.rest_base_url = v; }
        if let Ok(v) = std::env::var("WS_URL") { c.ws_url = v; }

        c
    }
}