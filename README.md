# Kalshi Market Maker (v1)

A high-performance, passive market-making bot built in Rust for the Kalshi prediction market. This version is optimized for short-term event contracts (e.g., 15-minute Bitcoin markets) and focuses on capturing the spread while strictly managing inventory and risk.

## Core Strategy
This bot dynamically manages order placement across three tactical layers:
*   **Spread Placement:** Joins the best bid/ask on tight spreads (1-2¢) to capture pair profit, or improves pricing by 1¢ on wider spreads to gain queue priority.
*   **Trend Fading:** Tracks midprice momentum using an EWMA tracker. If the market aggressively trends, it lags the quote on the "dangerous" side by 1¢ to avoid adverse selection, while keeping the safe side aggressive.
*   **Inventory Skew:** Automatically tweaks pricing to encourage fills that reduce inventory back to neutral when accumulating too much of one side.

## Safety & Risk Management
*   **Hard Limits:** Instant halting if the hard inventory cap or daily loss limits are breached.
*   **Stale Data Protection:** Halts trading if the WebSocket feed disconnects or if the orderbook state falls behind.

## Prerequisites
*   Rust and Cargo installed.
*   Kalshi API access (Key ID and Private Key).
