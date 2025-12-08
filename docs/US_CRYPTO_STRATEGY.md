# US Crypto Trading & Data Strategy

## Context
Major global crypto exchanges (Binance.com, Bybit, OKX, etc.) often restrict access from US IP addresses due to regulatory compliance (SEC/CFTC rules). Attempting to access these APIs from a US IP results in `451 Unavailable For Legal Reasons` or `Service unavailable from a restricted location` errors.

## Immediate Alternatives for US Data Collection

### 1. Binance.US (`binanceus`)
- **Status**: Available in most US states.
- **Liquidity**: Lower than Binance Global, but significant.
- **CCXT ID**: `binanceus`
- **API Compatibility**: Very similar to Binance Global.

### 2. Coinbase Advanced (`coinbaseadvanced`)
- **Status**: Fully regulated in US.
- **Liquidity**: High for USD pairs.
- **CCXT ID**: `coinbaseadvanced` (or `coinbase` for simple REST).
- **Notes**: Good WebSocket support.

### 3. Kraken (`kraken`)
- **Status**: Available in US.
- **Liquidity**: High.
- **CCXT ID**: `kraken`
- **Notes**: robust API, good for funding rate data (if available for spot/margin).

### 4. Gemini (`gemini`)
- **Status**: Available in US (NY regulated).
- **Liquidity**: Moderate.
- **CCXT ID**: `gemini`

## Strategy for Quant Research
Since we cannot access Binance Global directly from the US for live trading or direct data streaming without violating TOS (and risking account freeze):

1.  **Proxy/VPN (Risky)**: Using a VPN to access Binance Global is common but violates Terms of Service. Not recommended for institutional/pro setups due to withdrawal risks.
2.  **Offshore Entity**: Establishing an offshore entity to legally access global markets.
3.  **US-Only Universe**: Develop strategies optimized for the liquidity available on Coinbase, Kraken, and Binance.US.
    *   *Arbitrage*: US vs Global prices (requires data from both).
    *   *Market Making*: On US exchanges.
4.  **Data-Only Access**: Use a non-US server (AWS region Tokyo/Frankfurt) to run the ingestion pipeline for Binance Global data, storing it to S3, which is then accessed by US research teams.

## Current Implementation Plan
We will switch the ingestion pipeline to use **Binance.US** and **Coinbase Advanced** to ensure stability and compliance while developing the core infrastructure.
