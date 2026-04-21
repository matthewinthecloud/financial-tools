# Financial Tools

## Project Overview
A collection of tools for financial market analysis and research.
Not connected to real money. Paper trading only via Robinhood if needed.

## Stack
- Python
- yfinance (equity/options data)
- FRED API (macro/economic data)
- pandas for data manipulation
- Potential: Alpaca for paper trading

## Current Status
- Planning phase

## Conventions
- Write clean, well commented Python
- Every function needs a docstring
- No hardcoded API keys — use environment variables via .env file
- Never execute real trades — paper trading only
- Always validate data before analysis

## What NOT to do
- Never connect to real brokerage accounts
- Never hardcode credentials
- Don't build order execution without explicit human confirmation step

## Current Sprint Goal
- TBD — first session will define what to build first

## Ideas Backlog
- Stock screener
- Options analysis tool
- Macro dashboard (rates, inflation, employment)
- Portfolio backtester
- Earnings calendar tracker

## Data Sources
- yfinance: equities, options chains, fundamentals
- FRED API: macro data (free, instant access)
- Polygon.io: market data (free tier)