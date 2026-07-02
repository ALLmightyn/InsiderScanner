# InsiderScanner

**On-chain detector for informed trading on Polymarket, with a pre-registered backtest and an honest null-ish result.**

![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![Status](https://img.shields.io/badge/status-research%20complete-blue)
![License](https://img.shields.io/badge/license-proprietary-lightgrey)

## Overview

Monitors on-chain activity around Polymarket markets to detect wallets that look like they're trading on information rather than noise — large longshot bets, unusual timing relative to market-moving events, and wallet-behavior patterns that don't match typical retail flow.

What makes this project worth reading isn't just the detector — it's that the research was **pre-registered before running the backtest** (hypothesis and analysis plan committed in `research/PREREGISTRATION*.md` before looking at outcomes) and the results are reported honestly, including the parts that didn't confirm the original thesis.

## Findings

**Study A — wallet persistence (pre-registered, 2026-06-13):**
- Aggregate longshot win-rate for flagged bets: 20.0% (March) / 17.9% (April) vs. an implied base rate of ~5–7% — a real, population-level edge exists.
- **But it isn't tied to a reusable set of "smart" wallets.** Only 6 wallets had ≥2 longshot wins across the full resolved period; the one apparent "persistent" wallet turned out to be a high-frequency bot whose recorded win rate was an artifact of stale price data, not real edge.
- **Verdict: H0 not rejected.** There is no identifiable, follow-able population of insiders in this data — the edge is real but not attributable to specific predictable wallets. Reported as a negative result rather than reframed as a win.

**Earlier backtest (1.1M signals):** confirmed the same underlying tension from a different angle — informed-longshot signals carry real edge in aggregate (+7.8%), but a naive follow-the-signal execution strategy is dead on arrival, because the market re-prices before the follow trade can execute.

**Practical use:** rather than an attack strategy, the detector is repurposed as a **toxic-flow filter** for market-making risk management (see [MarketMakerBot](../MarketMakerBot)) — useful as a defensive signal even where it fails as an offensive one.

## Key features

- **On-chain enrichment pipeline** (`enrich_v2.py`, `market_discovery.py`) — pulls and labels wallet activity beyond what Polymarket's own API exposes
- **UMA oracle watcher** — tracks resolution-relevant events that can precede informed trading
- **Hybrid detection model** (`hybrid_detector.py`) — combines rule-based and statistical signals rather than a single heuristic
- **Pre-registered methodology** — hypotheses and analysis plans committed before results, to avoid post-hoc rationalization
- **Telegram alerting** for real-time flagged activity

## Tech stack

Python (async), on-chain data enrichment against Polygon, Polymarket Gamma/CLOB APIs, `pm2` process management.

## Project structure

```
InsiderScanner/
├── src/
│   ├── hybrid_detector.py
│   ├── enrich_v2.py
│   ├── market_discovery.py
│   ├── uma_oracle_watcher.py
│   ├── alert_manager.py
│   └── retro_worker.py / performance_worker.py
├── research/
│   ├── PREREGISTRATION*.md   # hypotheses committed before results
│   └── RESULTS_*.md          # findings, reported as-is
├── config/
└── support/                   # watchlist + label data (real watchlist excluded from git)
```

## Setup

```bash
cp config/.env.example config/.env   # RPC URL, Polygonscan key, Telegram token
pip install -r requirements.txt
python src/maintest.py
```

## License

Proprietary — shared for demonstration purposes only. Not licensed for reuse, redistribution, or commercial use.
