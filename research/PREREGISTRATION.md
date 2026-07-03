# HYPOTHESIS PRE-REGISTRATION — InsiderScanner v2
**Locked on: 2026-06-10. This file is NOT edited after data collection begins.**

## Context
Backtest of March-April 2026 data (see HLCarryBot/brain/insider_scanner_verdict.md):
broad signals have no edge; the only unproven slice is executable longshots ≥$700
(win 11.6% vs implied 5.6%, 1.7σ on 43 units). This collection run tests exactly that slice.

## H1 (main hypothesis)
Signals satisfying ALL conditions at the moment of detection (fields from signals_enrich):
- `is_sports = 0`
- `exec_px_300 IS NOT NULL AND exec_px_300 < 0.10` (executable for $300 at ask, cheaper than 10c)
- `ask_depth_12x_usd >= 200` (depth within 1.2x of trade price)
- `usd_size >= 700`
- `enrich_status = 'ok'` and `detect_lag_s <= 300`
have a win-rate exceeding exec_px_300 (implied) by a margin ≥ 3σ (binomial)
AND mean ROI > 0 when entering at exec_px_300, flat stake.

## Analysis rules
- Unit = (gamma_slug, token side). First qualifying signal per unit.
- Resolution: Gamma outcomePrices (decisive: max > 0.99).
- Required volume: **n ≥ 120 units** (~3 months at the March-April pace).
- One interim look at n=60, threshold 3.5σ. No other peeking.
- Windows with heartbeat gaps > 5 min in a row are excluded entirely (the whole day is excluded if the gap > 1h).

## Negative control (mandatory)
Same criterion applied to the `usd_size < 700` slice (win 2.0% in the backtest — known noise).
If an "edge" ≥ 3σ appears there too → systematic pipeline error, H1 is not credited.

## Decision criterion
- H1 confirmed (≥3σ AND ROI>0 AND negative control clean) → build an executor, size $20-30/unit.
- Otherwise → the insider-trading direction is closed FOR GOOD, with no post-hoc reformulation of the hypothesis.
