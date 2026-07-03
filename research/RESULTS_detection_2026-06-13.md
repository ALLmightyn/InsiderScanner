# RESULTS — Insider Detection (Study A: persistence)
Run on 2026-06-13 per pre-registration PREREGISTRATION_detection_2026-06-13.md.

## Data
- Resolved non-sports longshot units (wallet, token, price<0.10): **1022**, but decisive resolutions
  exist ONLY for March (441) and April (580). May-June markets are still open (n=1) →
  **out-of-sample holdout is not possible with the current data.**
- Win-rate of notable longshots: March 20.0% / April 17.9% (vs implied ~5-7%) — a population-level edge exists.

## Study A — VERDICT: persistence NOT confirmed (H0 not rejected)
- Wallets with ≥2 longshot wins over the ENTIRE resolved period: **only 6**.
  5 of them are exactly 2 wins out of 3 bets (n=3, consistent with luck).
- The only "persistent" one, `0xe3f18acc55`: 258 tokens, 48.1% win @ avg 0.022 — BUT this is
  `is_bot=1, High Activity, 45748 trades`. The anomalously low recorded price (2.2c with a 48% win rate)
  is exactly the `price ≠ market price` artifact from the earlier backtest; on real prices the edge disappears.
- Repeat winners across 2+ months: 3 (including the bot).

**Conclusion:** there is NO identifiable, reusable population of insiders in the data.
The aggregate longshot edge is real, but it is population-level/statistical, NOT tied to
specific predictable wallets. "Persistence" was produced by one flagged bot plus a price artifact.

## What would remain to fully clean this up (if revisited)
1. Use real executable entry prices from the CLOB prices-history (NOT the `price` field) — remove the artifact.
   The earlier backtest did this → the edge on achievable prices disappeared. Expectation: same result here.
2. Wait for the May-June markets to resolve (weeks/months) for a genuine out-of-sample test.
3. Study B (feature separation) only makes sense AFTER item 1 — on raw `price` it would produce noise.
   wallet_relationships is empty → the relationship graph was not evaluated.

## User-facing features — actual status
- Wallet age: lit up on the bot (24h), but that was an artifact, not an insider.
- Funding→bet lag, source of funds: not reached (Study B is blocked by item 1).
- Wallet relationships: no data (wallet_relationships=0).
