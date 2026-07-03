# PRE-REGISTRATION — Insider Detection (identifiability + features)
**Locked on: 2026-06-13. This file is NOT edited after this date. Results are written separately.**

## Question (differs from the 2026-06-10 prereg)
The 06-10 prereg tested EXECUTABILITY (profit on ≥$700 longshots). This one is the upstream question:
**is it even possible in principle to DETECT an insider ahead of time**, and do forensic features
(funding-to-bet lag, wallet age, dominance, source of funds, time to resolution)
carry predictive power. Profitability/executability is separate and comes later.

## Data
- `scanner.db` signals: 2026-03-17 to 2026-06-12 (~3 months). Longshots (price<0.10): 109.6k bets, 30.2k wallets.
- Resolutions: Gamma outcomePrices, decisive = max>0.99. Win = (token_id side == winner).
- Train = 03-17 to 04-30. Test (holdout) = 05-01 to 06-12.

## H0 (null hypothesis, what we're trying to reject)
Insiders are NOT identifiable as persistent entities; forensic features have no
out-of-sample predictive power for winning a longshot.

## Study A — Wallet persistence (DECISIVE, run first)
- Unit dedup: (market, side), non-sports, decisively resolved.
- Candidate = wallet with ≥2 longshot WINS in TRAIN.
- Metric: candidate win-rate in TEST vs the baseline win-rate of all longshot bets in TEST.
- **Success criterion A:** candidates in TEST beat baseline by ≥3σ (binomial) AND actually deliver n_test ≥ 30 bets.
- If candidates barely reappear (n_test < 30) → verdict is "insiders are episodic,
  not persistent → not identifiable ahead of time." This is a valid negative finding.

## Study B — Feature separation (case-control, if A is non-empty OR as a descriptive study)
- Case = longshot winners, control = longshot losers. Non-sports, dedup.
- Features: funding→bet lag (funding_sources.funding_ts), wallet_age_hours, dominance,
  usd_size, time_to_resolution (resolved_ts − bet_ts), source of funds (ultimate_source).
- For each: effect (case vs control) + σ. Thresholds are SEARCHED on train, CONFIRMED on test.
- Multiple comparisons: Bonferroni across the number of features; only out-of-sample results count.

## Negative control (mandatory)
Same pipeline applied to mid-price bets (0.30–0.70) — separation should be ≈0.
If features "separate" winners there too → pipeline artifact, H0 is NOT rejected.

## Hygiene
- Heartbeat gaps are excluded (collector_heartbeat).
- wallet_relationships is EMPTY → the relationship graph is NOT evaluated in this iteration (noted honestly).
- signals = only "detected" trades (a bias); if A/B come out borderline — pull the
  full history of candidate wallets from data-api.polymarket.com before the final verdict.

## Decision criterion
- A is persistent AND ≥1 feature holds out-of-sample AND the negative control is clean
  → insider is DETECTABLE; record which tools work; executability comes next.
- Otherwise → insider detection is deemed structurally impossible at our data scale;
  this direction is closed WITHOUT post-hoc reformulation.
