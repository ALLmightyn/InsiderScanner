
# --- PATH BOOTSTRAP ---
import sys as _sys, os as _os
_SRC_DIR = _os.path.dirname(_os.path.abspath(__file__))
_PROJECT_DIR = _os.path.dirname(_SRC_DIR)
for _p in [_SRC_DIR, _os.path.join(_PROJECT_DIR, 'config')]:
    if _p not in _sys.path:
        _sys.path.insert(0, _p)
del _sys, _os, _SRC_DIR, _PROJECT_DIR, _p
import os
import sys
import time
import asyncio
import json
import aiohttp
import aiofiles
import traceback
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

import alert_manager
from config import (
    crash_log,
    get_async_db_connection,
    CASINO_KEYWORDS,
    USD_LIMIT_NEW,
    USD_LIMIT_OLD,
    PRED_LIMIT_NEW,
    redis_client,
    polymarket_limiter,
    get_polymarket_predictions,
    BASE_DIR,
    DB_FILENAME,
    CUSTOM_LABELS_FILE,
)

POLYMARKET_GAMMA_API = "https://gamma-api.polymarket.com/markets"
POLYMARKET_CLOB_PRICES_HISTORY = "https://clob.polymarket.com/prices-history"
POLYMARKET_DATA_API = "https://data-api.polymarket.com/traded"
POLYMARKET_PRO_API = "https://data-api.polymarket.com/pro-info"

BATCH_SIZE = 20
UPDATE_INTERVAL = 1800  # 30 minutes

# --- DEBUG FLAG ---
DEBUG_MODE = False

# --- DUST TRADE THRESHOLD ---
MIN_ENTRY_PRICE = 0.01  # Ignore trades with entry price < 1 cent


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def debug(msg):
    if DEBUG_MODE:
        print(f"[DEBUG] {msg}")


async def fetch_global_pnl(session, wallet: str) -> Optional[float]:
    """
    Fetches trader's GLOBAL PnL from Polymarket Pro API.
    Endpoint: https://data-api.polymarket.com/pro-info?user={wallet}
    Stage 4 Task 3: Uses polymarket_limiter for rate limiting.

    Returns: total_pnl from API, or None if unavailable
    """
    params = {"user": wallet}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Connection": "keep-alive"
    }

    try:
        # Stage 4: Use token bucket rate limiter
        await polymarket_limiter.consume()
        async with session.get(POLYMARKET_PRO_API, params=params, headers=headers, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                # Extract total_pnl or profit field
                global_pnl = data.get("total_pnl") or data.get("profit") or data.get("pnl")
                if global_pnl is not None:
                    try:
                        return float(global_pnl)
                    except (ValueError, TypeError):
                        debug(f"Global PnL parse error for {wallet[:10]}: {global_pnl}")
                return 0.0  # API returned but no PnL field
            else:
                debug(f"Global PnL API Error {resp.status} for {wallet[:10]}")
                return None
    except Exception as e:
        debug(f"Global PnL Exception for {wallet[:10]}: {e}")
        return None


# --- CUSTOM LABELS ---
async def load_custom_labels():
    """Loads manually marked exchange addresses (async version with aiofiles)"""
    if not os.path.exists(CUSTOM_LABELS_FILE):
        async with aiofiles.open(CUSTOM_LABELS_FILE, 'w') as f:
            await f.write(json.dumps({"0x123...": "My Marked Exchange"}))
        return {}
    try:
        async with aiofiles.open(CUSTOM_LABELS_FILE, 'r') as f:
            content = await f.read()
            return json.loads(content)
    except Exception as e:
        log(f"[load_custom_labels] Failed to parse {CUSTOM_LABELS_FILE}: {type(e).__name__}: {e}")
        return {}


# --- MARKET DATA (GAMMA-FIRST APPROACH) ---
async def fetch_market_data(session, slug: str) -> Optional[Dict[str, Any]]:
    """
    Fetches market data from Gamma API by SLUG using query params.
    IMPORTANT: Gamma API expects numeric ID in path, so we use ?slug=... for text slugs.

    Returns: {
        'closed': bool,
        'winner': str (YES/NO or None),
        'current_prices': {'YES': 0.45, 'NO': 0.55},  # From outcomePrices array
        'clob_ids': {'YES': '123456...', 'NO': '789012...'}  # From clobTokenIds array
    }
    """
    conn = await get_async_db_connection()
    cursor = await conn.execute(
        "SELECT * FROM market_resolution_cache WHERE market_slug = ?",
        (slug,)
    )
    cached = await cursor.fetchone()

    # If cached and FRESH (or market is closed - won't change)
    if cached:
        cached_dict = dict(cached)
        is_cached_closed = bool(cached_dict.get('is_closed', False))
        last_checked = cached_dict.get('last_checked', 0)
        age = int(time.time()) - last_checked

        # Only use cache for CLOSED markets or very recent (< 5 min for open)
        if is_cached_closed or age < 300:  # 5 minutes for open markets
            await conn.close()
            return {
                'closed': is_cached_closed,
                'winner': cached_dict.get('winner_outcome'),
                'current_prices': {},  # Cache doesn't store prices
                'clob_ids': {},
                'from_cache': True
            }
    await conn.close()

    try:
        await asyncio.sleep(0.2)
        # FIX: Use params={'slug': slug} instead of path /{slug}
        # Gamma API returns a LIST of markets, so we take [0]
        async with session.get(POLYMARKET_GAMMA_API, params={"slug": slug}, timeout=10) as r:
            if r.status == 422:
                debug(f"Gamma API error for {slug}: HTTP 422 (Invalid slug format)")
                return None
            elif r.status != 200:
                debug(f"Gamma API error for {slug}: HTTP {r.status}")
                return None

            data = await r.json()

            # Gamma API returns a list when querying by slug
            market = None
            if isinstance(data, list) and len(data) > 0:
                market = data[0]
            elif isinstance(data, dict):
                market = data

            if not market:
                debug(f"Market not found for slug: {slug}")
                return None

            is_closed = market.get("closed", False)

            # --- WINNER DETECTION FOR CLOSED MARKETS ---
            winner = None
            if is_closed:
                winner = market.get("winningOutcome")
                if winner:
                    winner = str(winner).upper()

            # --- EXTRACT PRICES AND CLOB IDs FROM JSON ARRAYS ---
            # Use LOWERCASE for consistent matching (Gamma API returns "Yes", "No")
            current_prices = {}
            clob_ids = {}

            try:
                # Gamma API stores these as JSON strings in arrays
                outcomes = json.loads(market.get("outcomes", "[]"))
                prices = json.loads(market.get("outcomePrices", "[]"))
                ids = json.loads(market.get("clobTokenIds", "[]"))

                for i, out in enumerate(outcomes):
                    # Convert to lowercase for matching: "Yes" -> "yes", "No" -> "no"
                    outcome_key = str(out).strip().lower()
                    if i < len(prices):
                        current_prices[outcome_key] = float(prices[i])
                    if i < len(ids):
                        clob_ids[outcome_key] = str(ids[i])
            except Exception as e:
                debug(f"JSON parse error for {slug}: {e}")

            # Fallback: also check tokens array
            tokens = market.get("tokens", [])
            for t in tokens:
                outcome = t.get("outcome", "").strip().lower()
                token_id = t.get("clobTokenId") or t.get("clob_token_id") or t.get("token_id")
                if outcome and token_id:
                    if outcome not in clob_ids:
                        clob_ids[outcome] = str(token_id)

            # Save to cache ONLY for CLOSED markets (cache doesn't store prices)
            if is_closed:
                conn = await get_async_db_connection()
                await conn.execute("""
                    INSERT INTO market_resolution_cache
                    (market_slug, is_closed, winner_outcome, last_checked)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(market_slug) DO UPDATE SET
                        is_closed=excluded.is_closed,
                        winner_outcome=excluded.winner_outcome,
                        last_checked=excluded.last_checked
                """, (slug, is_closed, winner, int(time.time())))
                await conn.commit()
                await conn.close()
                debug(f"  Cached (closed) for {slug[:40]}...")
            # Don't cache open markets - fetch fresh each time

            return {
                'closed': is_closed,
                'winner': winner,
                'current_prices': current_prices,
                'clob_ids': clob_ids,
                'from_cache': False
            }

    except Exception as e:
        debug(f"Market fetch error for {slug}: {e}")
        return None

    return None


# --- PUMP DETECTION VIA HISTORY API ---
async def fetch_price_history(session, clob_id: str) -> Optional[List[Dict]]:
    """
    Fetches price history from CLOB API.
    Endpoint: https://clob.polymarket.com/prices-history?interval=1m&market={clob_id}&duration=1d

    Returns None on HTTP 400/404 or any error - caller must handle gracefully.
    """
    if not clob_id:
        return None

    params = {
        "interval": "1m",
        "market": clob_id,
        "duration": "1d"
    }

    try:
        async with session.get(POLYMARKET_CLOB_PRICES_HISTORY, params=params, timeout=10) as r:
            if r.status == 400:
                debug(f"History Fetch: {clob_id} -> HTTP 400 (Invalid CLOB ID)")
                return None
            elif r.status == 404:
                debug(f"History Fetch: {clob_id} -> HTTP 404 (Not Found)")
                return None
            elif r.status != 200:
                debug(f"History Fetch: {clob_id} -> HTTP {r.status}")
                return None

            history = await r.json()

            if not history or not isinstance(history, list):
                debug(f"History Fetch: {clob_id} -> No data or invalid format")
                return None

            debug(f"History Fetch: {clob_id} -> Found {len(history)} candles")
            return history

    except Exception as e:
        debug(f"History Fetch Error: {clob_id} -> {e}")
        return None


async def check_entry_vs_pump(
    session,
    clob_id: str,
    entry_ts: float,
    entry_price: float,
    outcome: str
) -> Optional[Dict[str, Any]]:
    """
    Checks if price spiked (pump) AFTER the entry.

    Logic:
    1. Find the price at trade_timestamp (or closest to it)
    2. Find the max price in window [trade_timestamp, trade_timestamp + 30 mins]
    3. If (Max_Price - Entry_Price) / Entry_Price > 0.10 (10% jump), mark as PUMP

    Returns None if history fetch fails - does NOT raise exceptions.
    """
    if not clob_id:
        return None

    history = await fetch_price_history(session, clob_id)
    if not history:
        # Graceful failure - no history data available
        return None

    # Sort by timestamp
    history.sort(key=lambda x: x.get('t', 0))

    # Find entry price point and max price in 30-min window after entry
    entry_price_found = None
    max_price_after = 0
    pump_time = 0
    window_end = entry_ts + (30 * 60)  # 30 minutes in seconds

    for point in history:
        curr_ts = point.get('t', 0)
        curr_price = point.get('p', 0)

        # Find price closest to entry timestamp (within 5 minutes tolerance)
        if abs(curr_ts - entry_ts) <= 300:  # 5 min tolerance
            if entry_price_found is None or abs(curr_ts - entry_ts) < abs(entry_price_found[0] - entry_ts):
                entry_price_found = (curr_ts, curr_price)

        # Check prices in the 30-min window AFTER entry
        if entry_ts <= curr_ts <= window_end:
            if curr_price > max_price_after:
                max_price_after = curr_price
                pump_time = curr_ts

    # Use provided entry_price if we couldn't find one in history
    if entry_price_found is None:
        entry_price_used = entry_price
    else:
        entry_price_used = entry_price_found[1]

    # Check for pump: >10% gain from entry price
    if entry_price_used > 0 and max_price_after > 0:
        gain_pct = (max_price_after - entry_price_used) / entry_price_used

        if gain_pct > 0.10:  # 10% threshold
            time_diff = pump_time - entry_ts if pump_time > entry_ts else 0
            return {
                "is_pumped": True,
                "pump_gain": round(gain_pct * 100, 1),
                "time_to_pump_mins": round(time_diff / 60, 1) if time_diff > 0 else 0,
                "entry_price": entry_price_used,
                "max_price": max_price_after
            }

    return None


# --- PNL CALCULATION ---
def calculate_realized_pnl(entry_price: float, shares: float, outcome: str, winner: str, stake_usd: float) -> float:
    """
    Calculates REALIZED PnL for a CLOSED market.

    If won: profit = (stake / entry_price) * 1.0 - stake
    If lost: loss = -stake
    """
    if outcome == winner:
        # Won the bet
        if entry_price > 0:
            payout = (stake_usd / entry_price) * 1.0
            return payout - stake_usd
        return 0
    else:
        # Lost the bet - lose entire stake
        return -stake_usd


def calculate_unrealized_pnl(entry_price: float, current_price: float, shares: float) -> float:
    """
    Calculates UNREALIZED PnL for an OPEN market.

    PnL = (Current_Price - Entry_Price) * Shares
    """
    return (current_price - entry_price) * shares


# --- TRADER ANALYSIS ---
async def analyze_trader(session, wallet: str):
    """
    Analyze a trader's performance with GLOBAL-FIRST approach.

    CRITICAL CHANGES (V7):
    1. Fetch global metrics (predictions, global_pnl) FIRST before any analysis
    2. Strict bot filtering: API trades > 150 = is_bot
    3. "Unrealized Alpha" detection: Include open market PnL in elite calculation
    4. Elite status: Total PnL (Realized + Unrealized) > $500 OR Win Rate > 80%

    V10 FIXES:
    - Strict outcome matching (YES/NO price confusion fixed)
    - Hardened Elite criteria (no false positives from 0% WR traders)
    """
    conn = await get_async_db_connection()
    cursor = await conn.execute(
        "SELECT * FROM signals WHERE trader_addr = ?",
        (wallet,)
    )
    signals = await cursor.fetchall()
    await conn.close()

    if not signals:
        debug(f"No signals found for wallet {wallet[:10]}")
        return

    custom_labels = await load_custom_labels()
    if wallet in custom_labels:
        log(f"Skipping {wallet} (Marked as {custom_labels[wallet]})")
        return

    # === STEP 1: FETCH GLOBAL METRICS FIRST (Before any analysis) ===
    total_predictions = await get_polymarket_predictions(session, wallet)
    global_pnl = await fetch_global_pnl(session, wallet)

    if total_predictions == 0:
        total_predictions = len(signals)  # Fallback to local count

    if global_pnl is None:
        global_pnl = 0.0

    debug(f"Global Metrics for {wallet[:10]}: Trades={total_predictions}, PnL=${global_pnl:.2f}")

    # === STEP 2: STRICT BOT FILTERING ===
    is_bot = False
    bot_reason = None

    # Check 1: High Activity (>150 global trades = bot)
    if total_predictions > 150:
        is_bot = True
        bot_reason = "High Activity"
        log(f"🤖 BOT DETECTED: {wallet[:8]}.. | Reason: {bot_reason} (Trades: {total_predictions})")

    # Check 2: Hedging (betting YES and NO on same market)
    if not is_bot:
        market_outcomes = {}
        for s in signals:
            row = dict(s)
            m_slug = row['slug']
            m_outcome = row['outcome'].replace('🟢 ', '').replace('🔴 ', '').strip().upper()
            if m_slug in market_outcomes:
                if m_outcome not in market_outcomes[m_slug]:
                    market_outcomes[m_slug].append(m_outcome)
                    if len(set(market_outcomes[m_slug])) > 1:
                        is_bot = True
                        bot_reason = "Hedging"
                        break
            else:
                market_outcomes[m_slug] = [m_outcome]

    # Check 3: Over-Diversification (>15 unique markets = bot/scatter shooter)
    if not is_bot:
        unique_markets = set(dict(s)['slug'] for s in signals)
        if len(unique_markets) > 15:
            is_bot = True
            bot_reason = "Diversification"

    if is_bot:
        # Save bot status and skip further analysis
        conn = await get_async_db_connection()
        await conn.execute("""
            INSERT INTO trader_performance
            (wallet_addr, total_trades, is_bot, bot_reason, last_updated)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(wallet_addr) DO UPDATE SET
                total_trades=excluded.total_trades,
                is_bot=excluded.is_bot,
                bot_reason=excluded.bot_reason,
                last_updated=excluded.last_updated
        """, (wallet, total_predictions, 1, bot_reason, int(time.time())))
        await conn.commit()
        await conn.close()
        log(f"Analyzed {wallet[:6]}.. | BOT | Global Trades: {total_predictions} | Reason: {bot_reason}")
        return

    # === STEP 3: ANALYZE SIGNALS (Including Unrealized Alpha) ===
    wins = 0
    losses = 0
    total_pnl = 0.0
    pump_detects = []
    open_market_pnl = 0.0
    closed_market_pnl = 0.0
    signals_processed = 0
    total_closed_trades = 0

    for s in signals:
        row = dict(s)
        m_slug = row['slug']
        m_outcome_raw = row['outcome'].replace('🟢 ', '').replace('🔴 ', '').strip()
        m_outcome = m_outcome_raw.lower()
        m_outcome_upper = m_outcome_raw.upper()  # Keep uppercase for matching
        m_size_usd = row.get('usd_size', 0) or 0
        m_entry_price = row.get('price', 0.5) or 0.5

        # === DUST TRADE FILTER: Ignore trades with entry_price < 0.01 ===
        if m_entry_price < MIN_ENTRY_PRICE:
            debug(f"  -> DUST TRADE FILTERED | Market: {m_slug} | Entry: {m_entry_price} < {MIN_ENTRY_PRICE}")
            continue

        signals_processed += 1

        # --- FETCH MARKET DATA FROM GAMMA API (PRIMARY SOURCE) ---
        m_data = await fetch_market_data(session, m_slug)

        if not m_data:
            debug(f"Market: {m_slug} | Status: FAILED TO FETCH")
            continue

        is_closed = m_data.get('closed', False)
        winner = m_data.get('winner')
        current_prices = m_data.get('current_prices', {})
        clob_ids = m_data.get('clob_ids', {})

        # === FIX #1: STRICT OUTCOME MATCHING ===
        # Try multiple matching strategies to find the exact price for the outcome
        current_price = None

        # Strategy 1: Direct match (outcome name exists in prices dict)
        if m_outcome in current_prices:
            current_price = current_prices[m_outcome]
        elif m_outcome_upper in current_prices:
            current_price = current_prices[m_outcome_upper]

        # Strategy 2: Case-insensitive match
        if current_price is None:
            for key, price in current_prices.items():
                if key.lower() == m_outcome or key.upper() == m_outcome_upper:
                    current_price = price
                    break

        # Log warning if price differs significantly from entry (potential data issue)
        if current_price and abs(current_price - m_entry_price) / m_entry_price > 0.8:
            debug(f"  ⚠️  [PRICE ALERT] {m_slug} | Entry: {m_entry_price:.3f} | Current: {current_price:.3f} | Diff: {((current_price - m_entry_price) / m_entry_price * 100):.0f}%")

        # Get REAL CLOB ID from Gamma API for history lookup
        real_clob_id = clob_ids.get(m_outcome) or clob_ids.get(m_outcome_upper)

        debug(f"Market: {m_slug} | Outcome: {m_outcome_upper} | Entry: {m_entry_price:.3f} | Gamma: {current_price} | Real CLOB ID: {real_clob_id}")

        # --- 1. PNL CALCULATION (Realized + Unrealized) ===
        if is_closed:
            # CLOSED MARKET - Realized PnL
            if winner:
                winner_lower = str(winner).strip().lower()
                shares = m_size_usd / m_entry_price if m_entry_price > 0 else 0
                pnl = calculate_realized_pnl(m_entry_price, shares, m_outcome, winner_lower, m_size_usd)
                total_closed_trades += 1

                if m_outcome == winner_lower:
                    wins += 1
                    debug(f"  -> WIN | Winner: {winner} | PnL: ${pnl:.2f}")
                else:
                    losses += 1
                    debug(f"  -> LOSS | Winner: {winner} | PnL: ${pnl:.2f}")

                closed_market_pnl += pnl
                total_pnl += pnl
            else:
                debug(f"  -> SKIPPED | No winner determined for closed market")

        else:
            # OPEN MARKET - Unrealized PnL (using Gamma API price)
            if current_price is not None and m_entry_price > 0:
                shares = m_size_usd / m_entry_price
                unrealized_pnl = calculate_unrealized_pnl(m_entry_price, current_price, shares)
                open_market_pnl += unrealized_pnl
                total_pnl += unrealized_pnl  # Add to total for elite detection

                debug(f"  -> OPEN | Entry: {m_entry_price:.3f} | Gamma: {current_price:.3f} | Shares: {shares:.1f} | Unrealized PnL: ${unrealized_pnl:.2f}")
            else:
                debug(f"  -> OPEN | No price available for outcome {m_outcome_upper} (current_price={current_price}, entry={m_entry_price})")

        # --- 2. PUMP DETECTION (using REAL CLOB ID from Gamma API) ---
        try:
            meta = json.loads(row.get('meta_data', '{}'))
            entry_ts = meta.get('ts', 0)

            if entry_ts and real_clob_id:
                pump_info = await check_entry_vs_pump(
                    session,
                    real_clob_id,
                    entry_ts,
                    m_entry_price,
                    m_outcome
                )

                if pump_info and pump_info.get('is_pumped'):
                    pump_detects.append(
                        f"{m_slug}: +{pump_info['pump_gain']}% in {pump_info['time_to_pump_mins']}m"
                    )
                    debug(f"  -> PUMP DETECTED | +{pump_info['pump_gain']}% in {pump_info['time_to_pump_mins']}m")
                elif real_clob_id and pump_info is None:
                    debug(f"  -> PUMP CHECK | No history data for CLOB ID: {real_clob_id}")
        except Exception as e:
            debug(f"  -> Pump check error: {e}")

    # === STEP 4: CALCULATE METRICS ===
    win_rate = (wins / total_closed_trades * 100) if total_closed_trades > 0 else 0
    local_roi = (total_pnl / sum(dict(s)['usd_size'] for s in signals) * 100) if signals else 0

    # === STEP 5: ELITE / HIGH PERFORMER DETECTION (V10 HARDENED) ===
    is_high_performer = False
    high_performer_reason = None

    # Elite Logic 1: Total PnL (Realized + Unrealized) > $500 AND local_roi > 50%
    # BUT: Only if win_rate > 0 OR (total_trades > 5 AND global_pnl > 0)
    if total_pnl > 500 and local_roi > 50:
        # Additional checks to prevent false positives
        if win_rate > 0:
            is_high_performer = True
            high_performer_reason = f"High Alpha (${total_pnl:.0f})"
        elif total_closed_trades > 5 and global_pnl > 0:
            # Allow elite status if global_pnl from API confirms profitability
            is_high_performer = True
            high_performer_reason = f"High Alpha + Global PnL (${global_pnl:.0f})"
        # else: 0% win rate with few trades = too risky, don't mark as elite

    # Elite Logic 2: Win Rate > 80% (only if total_closed_trades >= 2)
    if total_closed_trades >= 2 and win_rate > 80:
        is_high_performer = True
        high_performer_reason = f"High WR ({win_rate:.0f}%)"

    # === STEP 6: INSIDER DETECTION ===
    is_insider = False
    insider_reason = []

    # Exclude if global_pnl is negative
    if global_pnl < 0:
        insider_reason.append(f"Global PnL Negative: ${global_pnl:.2f}")
    else:
        # High win rate on closed markets
        if total_closed_trades >= 3 and win_rate > 70:
            is_insider = True
            insider_reason.append(f"High WR {win_rate:.0f}% (${closed_market_pnl:.0f})")

        # Pump hunter
        if len(pump_detects) > 0:
            is_insider = True
            insider_reason.append(f"Pump Hunter ({len(pump_detects)}x)")

    # === STEP 7: UPDATE DATABASE ===
    conn = await get_async_db_connection()
    await conn.execute("""
        INSERT INTO trader_performance
        (wallet_addr, total_trades, wins, losses, win_rate, total_pnl_usd, global_pnl_usd,
         is_bot, bot_reason, is_high_performer, is_insider, insider_reason, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(wallet_addr) DO UPDATE SET
            total_trades=excluded.total_trades,
            wins=excluded.wins,
            losses=excluded.losses,
            win_rate=excluded.win_rate,
            total_pnl_usd=excluded.total_pnl_usd,
            global_pnl_usd=excluded.global_pnl_usd,
            is_bot=excluded.is_bot,
            bot_reason=excluded.bot_reason,
            is_high_performer=excluded.is_high_performer,
            is_insider=excluded.is_insider,
            insider_reason=excluded.insider_reason,
            last_updated=excluded.last_updated
    """, (
        wallet,
        total_predictions,  # Use API count, not local
        wins,
        losses,
        win_rate,
        total_pnl,  # Includes unrealized
        global_pnl,
        1 if is_bot else 0,
        bot_reason,
        1 if is_high_performer else 0,
        1 if is_insider else 0,
        ", ".join(insider_reason + pump_detects),
        int(time.time())
    ))
    await conn.commit()
    await conn.close()

    # === FINAL LOG ===
    status = "ELITE" if is_high_performer else "USER"
    log(f"Analyzed {wallet[:6]}.. | {status} | Global Trades: {total_predictions} | PnL: ${total_pnl:.0f} (Realized: ${closed_market_pnl:.0f}, Unrealized: ${open_market_pnl:.0f}) | WR: {win_rate:.0f}%")


async def worker():
    # First things first — check the environment
    from config import check_required_env
    check_required_env()

    print("📊 Analyst V7 (Redis Consumer Mode) - Global-First Analysis + Unrealized Alpha Detection")
    print("=" * 70)
    print(f"📋 Dust Trade Filter: Entry Price < ${MIN_ENTRY_PRICE} will be ignored")
    print(f"🤖 Bot Filter: Global Trades > 150 = BOT")
    print(f"💎 Elite Detection: Total PnL > $500 + ROI > 50% OR WR > 80%")
    print(f"📁 Database: {DB_FILENAME}")
    print("=" * 70)

    # ==========================================
    # 🚀 STAGE 4 TASK 1: REDIS CONSUMER (FIX-1: Pub/Sub)
    # Replace DB polling with Redis Pub/Sub subscriber
    # ==========================================
    log("[REDIS] Starting signals_broadcast Pub/Sub subscriber for performance analysis...")

    # Subscribe to broadcast channel
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("signals_broadcast")
    log("[REDIS] Subscribed to signals_broadcast channel")

    # Single persistent session for all requests (reuse TCP connections)
    connector = aiohttp.TCPConnector(limit=20, keepalive_timeout=30)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            try:
                # Listen for messages from Pub/Sub
                async for message in pubsub.listen():
                    if message["type"] != "message":
                        continue

                    signal_data = json.loads(message["data"])

                    # Extract wallet from signal
                    wallet = signal_data.get('wallet')
                    if wallet:
                        try:
                            # Analyze the trader's performance (reuse session)
                            await analyze_trader(session, wallet)
                        except Exception as analyze_err:
                            log(f"[REDIS] Error analyzing wallet {wallet[:10]}...: {analyze_err}")
                            crash_log("performance_worker_analyze", analyze_err, traceback.format_exc())

            except Exception as e:
                log(f"[REDIS] Consumer error: {e}")
                crash_log("performance_worker_redis", e, traceback.format_exc())
                await asyncio.sleep(1)


if __name__ == "__main__":
    try:
        asyncio.run(worker())
    except KeyboardInterrupt:
        pass