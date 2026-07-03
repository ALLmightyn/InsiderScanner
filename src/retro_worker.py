
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
import aiosqlite
import traceback
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any

# ==========================================
# ⚙️ CONFIG
# ==========================================

# BASE_DIR and DB_FILENAME are imported from config below

POLYMARKET_GAMMA_API = "https://gamma-api.polymarket.com/markets"
POLYMARKET_CLOB_HISTORY = "https://clob.polymarket.com/prices-history"

# Import from central config - STAGE 2: async DB support
from config import (
    ORACLE_MAX_TRADES,
    ORACLE_MIN_MEANINGFUL_TRADES,
    normalize_outcome,
    normalize_outcome_for_price,
    acquire_lock,
    release_lock,
    get_async_db_connection,
    crash_log,
    get_data_cleanup_cutoff,
    get_cron_state_async,
    set_cron_state_async,
    get_polymarket_predictions,
    BASE_DIR,
    DB_FILENAME,
)

# Scan settings
SCAN_INTERVAL_HOURS = 6
ORACLE_PULSE_INTERVAL_HOURS = 6  # Oracle Pulse scan interval

# 🛡️ PRODUCTION THRESHOLDS
ROI_THRESHOLD = 50              # Only show 50%+ profit
ORACLE_MIN_ROI = 50             # Minimum ROI for Oracle Pulse
ORACLE_MIN_USD_SIZE = 2000      # Only report positions > $2,000
MIN_USD_SIZE = 1000             # Minimum trade size to consider
RETRO_MAX_TRADES = 50           # Retro worker: stricter threshold for historical analysis

PUMP_THRESHOLD = 0.10           # 10% pump threshold
SNIPE_WINDOW_MINS = 30          # Entry must be <30 min before pump

# Telegram - Import from central config
from config import TG_TOKEN, TG_CHAT_ID

# ==========================================
# 📊 DATABASE HELPERS
# ==========================================


def log(msg):
    t = datetime.now().strftime("%H:%M:%S")
    print(f"[{t}] {msg}")


# ==========================================
# 🏆 ROBUST WINNER DETECTION
# ==========================================

def detect_market_winner(data: Dict[str, Any]) -> Optional[str]:
    """
    Robust winner detection with multiple fallbacks for closed markets.

    Priority:
    1. winningOutcome field from API
    2. tokens array with winner=True flag
    3. tokens array with price == 1.0
    4. outcomePrices array with value == 1.0 (mapped to outcomes array)

    Returns: Winner outcome in UPPERCASE (e.g., "YES", "NO") or None if undetermined
    """
    if not data or not isinstance(data, dict):
        return None

    # Step 1: Check winningOutcome field
    winner = data.get("winningOutcome")
    if winner:
        return str(winner).upper().strip()

    # Step 2: Check tokens array for winner flag or price = 1.0
    tokens = data.get("tokens", [])
    if tokens and isinstance(tokens, list):
        for t in tokens:
            if not isinstance(t, dict):
                continue
            # Check winner flag
            if t.get("winner") is True:
                outcome = t.get("outcome", "")
                if outcome:
                    return str(outcome).upper().strip()
            # Check price = 1.0
            t_price = t.get("price")
            if t_price is not None:
                try:
                    if float(t_price) == 1.0:
                        outcome = t.get("outcome", "")
                        if outcome:
                            return str(outcome).upper().strip()
                except (ValueError, TypeError):
                    pass

    # Step 3: Check outcomePrices array for value = 1.0 and map to outcomes
    outcomes_raw = data.get("outcomes", [])
    prices_raw = data.get("prices", []) or data.get("outcomePrices", [])

    # Parse JSON strings if needed
    if isinstance(outcomes_raw, str):
        try:
            outcomes = json.loads(outcomes_raw)
        except (json.JSONDecodeError, ValueError):
            outcomes = []
    else:
        outcomes = outcomes_raw

    if isinstance(prices_raw, str):
        try:
            prices_array = json.loads(prices_raw)
        except (json.JSONDecodeError, ValueError):
            prices_array = []
    else:
        prices_array = prices_raw

    if outcomes and prices_array and isinstance(outcomes, list) and isinstance(prices_array, list):
        for i, price in enumerate(prices_array):
            try:
                if float(price) == 1.0 and i < len(outcomes):
                    outcome = outcomes[i]
                    if outcome:
                        return str(outcome).upper().strip()
            except (ValueError, TypeError):
                continue

    return None


# ==========================================
# 📡 API HELPERS
# ==========================================

async def fetch_single_market(session, slug: str) -> Optional[Dict]:
    """
    Fetch single market data from Gamma API by slug.
    Returns market data dict with FULL fields including tokens/CLOB IDs.
    """
    try:
        async with session.get(POLYMARKET_GAMMA_API, params={"slug": slug}, timeout=10) as r:
            if r.status == 200:
                data = await r.json()
                if isinstance(data, list) and len(data) > 0:
                    market = data[0]
                    log(f"  → Fetched market: {slug[:40]}... | Keys: {list(market.keys())[:15]}")
                    if 'tokens' in market:
                        log(f"  → Tokens found: {len(market['tokens'])} outcomes")
                    return market
    except Exception as e:
        log(f"Error fetching market {slug}: {e}")
    return None


async def fetch_all_closed_markets(session) -> List[Dict]:
    """
    Fetch ALL closed markets from Gamma API using pagination.
    Returns list of markets with 'closed' = True.
    """
    all_markets = []
    limit = 1000  # Max per page
    offset = 0
    max_pages = 10  # Fetch up to 10,000 markets

    try:
        while offset < max_pages * limit:
            params = {"limit": limit, "offset": offset}
            async with session.get(POLYMARKET_GAMMA_API, params=params, timeout=30) as r:
                if r.status == 200:
                    data = await r.json()
                    if isinstance(data, list) and len(data) > 0:
                        closed = [m for m in data if m.get('closed')]
                        all_markets.extend(closed)
                        log(f"Page {offset//limit + 1}: Found {len(closed)} closed markets (Total: {len(all_markets)})")

                        if len(closed) == 0:
                            break

                        offset += limit
                    else:
                        break
                else:
                    log(f"API Error: HTTP {r.status}")
                    break

            await asyncio.sleep(0.5)

    except Exception as e:
        log(f"Error fetching markets: {e}")

    log(f"Total closed markets fetched: {len(all_markets)}")
    return all_markets


async def fetch_price_history(session, clob_id: str, duration: str = "max") -> Optional[List[Dict]]:
    """
    Fetch price history from CLOB API.
    Returns list of {t: timestamp, p: price} dicts.
    """
    if not clob_id:
        return None

    params = {
        "interval": "1m",
        "market": clob_id,
    }

    if duration and duration != "max":
        params["duration"] = duration

    try:
        async with session.get(POLYMARKET_CLOB_HISTORY, params=params, timeout=10) as r:
            if r.status == 200:
                history = await r.json()
                if isinstance(history, list):
                    return history
    except Exception as e:
        log(f"Price history fetch error: {e}")

    return None


# ==========================================
# 📡 TELEGRAM - Use alert_manager
# ==========================================
# All Telegram messages should use alert_manager.send_telegram
# This function is deprecated but kept for backward compatibility
async def send_telegram(message: str) -> bool:
    """Send Telegram message - wrapper for alert_manager.send_telegram."""
    from alert_manager import send_telegram as alert_send
    return await alert_send(message)


# ==========================================
# 🔍 INSIDER DETECTION (DB-ONLY)
# ==========================================

async def detect_high_roi_traders(conn, market_slug: str, winner: str) -> List[Dict]:
    """
    Find traders with ROI > threshold on this market.

    ROI Calculation:
    - For YES bets: ROI = (1.0 - entry_price) / entry_price * 100
    - For NO bets: ROI = entry_price / (1.0 - entry_price) * 100

    Filters:
    - DATA_CLEANUP_CUTOFF: Ignore trades before cutoff timestamp
    - Wash Trading Exclusion: Exclude wallets that bet on multiple outcomes
    - Aggregation: GROUP BY trader_addr, slug with HAVING SUM(usd_size) >= threshold

    FIX: Now aggregates ALL trades per wallet per market BEFORE applying size threshold.
    This ensures correct weighted average entry price and total PnL calculation.
    """
    winner_clean = winner.upper().strip() if winner else ""

    # Count total trades vs trades after cutoff
    trade_count_total = await conn.execute(
        "SELECT COUNT(*) as cnt FROM signals WHERE slug = ?", (market_slug,)
    )
    trade_count_total = await trade_count_total.fetchone()
    
    trade_count_after_cutoff = await conn.execute(
        "SELECT COUNT(*) as cnt FROM signals WHERE slug = ? AND (timestamp >= ? OR timestamp IS NULL)",
        (market_slug, get_data_cleanup_cutoff())
    )
    trade_count_after_cutoff = await trade_count_after_cutoff.fetchone()

    total_trades = trade_count_total['cnt'] if trade_count_total else 0
    filtered_trades = trade_count_after_cutoff['cnt'] if trade_count_after_cutoff else 0

    if total_trades == 0:
        log(f"    [DEBUG] No trades found in DB for market {market_slug}.")
        return []

    if filtered_trades == 0:
        log(f"    [DEBUG] All {total_trades} trades filtered out by DATA_CLEANUP_CUTOFF ({get_data_cleanup_cutoff()})")
        return []

    if filtered_trades < total_trades:
        log(f"    [DEBUG] {total_trades - filtered_trades} trades filtered by DATA_CLEANUP_CUTOFF ({filtered_trades} remain)")

    # FIX: Aggregated query with GROUP BY trader_addr, slug
    # - Removes per-row usd_size filter
    # - Uses HAVING SUM(usd_size) >= ? for total position threshold
    # - Uses subquery for MAX(predictions) to get current lifetime trades
    # - Calculates weighted average entry price
    # - Sums PnL across all trades for the wallet on this market
    #
    # PnL Calculation (FIXED):
    # - If user's outcome MATCHES winner → Profit = (1/entry_price - 1) * size
    # - If user's outcome DOES NOT MATCH winner → PnL = -size (total loss of stake)
    query = """
        SELECT
            s.trader_addr,
            -- Use the most common outcome for this wallet on this market
            (SELECT outcome FROM signals
             WHERE trader_addr = s.trader_addr AND slug = s.slug
             GROUP BY outcome
             ORDER BY COUNT(*) DESC
             LIMIT 1) as outcome,
            -- Weighted average entry price: SUM(price * usd_size) / SUM(usd_size)
            SUM(s.price * s.usd_size) / SUM(s.usd_size) as entry_price,
            -- Total position size
            SUM(s.usd_size) as usd_size,
            -- Total PnL across all trades (FIXED: check if outcome matches winner)
            SUM(
                CASE
                    -- User bet on winning outcome → calculate profit
                    WHEN UPPER(REPLACE(REPLACE(s.outcome, '🟢 ', ''), '🔴 ', '')) = ?
                        THEN ((1.0 / s.price - 1.0) * s.usd_size)
                    -- User bet on losing outcome → total loss of stake
                    ELSE -s.usd_size
                END
            ) as pnl_usd,
            -- Lifetime predictions (current, not stale)
            (SELECT MAX(predictions) FROM signals WHERE trader_addr = s.trader_addr) as lifetime_trades,
            -- Meta data from most recent trade
            (SELECT meta_data FROM signals
             WHERE trader_addr = s.trader_addr AND slug = s.slug
             ORDER BY id DESC LIMIT 1) as meta_data
        FROM signals s
        WHERE s.slug = ?
          AND s.price > 0
          AND s.price < 1.0
          AND (s.timestamp >= ? OR s.timestamp IS NULL)
          -- Wash Trading Exclusion: exclude wallets betting on multiple outcomes
          AND s.trader_addr NOT IN (
              SELECT trader_addr
              FROM signals
              WHERE slug = ?
              GROUP BY trader_addr
              HAVING COUNT(DISTINCT UPPER(REPLACE(REPLACE(outcome, '🟢 ', ''), '🔴 ', ''))) > 1
          )
        GROUP BY s.trader_addr, s.slug
        -- FIX: Filter by TOTAL position size, not individual trades
        HAVING SUM(s.usd_size) >= ?
           AND SUM(
               CASE
                   WHEN UPPER(REPLACE(REPLACE(s.outcome, '🟢 ', ''), '🔴 ', '')) = ?
                       THEN ((1.0 / s.price - 1.0) * s.usd_size)
                   ELSE -s.usd_size
               END
           ) > 0
        ORDER BY pnl_usd DESC
    """

    cursor = await conn.execute(query, (winner_clean, market_slug, get_data_cleanup_cutoff(), market_slug, MIN_USD_SIZE, winner_clean))
    rows = await cursor.fetchall()

    log(f"  → Found {len(rows)} high ROI traders for winner '{winner_clean}' (Aggregated position >= ${MIN_USD_SIZE})")

    return [
        {
            'wallet': row['trader_addr'],
            'outcome': row['outcome'],
            'entry_price': row['entry_price'],
            'usd_size': row['usd_size'],
            'roi_pct': None,  # Will be calculated in send_retro_insider_report based on aggregated data
            'pnl_usd': row['pnl_usd'],
            'meta_data': row['meta_data'],
            'predictions': row['lifetime_trades'],
            'total_bets': row['lifetime_trades'],  # Use same value for total_bets
            'slug': market_slug
        }
        for row in rows
    ]


async def detect_sniper_entries(
    session,
    conn,
    market_slug: str,
    clob_ids: Dict[str, str],
    winner: str
) -> List[Dict]:
    """
    Find traders who entered <30 min before price pump.

    FIX: Now aggregates by trader_addr, slug to get:
    - Earliest entry timestamp (for sniper detection)
    - Total position size (SUM)
    - Weighted average entry price
    """
    trade_count_cursor = await conn.execute(
        "SELECT COUNT(*) as cnt FROM signals WHERE slug = ?", (market_slug,)
    )
    trade_count = await trade_count_cursor.fetchone()

    if trade_count is None or trade_count['cnt'] == 0:
        log(f"    [DEBUG] No trades found in DB for market {market_slug}.")
        return []

    # FIX: Aggregated query - get earliest entry timestamp and total position per wallet
    query = """
        SELECT
            s.trader_addr,
            -- Use the most common outcome for this wallet on this market
            (SELECT outcome FROM signals
             WHERE trader_addr = s.trader_addr AND slug = s.slug
             GROUP BY outcome
             ORDER BY COUNT(*) DESC
             LIMIT 1) as outcome,
            -- Weighted average entry price
            SUM(s.price * s.usd_size) / SUM(s.usd_size) as entry_price,
            -- Total position size
            SUM(s.usd_size) as usd_size,
            -- Earliest timestamp (for sniper detection)
            MIN(
                COALESCE(
                    CAST(json_extract(s.meta_data, '$.ts') AS INTEGER),
                    CAST(strftime('%s', REPLACE(s.timestamp, 'Z', '+00:00')) AS INTEGER)
                )
            ) as entry_ts,
            -- Lifetime predictions (current, not stale)
            (SELECT MAX(predictions) FROM signals WHERE trader_addr = s.trader_addr) as lifetime_trades,
            -- Meta data from earliest trade
            (SELECT meta_data FROM signals
             WHERE trader_addr = s.trader_addr AND slug = s.slug
             ORDER BY COALESCE(CAST(json_extract(meta_data, '$.ts') AS INTEGER), 0) ASC
             LIMIT 1) as meta_data
        FROM signals s
        WHERE s.slug = ?
          AND s.price > 0
          AND s.price < 1.0
          AND (s.timestamp >= ? OR s.timestamp IS NULL)
          -- 🛡️ BOT KILLER
          AND (
              SELECT MAX(predictions) FROM signals WHERE trader_addr = s.trader_addr
          ) <= 50
        GROUP BY s.trader_addr, s.slug
        -- FIX: Filter by TOTAL position size, not individual trades
        HAVING SUM(s.usd_size) >= ?
        ORDER BY usd_size DESC
    """

    cursor = await conn.execute(query, (market_slug, get_data_cleanup_cutoff(), MIN_USD_SIZE))
    rows = await cursor.fetchall()
    snipers = []

    log(f"  → Checking {len(rows)} aggregated DB positions for sniper entries...")

    for row in rows:
        outcome = row['outcome'].replace('🟢 ', '').replace('🔴 ', '').strip().upper()
        entry_price = row['entry_price']
        meta = json.loads(row['meta_data']) if row['meta_data'] else {}
        entry_ts = row['entry_ts'] or meta.get('ts', 0)

        if not entry_ts or not outcome:
            continue

        clob_id = clob_ids.get(outcome)
        if not clob_id:
            continue

        history = await fetch_price_history(session, clob_id, "max")

        if not history:
            continue

        history.sort(key=lambda x: x.get('t', 0))

        max_price = 0
        pump_time = 0
        window_end = entry_ts + (SNIPE_WINDOW_MINS * 60)

        for point in history:
            ts = point.get('t', 0)
            price = point.get('p', 0)

            if entry_ts < ts <= window_end:
                if price > max_price:
                    max_price = price
                    pump_time = ts

        if max_price > 0 and entry_price > 0:
            gain_pct = (max_price - entry_price) / entry_price

            if gain_pct > PUMP_THRESHOLD:
                time_to_pump = (pump_time - entry_ts) / 60

                if 0 < time_to_pump <= SNIPE_WINDOW_MINS:
                    pnl = (1.0 - entry_price) / entry_price * entry_price if outcome == winner else -entry_price

                    snipers.append({
                        'wallet': row['trader_addr'],
                        'outcome': outcome,
                        'entry_price': entry_price,
                        'usd_size': row['usd_size'],
                        'entry_ts': entry_ts,
                        'pump_gain_pct': gain_pct * 100,
                        'time_to_pump_mins': time_to_pump,
                        'pnl_usd': pnl,
                        'predictions': row['lifetime_trades'],
                        'total_bets': row['lifetime_trades']
                    })
                    log(f"    🎯 SNIPER DETECTED: {row['trader_addr'][:10]}... | Entry: {entry_price:.3f} | Pump: +{gain_pct*100:.0f}% in {time_to_pump:.1f}m")

        await asyncio.sleep(0.1)

    if snipers:
        log(f"  → Found {len(snipers)} sniper entries from DB!")

    return snipers


# ==========================================
# 📡 ORACLE PULSE - OPEN POSITIONS SCAN (V15 GOD MODE)
# ==========================================

# Semaphore for rate-limiting Polymarket Data API calls
DATA_API_SEMAPHORE = asyncio.Semaphore(10)  # Max 10 concurrent requests


async def fetch_positions_from_api(session, wallet: str) -> Dict[str, Dict[str, Any]]:
    """
    🚀 PORTFOLIO VERIFICATION: Fetch TRUE positions from Polymarket Data API.

    Returns dict: {slug: {'avgPrice': float, 'size': float, 'outcome': str, 'total': float, 
                          'curPrice': float, 'currentValue': float}}

    Rate limited via semaphore to avoid 429 errors.
    """
    positions = {}

    async with DATA_API_SEMAPHORE:
        try:
            url = f"https://data-api.polymarket.com/positions"
            params = {"user": wallet}

            async with session.get(url, params=params, timeout=15) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    # API returns a list: [{asset, slug, avgPrice, size, outcome, currentValue, curPrice}, ...]
                    if isinstance(data, list) and len(data) > 0:
                        for pos in data:
                            if isinstance(pos, dict):
                                slug = pos.get('slug')
                                if slug:
                                    positions[slug] = {
                                        'avgPrice': float(pos.get('avgPrice', 0)),
                                        'size': float(pos.get('size', 0)),
                                        'outcome': pos.get('outcome', ''),
                                        'total': float(pos.get('currentValue', 0)),
                                        'curPrice': float(pos.get('curPrice', 0)),
                                        'currentValue': float(pos.get('currentValue', 0))
                                    }
        except Exception as e:
            # Log rate limits or API errors to crash log for debugging
            crash_log("fetch_positions_from_api", e, traceback.format_exc())
            pass  # Silent fail for rate limits or API errors

    return positions


async def fetch_market_price_from_gamma(session, slug: str) -> Dict[str, Any]:
    """
    TASK 3: Fetch live prices AND market title from Gamma API.
    
    Universal Price Normalization - Case-insensitive, emoji-blind outcome matching.
    Creates a mapping: {'YES': price, 'NO': price, ...} and matches outcomes perfectly.

    Returns dict: {'yes': price or None, 'no': price or None, 'title': str, 'closed': bool, 'resolved': bool}
    """
    result = {'yes': None, 'no': None, 'title': slug, 'closed': False, 'resolved': False}

    try:
        async with session.get(POLYMARKET_GAMMA_API, params={"slug": slug}, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                if isinstance(data, list) and len(data) > 0:
                    market = data[0]
                    result['title'] = market.get('title', slug)
                    result['closed'] = market.get('closed', False)
                    result['resolved'] = market.get('resolved', False)

                    outcomes_raw = market.get('outcomes', [])
                    prices_raw = market.get('outcomePrices', []) or market.get('prices', [])

                    if isinstance(outcomes_raw, str):
                        try:
                            outcomes = json.loads(outcomes_raw)
                        except (json.JSONDecodeError, ValueError):
                            outcomes = []
                    else:
                        outcomes = outcomes_raw

                    if isinstance(prices_raw, str):
                        try:
                            outcome_prices = json.loads(prices_raw)
                        except (json.JSONDecodeError, ValueError):
                            outcome_prices = []
                    else:
                        outcome_prices = prices_raw

                    # TASK 3: Build price map with normalized outcome keys
                    price_map = {}
                    for i, outcome in enumerate(outcomes):
                        if i < len(outcome_prices):
                            price = float(outcome_prices[i]) if outcome_prices[i] else 0
                            # Normalize outcome to uppercase, emoji-free for consistent matching
                            outcome_normalized = normalize_outcome_for_price(str(outcome))
                            price_map[outcome_normalized] = price

                    # Map YES/NO prices from the price map
                    if 'YES' in price_map:
                        result['yes'] = price_map['YES']
                    if 'NO' in price_map:
                        result['no'] = price_map['NO']
    except Exception as e:
        log(f"  → Failed to fetch price for {slug[:40]}...: {e}")

    return result


async def scan_open_positions(conn, session) -> List[Dict]:
    """
    🚀 V17 FIXED MODE: Scan for profitable open positions using ONLY Polymarket Data API.

    API-First Architecture (FIXED):
    - DB (signals table) is used ONLY to get a list of wallet addresses to check
    - ALL PnL/ROI data comes DIRECTLY from Polymarket Data API (avgPrice, size, currentValue, curPrice)
    - NO Gamma API calls for prices - use api_pos['curPrice'] and api_pos['currentValue'] directly
    - NO outcome matching (YES/NO) - works with ANY outcome type (TEXANS, UNDER, etc.)
    - DB fallback removed - if API has no position, we don't report it

    🛡️ FILTER STACK:
    - Sybil Filter: ORACLE_MIN_MEANINGFUL_TRADES = 5 (minimum trades for statistical significance)
    - Bot Killer: RETRO_MAX_TRADES = 50 (wallets with >50 trades are bots)
    - Valid Insiders: 5-50 trades (systematic insiders)
    - Wash Trade Filter: Exclude multi-outcome wallets (from DB history)
    - Open Markets Only: Skip truly closed/resolved markets

    ROI/PnL Calculation (FIXED):
    - current_price = api_pos.get('curPrice', 0) - directly from Positions API
    - api_current_value = api_pos.get('currentValue', 0) - directly from Positions API
    - invested = avg_entry_price * total_size
    - pnl_usd = api_current_value - invested
    - roi_pct = (pnl_usd / invested) * 100
    """
    # Step 1: Get all unique wallets from DB (just addresses, no position data)
    wallets_query = """
        SELECT DISTINCT trader_addr FROM signals
        WHERE price > 0 AND price < 1.0
          AND (timestamp >= ? OR timestamp IS NULL)
    """
    cursor = await conn.execute(wallets_query, (get_data_cleanup_cutoff(),))
    wallet_rows = await cursor.fetchall()
    unique_wallets = [row['trader_addr'] for row in wallet_rows]

    log(f"  → Found {len(unique_wallets)} unique wallets in DB to check")

    # Step 2: Fetch TRUE positions from Polymarket Data API for all wallets
    log(f"  → Fetching verified positions from Polymarket Data API...")
    wallet_positions_api = {}

    async def fetch_wallet_positions(wallet):
        positions = await fetch_positions_from_api(session, wallet)
        return wallet, positions

    tasks = [fetch_wallet_positions(w) for w in unique_wallets]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, tuple) and len(result) == 2:
            wallet, positions = result
            if positions:
                wallet_positions_api[wallet] = positions

    log(f"  → Fetched API positions for {len(wallet_positions_api)} wallets")

    # Step 3: Fetch ONLY market status (closed/resolved) from Gamma API - NOT prices
    # We only need to know if market is closed to skip it
    api_slugs = set()
    for positions in wallet_positions_api.values():
        api_slugs.update(positions.keys())

    log(f"  → Fetching market status for {len(api_slugs)} markets from Gamma API...")
    market_status_cache = {}

    gamma_semaphore = asyncio.Semaphore(10)

    async def fetch_market_status(slug):
        """Fetch only market closed/resolved status, NOT prices."""
        async with gamma_semaphore:
            try:
                async with session.get(POLYMARKET_GAMMA_API, params={"slug": slug}, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if isinstance(data, list) and len(data) > 0:
                            market = data[0]
                            return slug, {
                                'closed': market.get('closed', False),
                                'resolved': market.get('resolved', False),
                                'title': market.get('title', slug)
                            }
            except Exception as e:
                log(f"  → Failed to fetch market status for {slug[:40]}...: {e}")
            return slug, {'closed': False, 'resolved': False, 'title': slug}

    tasks = [fetch_market_status(slug) for slug in api_slugs]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, tuple) and len(result) == 2:
            slug, data = result
            market_status_cache[slug] = data

    log(f"  → Fetched market status for {len(market_status_cache)} markets")

    # Step 4: Build positions list using API data as primary source
    positions = []
    api_verified_count = 0
    skipped_closed_count = 0
    skipped_invalid_count = 0

    for wallet, api_positions in wallet_positions_api.items():
        # Get lifetime trades for bot filter
        lifetime_trades_cursor = await conn.execute(
            "SELECT MAX(predictions) FROM signals WHERE trader_addr = ?", (wallet,)
        )
        lifetime_trades_result = await lifetime_trades_cursor.fetchone()
        lifetime_trades = lifetime_trades_result[0] if lifetime_trades_result else 0

        # 🛡️ SYBIL FILTER: Skip wallets with < ORACLE_MIN_MEANINGFUL_TRADES (5) lifetime trades
        # Win rate is statistically noise for accounts with fewer than 5 trades
        if not lifetime_trades or lifetime_trades < ORACLE_MIN_MEANINGFUL_TRADES:
            log(f"  [ORACLE] Skipped {wallet[:10]}... - only {lifetime_trades or 0} predictions (min {ORACLE_MIN_MEANINGFUL_TRADES} required)")
            continue

        # 🛡️ BOT KILLER: Skip wallets with > RETRO_MAX_TRADES (50) predictions
        # Systematic insiders are those between 5 and 50 trades
        if lifetime_trades and lifetime_trades > RETRO_MAX_TRADES:
            log(f"  [ORACLE] Skipped {wallet[:10]}... - {lifetime_trades} predictions (max {RETRO_MAX_TRADES})")
            continue

        # Process each API position
        for slug, api_pos in api_positions.items():
            avg_entry_price = api_pos['avgPrice']
            total_size = api_pos['size']
            api_outcome = api_pos['outcome']

            # 🛡️ SANITY GUARD: Validate price range
            if avg_entry_price <= 0 or avg_entry_price >= 1.0 or total_size <= 0:
                continue

            # 🛡️ ORACLE_MIN_USD_SIZE FILTER: Skip positions < $2000
            if total_size < ORACLE_MIN_USD_SIZE:
                log(f"  [ORACLE] Skipped {slug[:30]}... - size ${total_size:.0f} < ${ORACLE_MIN_USD_SIZE} minimum")
                continue

            # Get market status (closed/resolved) from cache
            market_status = market_status_cache.get(slug, {'closed': False, 'resolved': False, 'title': slug})
            market_title = market_status.get('title', slug)

            # Skip truly closed/resolved markets
            if market_status.get('closed', False) or market_status.get('resolved', False):
                log(f"[ORACLE CLEANUP] Skipped position in {slug} because market is CLOSED.")
                skipped_closed_count += 1
                continue

            # ==========================================================
            # 🔧 FIX: Use api_pos data directly - NO Gamma API call for prices
            # ==========================================================
            # current_price comes DIRECTLY from Positions API (curPrice field)
            # This works for ANY outcome type: YES, NO, TEXANS, UNDER, etc.
            current_price = api_pos.get('curPrice', 0)
            
            # 🛡️ SANITY GUARD: Validate current price from API
            if current_price is None or current_price <= 0 or current_price > 1.0:
                log(f"    ⚠️ SKIPPED: {market_title[:40]}... | Invalid curPrice from API: {current_price}")
                skipped_invalid_count += 1
                continue

            # api_current_value comes DIRECTLY from Positions API (currentValue field)
            # This is the current USD value of the position
            api_current_value = api_pos.get('currentValue', 0)

            # ==========================================================
            # 🔧 FIX: Unified PnL/ROI calculation for ALL outcomes
            # ==========================================================
            # invested = entry price * number of shares (same for YES and NO)
            invested = avg_entry_price * total_size

            # PnL = current value - invested (same formula for ALL outcomes)
            pnl_usd = api_current_value - invested

            # ROI = (PnL / invested) * 100
            roi_pct = (pnl_usd / invested) * 100 if invested > 0 else 0

            # 🛡️ Skip if ROI/PnL calculation resulted in invalid values
            if roi_pct is None or pnl_usd is None or roi_pct <= 0 or pnl_usd <= 0:
                log(f"    ⚠️ SKIPPED: {market_title[:40]}... | Invalid ROI/PnL: {roi_pct:.1f}% / ${pnl_usd:,.0f}")
                skipped_invalid_count += 1
                continue

            # Only include profitable positions with significant ROI
            if roi_pct > ROI_THRESHOLD and pnl_usd > 0:
                positions.append({
                    'wallet': wallet,
                    'slug': slug,
                    'market_title': market_title,
                    'outcome': api_outcome,  # Keep original outcome string (YES, NO, TEXANS, etc.)
                    'entry_price': avg_entry_price,  # API verified
                    'current_price': current_price,  # API verified from curPrice
                    'usd_size': total_size,  # API verified
                    'roi_pct': roi_pct,
                    'pnl_usd': pnl_usd,
                    'lifetime_trades': lifetime_trades,
                    'api_verified': True,
                    'meta_data': json.dumps({'source': 'api'})
                })
                api_verified_count += 1

    log(f"  → Found {len(positions)} profitable open positions (API verified: {api_verified_count}, skipped closed: {skipped_closed_count}, skipped invalid: {skipped_invalid_count})")

    return positions


async def send_oracle_pulse_report(positions: List[Dict], session: aiohttp.ClientSession = None):
    """
    🚀 PROFESSIONAL Oracle Pulse report with API verification badges.
    
    FIX: Fetches LIVE predictions count from Polymarket Data API for each wallet.
    """
    if not positions:
        log("  → No profitable open positions to report")
        return

    # Import from config
    from config import format_wallet_short, format_pnl

    # Group by wallet
    wallet_stats = {}
    for pos in positions:
        w = pos['wallet']
        if w not in wallet_stats:
            wallet_stats[w] = {
                'count': 0,
                'total_pnl': 0,
                'total_roi': 0,
                'lifetime_trades': pos.get('lifetime_trades', 0),
                'api_verified_count': 0,
                'positions': []
            }
        wallet_stats[w]['count'] += 1
        wallet_stats[w]['total_pnl'] += pos['pnl_usd']
        wallet_stats[w]['total_roi'] += pos['roi_pct']
        wallet_stats[w]['positions'].append(pos)
        if pos.get('api_verified', False):
            wallet_stats[w]['api_verified_count'] += 1

    # Sort by total PnL
    top_wallets = sorted(
        wallet_stats.items(),
        key=lambda x: x[1]['total_pnl'],
        reverse=True
    )[:10]

    # ==========================================================
    # 🔧 FIX: Fetch LIVE predictions count - MANDATORY API CALL
    # ==========================================================
    # No fallback to stale DB data - if API fails, we show N/A
    if not session:
        log("  → ERROR: session required for live predictions - cannot send Oracle Pulse report")
        return

    log(f"  → Fetching live predictions count for {len(top_wallets)} wallets...")
    skipped_api_error_count = 0
    
    for wallet, stats in top_wallets:
        try:
            live_preds = await get_polymarket_predictions(session, wallet)
            stats['live_predictions'] = live_preds
            stats['api_fetch_failed'] = False
            log(f"    {wallet[:10]}...: {live_preds} predictions")
        except Exception as e:
            log(f"    FAILED to fetch predictions for {wallet[:10]}...: {e}")
            # Mark as failed - will show N/A in report
            stats['live_predictions'] = None
            stats['api_fetch_failed'] = True
            skipped_api_error_count += 1

    # Build message
    msg = f"📡 <b>ORACLE PULSE</b> - Live Profitable Positions\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

    for i, (wallet, stats) in enumerate(top_wallets, 1):
        wallet_short = format_wallet_short(wallet)
        profile_link = f"https://polymarket.com/profile/{wallet}"
        arkham_link = f"https://intel.arkm.com/explorer/address/{wallet}"
        avg_roi = stats['total_roi'] / stats['count'] if stats['count'] > 0 else 0
        pnl_str = format_pnl(stats['total_pnl'], avg_roi)

        # FIX: Use LIVE predictions count from API - show N/A if failed
        if stats.get('api_fetch_failed', False):
            lifetime_trades = "N/A"
        else:
            lifetime_trades = stats.get('live_predictions', 0)

        # ✅ Add verification badge if all positions are API verified
        api_verified_count = stats.get('api_verified_count', 0)
        verification_badge = " ✅" if api_verified_count == stats['count'] and api_verified_count > 0 else ""

        # 🎯 Show TOP 3 most profitable markets (sorted by ROI)
        sorted_positions = sorted(stats['positions'], key=lambda x: x['roi_pct'], reverse=True)[:3]

        pos_lines = []
        for p in sorted_positions:
            market_title = p.get('market_title', p['slug'])[:45]
            market_link = f"https://polymarket.com/event/{p['slug']}"
            pos_badge = " ✅" if p.get('api_verified', False) else ""
            pos_lines.append(f"   📍 <a href='{market_link}'>{market_title}</a> | {p['outcome']} <b>+{p['roi_pct']:.0f}%</b>{pos_badge}")

        pos_str = "\n".join(pos_lines)

        msg += (
            f"{i}. 📈 <a href='{profile_link}'>{wallet_short}</a> | <a href='{arkham_link}'>Arkham</a>\n"
            f"   📊 <b>Preds: {lifetime_trades}</b> | 🎯 Positions: {stats['count']} | 💰 PnL: {pnl_str}{verification_badge}\n"
            f"{pos_str}\n\n"
        )

    if skipped_api_error_count > 0:
        msg += f"⚠️ API errors: {skipped_api_error_count} wallets showed N/A\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "<i>Live profitable positions - Oracle Pulse Bot</i>"

    await send_telegram(msg)
    log(f"  → Sent Oracle Pulse report with {len(positions)} positions from {len(top_wallets)} wallets")


# ==========================================
# 🔍 RETRO INSIDER AGGREGATED REPORT
# ==========================================

async def send_retro_insider_report(insiders: List[Dict], markets_scanned: int = 0, session=None):
    """
    🔍 Aggregated Retro Insider report - sends ONE summary instead of spam.

    TASK 3 FIX: Aggregates by wallet + market (slug).
    - Groups by (wallet, slug) to avoid duplicate markets per wallet
    - Sums pnl_usd and usd_size
    - Calculates weighted average entry_price
    - Shows: Wallet (0x...) | Total PnL: +$X | Markets: N
    
    FIX: Fetches LIVE predictions count from Polymarket Data API for each wallet.
    """
    if not insiders:
        log("  → No retro insiders found to report")
        return

    # Import from config
    from config import format_wallet_short, format_pnl

    # TASK 3: First aggregate by (wallet, slug) to combine duplicate market entries
    wallet_market_agg = {}
    for insider in insiders:
        key = (insider['wallet'], insider.get('slug', ''))
        if key not in wallet_market_agg:
            wallet_market_agg[key] = {
                'wallet': insider['wallet'],
                'slug': insider.get('slug', ''),
                'market_title': insider.get('market_title', ''),
                'outcome': insider.get('outcome', ''),
                'total_pnl': insider.get('pnl_usd', 0) or 0,
                'total_usd_size': insider.get('usd_size', 0) or 0,
                'weighted_entry_price_sum': (insider.get('entry_price', 0) or 0) * (insider.get('usd_size', 0) or 0),
                'count': 1,
                'is_sniper': 'time_to_pump_mins' in insider,
                'time_to_pump_mins': insider.get('time_to_pump_mins'),
                'predictions': insider.get('predictions', 0),
                'total_bets': insider.get('total_bets', 0)
            }
        else:
            agg = wallet_market_agg[key]
            agg['total_pnl'] += insider.get('pnl_usd', 0) or 0
            agg['total_usd_size'] += insider.get('usd_size', 0) or 0
            agg['weighted_entry_price_sum'] += (insider.get('entry_price', 0) or 0) * (insider.get('usd_size', 0) or 0)
            agg['count'] += 1
            # Keep sniper flag if any entry was a sniper
            if 'time_to_pump_mins' in insider:
                agg['is_sniper'] = True
                # Keep the shortest time_to_pump
                if insider.get('time_to_pump_mins') is not None:
                    if agg['time_to_pump_mins'] is None or insider['time_to_pump_mins'] < agg['time_to_pump_mins']:
                        agg['time_to_pump_mins'] = insider['time_to_pump_mins']

    # Convert to list and calculate weighted average entry price
    aggregated_insiders = []
    for key, agg in wallet_market_agg.items():
        if agg['total_usd_size'] > 0:
            weighted_avg_entry = agg['weighted_entry_price_sum'] / agg['total_usd_size']
        else:
            weighted_avg_entry = 0

        # Calculate ROI for aggregated position - FIX: properly store roi_pct
        if weighted_avg_entry > 0:
            agg_roi = (agg['total_pnl'] / agg['total_usd_size']) * 100
        else:
            agg_roi = 0

        aggregated_insiders.append({
            'wallet': agg['wallet'],
            'slug': agg['slug'],
            'market_title': agg['market_title'],
            'outcome': agg['outcome'],
            'pnl_usd': agg['total_pnl'],
            'usd_size': agg['total_usd_size'],
            'entry_price': weighted_avg_entry,
            'roi_pct': agg_roi,  # FIX: Properly set roi_pct
            'is_sniper': agg['is_sniper'],
            'time_to_pump_mins': agg['time_to_pump_mins'],
            'predictions': agg['predictions'],
            'total_bets': agg['total_bets'],
            'original_count': agg['count']  # How many entries were aggregated
        })

    # Group by wallet (now with aggregated market data)
    wallet_stats = {}
    for insider in aggregated_insiders:
        w = insider['wallet']
        if w not in wallet_stats:
            wallet_stats[w] = {
                'count': 0,
                'total_pnl': 0,
                'total_roi': 0,
                'predictions': insider.get('predictions', 0),
                'total_bets': insider.get('total_bets', 0),
                'is_sniper': False,
                'markets': []
            }
        wallet_stats[w]['count'] += 1
        wallet_stats[w]['total_pnl'] += insider.get('pnl_usd', 0)
        wallet_stats[w]['total_roi'] += insider.get('roi_pct', 0)

        # Check if this is a sniper
        if insider.get('is_sniper', False):
            wallet_stats[w]['is_sniper'] = True

        wallet_stats[w]['markets'].append({
            'slug': insider.get('slug', ''),
            'market_title': insider.get('market_title', ''),
            'outcome': insider.get('outcome', ''),
            'roi_pct': insider.get('roi_pct', 0),
            'pnl_usd': insider.get('pnl_usd', 0),
            'entry_price': insider.get('entry_price', 0),
            'usd_size': insider.get('usd_size', 0),
            'time_to_pump_mins': insider.get('time_to_pump_mins'),
            'original_count': insider.get('original_count', 1)
        })

    # Sort by total PnL
    top_wallets = sorted(
        wallet_stats.items(),
        key=lambda x: x[1]['total_pnl'],
        reverse=True
    )[:10]

    # ==========================================================
    # 🔧 FIX: Fetch LIVE predictions count - MANDATORY API CALL
    # ==========================================================
    # No fallback to stale DB data - if API fails, we show N/A or skip wallet
    if not session:
        log("  → ERROR: session required for live predictions - cannot send report")
        return

    log(f"  → Fetching live predictions count for {len(top_wallets)} Retro Insider wallets...")
    skipped_api_error_count = 0
    
    for wallet, stats in top_wallets:
        try:
            live_preds = await get_polymarket_predictions(session, wallet)
            stats['live_predictions'] = live_preds
            stats['api_fetch_failed'] = False
            log(f"    {wallet[:10]}...: {live_preds} predictions (live)")
        except Exception as e:
            log(f"    FAILED to fetch predictions for {wallet[:10]}...: {e}")
            # Mark as failed - will show N/A in report
            stats['live_predictions'] = None
            stats['api_fetch_failed'] = True
            skipped_api_error_count += 1

    # Filter out wallets with failed API calls from top display
    # (We keep them in stats but mark them as failed)
    top_wallets_display = [(w, s) for w, s in top_wallets if not s.get('api_fetch_failed', False)]
    
    # If all wallets failed API, still show them with N/A
    if not top_wallets_display:
        top_wallets_display = top_wallets

    # Build message
    msg = f"🔍 <b>RETRO INSIDER REPORT</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"📊 Markets scanned: {markets_scanned}\n"
    msg += f"🎯 Total insiders found: {len(insiders)}\n"
    msg += f"👛 Unique wallets: {len(wallet_stats)}\n"
    if skipped_api_error_count > 0:
        msg += f"⚠️ API errors: {skipped_api_error_count} wallets (showing N/A)\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

    for i, (wallet, stats) in enumerate(top_wallets_display, 1):
        wallet_short = format_wallet_short(wallet)
        profile_link = f"https://polymarket.com/profile/{wallet}"
        arkham_link = f"https://intel.arkm.com/explorer/address/{wallet}"
        avg_roi = stats['total_roi'] / stats['count'] if stats['count'] > 0 else 0
        pnl_str = format_pnl(stats['total_pnl'], avg_roi)

        # FIX: Use LIVE predictions count from API - show N/A if failed
        if stats.get('api_fetch_failed', False):
            preds = "N/A"
        else:
            preds = stats.get('live_predictions', 0)

        # Sniper badge
        sniper_badge = "🎯 " if stats['is_sniper'] else ""

        # Show TOP 3 most profitable markets (sorted by ROI)
        # TASK 3: Markets are now aggregated - no duplicate wallet+market
        sorted_markets = sorted(stats['markets'], key=lambda x: x['roi_pct'], reverse=True)[:3]

        market_lines = []
        for m in sorted_markets:
            market_title = m['market_title'][:40] if m['market_title'] else m['slug'][:40]
            market_link = f"https://polymarket.com/event/{m['slug']}"

            # Sniper indicator
            sniper_indicator = ""
            if m.get('time_to_pump_mins'):
                sniper_indicator = f" ⚡{m['time_to_pump_mins']:.0f}m"

            # TASK 3: Show aggregated data - weighted avg entry, summed PnL/size
            market_lines.append(
                f"   📍 <a href='{market_link}'>{market_title}</a> | {m['outcome']}\n"
                f"      <b>+{m['roi_pct']:.0f}%</b> | ${m['pnl_usd']:,.0f} | Entry: {m['entry_price']:.3f}{sniper_indicator}"
            )

        market_str = "\n".join(market_lines)

        msg += (
            f"{i}. {sniper_badge}<a href='{profile_link}'>{wallet_short}</a> | <a href='{arkham_link}'>Arkham</a>\n"
            f"   📊 <b>Preds: {preds}</b> | 🎯 Markets: {stats['count']} | 💰 PnL: {pnl_str}\n"
            f"{market_str}\n\n"
        )

    msg += "━━━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "<i>Retro Insider Bot - Closed Markets Analysis</i>"

    await send_telegram(msg)
    log(f"  → Sent Retro Insider report with {len(insiders)} insiders from {len(top_wallets)} wallets")


# ==========================================
# 📊 REPORT GENERATION
# ==========================================
# NOTE: send_retro_insider_alert() deprecated - use send_retro_insider_report() instead
# send_daily_top_insiders() deprecated - use send_retro_insider_report() instead

async def send_retro_insider_alert(wallet: str, market_title: str, outcome: str,
                                    entry_price: float, usd_size: float,
                                    roi_pct: float, pnl_usd: float, is_sniper: bool = False):
    """
    Send Telegram alert for individual retro insider detection.
    """
    from config import format_wallet_short, format_pnl
    
    wallet_short = format_wallet_short(wallet)
    profile_link = f"https://polymarket.com/profile/{wallet}"
    arkham_link = f"https://intel.arkm.com/explorer/address/{wallet}"
    pnl_str = format_pnl(pnl_usd, roi_pct)

    alert_type = "🎯 RETRO SNIPER" if is_sniper else "🔍 RETRO INSIDER"

    msg = (
        f"{alert_type} DETECTED\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>Wallet:</b> <a href='{profile_link}'>{wallet_short}</a>\n"
        f"🔗 <a href='{arkham_link}'>Arkham</a>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>PnL:</b> {pnl_str}\n"
        f"📊 <b>ROI:</b> +{roi_pct:.0f}%\n"
        f"💵 <b>Size:</b> ${usd_size:,.0f}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"❓ <b>Market:</b> {market_title}\n"
        f"🎯 <b>Position:</b> {outcome} @ {entry_price:.3f}\n"
        f"━━━━━━━━━━━━━━����━━━━━��━━\n"
        f"<code>{wallet}</code>\n"
        f"<i>{'Entry <30min before pump!' if is_sniper else 'High ROI trader on closed market!'}</i>"
    )

    await send_telegram(msg)


async def send_daily_top_insiders(insiders: List[Dict], date: str):
    """
    Send "Daily Top Insiders" report to Telegram.
    """
    from config import format_wallet_short, format_pnl
    
    if not insiders:
        return

    wallet_stats = {}
    for insider in insiders:
        w = insider['wallet']
        if w not in wallet_stats:
            wallet_stats[w] = {
                'count': 0,
                'total_pnl': 0,
                'total_roi': 0,
                'reasons': [],
                'is_sniper': False
            }
        wallet_stats[w]['count'] += 1
        wallet_stats[w]['total_pnl'] += insider.get('pnl_usd', 0)
        wallet_stats[w]['total_roi'] += insider.get('roi_pct', 0)

        if 'time_to_pump_mins' in insider:
            wallet_stats[w]['reasons'].append(f"Sniped {insider['time_to_pump_mins']:.1f}m early")
            wallet_stats[w]['is_sniper'] = True
        elif 'roi_pct' in insider:
            wallet_stats[w]['reasons'].append(f"ROI +{insider['roi_pct']:.0f}%")

    top_wallets = sorted(
        wallet_stats.items(),
        key=lambda x: x[1]['total_pnl'],
        reverse=True
    )[:10]

    msg = f"🏆 <b>DAILY TOP INSIDERS</b> ({date})\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

    for i, (wallet, stats) in enumerate(top_wallets, 1):
        wallet_short = format_wallet_short(wallet)
        arkham_link = f"https://intel.arkm.com/explorer/address/{wallet}"
        avg_roi = stats['total_roi'] / stats['count'] if stats['count'] > 0 else 0
        reasons_str = ", ".join(stats['reasons'][:2])
        sniper_badge = "🎯 " if stats['is_sniper'] else ""

        msg += (
            f"{i}. {sniper_badge}<a href='https://polymarket.com/profile/{wallet}'>{wallet_short}</a> | <a href='{arkham_link}'>Arkham</a>\n"
            f"   🎯 Hits: {stats['count']} | 💰 PnL: {format_pnl(stats['total_pnl'], avg_roi)}\n"
            f"   📍 {reasons_str}\n\n"
        )

    msg += "━━━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "<i>Generated by Polymarket Insider Bot</i>"

    await send_telegram(msg)


async def send_sniper_alert(session, conn, sniper: Dict, market_title: str):
    """
    Send alert for individual sniper detection.
    """
    from config import format_wallet_short, format_pnl

    wallet = sniper['wallet']
    wallet_short = format_wallet_short(wallet)
    profile_link = f"https://polymarket.com/profile/{wallet}"
    arkham_link = f"https://intel.arkm.com/explorer/address/{wallet}"

    total_trades_cursor = await conn.execute(
        "SELECT total_trades FROM trader_performance WHERE wallet_addr = ?",
        (wallet,)
    )
    total_trades_result = await total_trades_cursor.fetchone()
    total_trades = total_trades_result[0] if total_trades_result else 0

    if total_trades == 0:
        total_trades = await get_polymarket_predictions(session, wallet)

    preds_str = str(total_trades) if total_trades > 0 else "0"

    msg = (
        f"🎯 <b>RETRO SNIPER DETECTED</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏱ <b>Time Before Pump:</b> {sniper['time_to_pump_mins']:.1f} minutes\n"
        f"📈 <b>Pump Gain:</b> +{sniper['pump_gain_pct']:.0f}%\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 <b>Wallet:</b> <a href='{profile_link}'>{wallet_short}</a> | <a href='{arkham_link}'>Arkham</a>\n"
        f"📊 <b>Preds:</b> {preds_str}\n"
        f"💰 <b>Size:</b> ${sniper['usd_size']:,.0f}\n"
        f"📊 <b>PnL:</b> {format_pnl(sniper['pnl_usd'], sniper['pump_gain_pct'])}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"❓ <b>Market:</b> {market_title}\n"
        f"🎯 <b>Position:</b> {sniper['outcome']} @ {sniper['entry_price']:.3f}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<code>{wallet}</code>"
    )

    await send_telegram(msg)


# ==========================================
# 🔄 MAIN WORKER
# ==========================================

async def scan_market(session, conn, market: Dict) -> List[Dict]:
    """
    Scan single closed market for insider patterns using DB signals.
    """
    slug = market.get('slug')

    winner = None

    if market.get('winningOutcome'):
        winner = str(market.get('winningOutcome')).upper().strip()
        log(f"  → Winner from winningOutcome field: {winner}")

    if not winner:
        outcomes = market.get('outcomes', [])
        outcome_prices = market.get('outcomePrices', []) or market.get('prices', [])

        if outcomes and outcome_prices and len(outcomes) == len(outcome_prices):
            log(f"  → Checking outcomePrices: {outcome_prices}")
            for i, price in enumerate(outcome_prices):
                try:
                    price_float = float(price)
                    if price_float >= 0.99:
                        winner = str(outcomes[i]).upper().strip()
                        log(f"  → Winner determined from outcomePrices: {winner} (price={price_float})")
                        break
                except (ValueError, TypeError) as e:
                    log(f"  → Error parsing price: {e}")

    if not winner:
        tokens = market.get('tokens', [])
        log(f"  → Checking {len(tokens)} tokens for winner...")
        for t in tokens:
            if isinstance(t, dict):
                if t.get('winner') is True:
                    winner = str(t.get('outcome', '')).upper().strip()
                    log(f"  → Winner from token winner=True: {winner}")
                    break
                try:
                    t_price = float(t.get('price', 0))
                    if t_price >= 0.99:
                        winner = str(t.get('outcome', '')).upper().strip()
                        log(f"  → Winner from token price={t_price}: {winner}")
                        break
                except (ValueError, TypeError):
                    pass

    close_ts = market.get('endTimestamp') or market.get('closeTime') or int(time.time())

    if close_ts > 1e12:
        close_ts = close_ts // 1000

    if not slug:
        log(f"  → Skipping: no slug")
        return []

    if not winner:
        log(f"  → Skipping: could not determine winner")
        return []

    log(f"  → Scanning market: {slug[:50]}... | Winner: {winner}")

    insiders = []
    market_title = market.get('title', slug)

    clob_ids = {}
    tokens = market.get('tokens', [])

    for t in tokens:
        outcome = t.get('outcome', '').upper()
        token_id = t.get('clobTokenId') or t.get('clob_token_id') or t.get('clobTokenIds')
        if outcome and token_id:
            clob_ids[outcome] = token_id

    if not clob_ids:
        condition_id = market.get('conditionId') or market.get('condition_id')
        outcomes = market.get('outcomes', [])

        if condition_id and outcomes:
            log(f"  → Building CLOB IDs from conditionId: {condition_id[:20]}...")
            for i, outcome in enumerate(outcomes):
                clob_id = f"{condition_id}_{outcome}".replace(' ', '_')
                clob_ids[outcome.upper()] = clob_id
            log(f"  → Built CLOB IDs: {list(clob_ids.keys())}")

    log(f"  → Checking for high ROI traders...")
    high_roi = await detect_high_roi_traders(conn, slug, winner)
    if high_roi:
        log(f"  → Found {len(high_roi)} high ROI traders from DB")
        insiders.extend(high_roi)
    else:
        log(f"  → No high ROI traders found for winner '{winner}'")

    if not clob_ids:
        log(f"  → Skipping sniper detection (no CLOB IDs available)")
    else:
        log(f"  → Scanning DB for sniper entries...")
        snipers = await detect_sniper_entries(
            session, conn, slug, clob_ids, winner
        )

        seen = set()
        unique_snipers = []
        for s in snipers:
            key = f"{s['wallet']}_{s['outcome']}"
            if key not in seen:
                seen.add(key)
                unique_snipers.append(s)

        if unique_snipers:
            log(f"  → Found {len(unique_snipers)} snipers from DB")
            insiders.extend(unique_snipers)
        else:
            log(f"  → No snipers detected")

    return insiders


async def run_oracle_pulse():
    """
    🚀 V16 API-FIRST MODE: Oracle Pulse Scan with API-verified PnL/ROI.
    Runs every 6 hours.

    API-First:
    - Uses Polymarket Data API for ALL position data (avgPrice, size, currentValue)
    - Fetches LIVE predictions count from API for report
    - No SQL-based PnL calculations

    🛡️ FILTERS:
    - Minimum lifetime trades >= 5 (anti-Sybil)
    - Minimum position size >= ORACLE_MIN_USD_SIZE ($2000)
    - ROI > ROI_THRESHOLD (50%)
    """
    log("📡 Starting Oracle Pulse Scan...")
    start_time = time.time()

    conn = await get_async_db_connection()

    try:
        async with aiohttp.ClientSession() as session:
            positions = await scan_open_positions(conn, session)

            # FIX: Send report INSIDE session block - session must be open for API calls
            if positions:
                await send_oracle_pulse_report(positions, session)

                for pos in positions:
                    try:
                        await conn.execute("""
                            INSERT INTO trader_performance
                            (wallet_addr, is_insider, insider_reason, last_updated)
                            VALUES (?, 1, ?, ?)
                            ON CONFLICT(wallet_addr) DO UPDATE SET
                                is_insider = 1,
                                insider_reason = excluded.insider_reason,
                                last_updated = excluded.last_updated
                        """, (
                            pos['wallet'],
                            f"Oracle Pulse: +{pos['roi_pct']:.0f}% (${pos['pnl_usd']:,.0f}) on {pos['outcome']}",
                            int(time.time())
                        ))
                    except Exception as db_err:
                        log(f"  → Failed to update trader_performance: {db_err}")

                await conn.commit()
            else:
                log("  → No profitable open positions found")
    except Exception as e:
        log(f"Oracle Pulse error: {e}")
    finally:
        await conn.close()

    elapsed = time.time() - start_time
    log(f"Oracle Pulse completed in {elapsed:.1f}s")


async def run_retro_scan():
    """
    Main scan loop.
    Runs every 12 hours.
    """
    log("🔍 Starting Retro Sniper Scan...")
    start_time = time.time()

    conn = await get_async_db_connection()
    all_insiders = []

    try:
        async with aiohttp.ClientSession() as session:
            # ==========================================
            # 🔧 FIX: Sync market_resolution_cache with market_cache
            # If a market is in market_cache (resolved=1) but not in market_resolution_cache,
            # add it to market_resolution_cache so we can process it
            # ==========================================
            log("Syncing market_resolution_cache with market_cache...")
            sync_cursor = await conn.execute("""
                INSERT OR IGNORE INTO market_resolution_cache (market_slug, is_closed, last_checked)
                SELECT m.slug, 1, strftime('%s', 'now')
                FROM market_cache m
                WHERE m.resolved = 1
                  AND m.slug NOT IN (SELECT mr.market_slug FROM market_resolution_cache mr WHERE mr.is_closed = 1)
            """)
            sync_result = sync_cursor
            await conn.commit()
            log(f"  → Synced {sync_result.rowcount} markets from market_cache")

            # ==========================================
            # 🔧 FIX: Update winner_outcome from market_cache
            # Copy winner from market_cache to market_resolution_cache for consistency
            # ==========================================
            log("Updating winner_outcome from market_cache...")
            update_cursor = await conn.execute("""
                UPDATE market_resolution_cache
                SET winner_outcome = (
                    SELECT m.winner
                    FROM market_cache m
                    WHERE m.slug = market_resolution_cache.market_slug
                      AND m.winner IS NOT NULL
                      AND m.winner != ''
                )
                WHERE winner_outcome IS NULL OR winner_outcome = ''
                  AND EXISTS (
                      SELECT 1 FROM market_cache m
                      WHERE m.slug = market_resolution_cache.market_slug
                        AND m.resolved = 1
                        AND m.winner IS NOT NULL
                        AND m.winner != ''
                  )
            """)
            update_result = update_cursor
            await conn.commit()
            log(f"  → Updated {update_result.rowcount} winners from market_cache")

            log("Checking local market_resolution_cache for closed markets...")
            local_closed_cursor = await conn.execute("""
                SELECT DISTINCT mrc.market_slug as slug,
                       COALESCE(s.market_title, mrc.market_slug) as market_title
                FROM market_resolution_cache mrc
                LEFT JOIN signals s ON mrc.market_slug = s.slug
                WHERE mrc.is_closed = 1
                  AND mrc.market_slug NOT IN (SELECT slug FROM market_cache WHERE resolved = 1)
            """)
            local_closed = await local_closed_cursor.fetchall()

            log(f"Found {len(local_closed)} closed markets in local cache to process")

            log("Fetching recently closed markets from Gamma API (last 5000)...")
            api_closed_markets = await fetch_all_closed_markets(session)
            api_closed_lookup = {m.get('slug'): m for m in api_closed_markets if m.get('slug')}
            log(f"Got {len(api_closed_lookup)} closed markets from API")

            markets_to_scan = []
            processed_slugs = set()

            for row in local_closed:
                slug = row['slug']
                if slug in processed_slugs:
                    continue
                processed_slugs.add(slug)

                cache_cursor = await conn.execute(
                    "SELECT winner_outcome FROM market_resolution_cache WHERE market_slug = ?", (slug,)
                )
                cache_row = await cache_cursor.fetchone()
                cached_winner = cache_row['winner_outcome'] if cache_row and cache_row['winner_outcome'] else None

                if slug in api_closed_lookup:
                    market_data = api_closed_lookup[slug]
                    if cached_winner and not market_data.get('winningOutcome'):
                        market_data['winningOutcome'] = cached_winner
                        log(f"  → Using cached winner for {slug[:40]}...: {cached_winner}")
                    markets_to_scan.append({
                        'slug': slug,
                        'title': row['market_title'] or slug,
                        'data': market_data,
                        'source': 'api'
                    })
                else:
                    market_data = {'slug': slug, 'winningOutcome': cached_winner} if cached_winner else None
                    markets_to_scan.append({
                        'slug': slug,
                        'title': row['market_title'] or slug,
                        'data': market_data,
                        'source': 'local',
                        'cached_winner': cached_winner
                    })

            for slug, market_data in api_closed_lookup.items():
                if slug in processed_slugs:
                    continue

                has_signals_cursor = await conn.execute(
                    "SELECT 1 FROM signals WHERE slug = ? LIMIT 1", (slug,)
                )
                has_signals = await has_signals_cursor.fetchone()

                if has_signals:
                    already_processed_cursor = await conn.execute(
                        "SELECT 1 FROM market_cache WHERE slug = ? AND resolved = 1", (slug,)
                    )
                    already_processed = await already_processed_cursor.fetchone()

                    if not already_processed:
                        processed_slugs.add(slug)
                        markets_to_scan.append({
                            'slug': slug,
                            'title': market_data.get('title', slug),
                            'data': market_data,
                            'source': 'api'
                        })

            # ==========================================
            # 🔧 FIX: Check markets with signals that may be closed but not in API list
            # Query Gamma API directly for markets that have signals but aren't resolved
            # ==========================================
            log("Checking markets with signals for closure status...")
            markets_with_signals_cursor = await conn.execute("""
                SELECT DISTINCT slug, COUNT(*) as signal_count
                FROM signals
                WHERE slug NOT IN (SELECT slug FROM market_cache WHERE resolved = 1)
                GROUP BY slug
                HAVING COUNT(*) >= 10  -- Only check markets with meaningful activity
                ORDER BY signal_count DESC
                LIMIT 50  -- Check top 50 markets by signal count
            """)
            markets_with_signals = await markets_with_signals_cursor.fetchall()

            log(f"  → Found {len(markets_with_signals)} markets with signals to check")

            for row in markets_with_signals:
                slug = row['slug']
                if slug in processed_slugs:
                    continue

                # Check if already in API closed list
                if slug in api_closed_lookup:
                    continue

                # Fetch single market to check if closed
                market_info = await fetch_single_market(session, slug)
                if market_info and market_info.get('closed'):
                    processed_slugs.add(slug)
                    log(f"  → Found closed market with signals: {slug[:40]}... ({row['signal_count']} signals)")
                    markets_to_scan.append({
                        'slug': slug,
                        'title': market_info.get('title', slug),
                        'data': market_info,
                        'source': 'api'
                    })

            log(f"Total markets to scan: {len(markets_to_scan)}")

            if len(markets_to_scan) == 0:
                log("No new closed markets to scan. All markets processed.")
                await conn.close()
                return

            for i, market in enumerate(markets_to_scan, 1):
                slug = market['slug']
                log(f"[{i}/{len(markets_to_scan)}] Scanning: {slug[:50]}... (Source: {market['source']})")

                market_data = market['data']
                cached_winner = market.get('cached_winner')

                if not market_data:
                    log(f"  → Fetching market data from API...")
                    market_data = await fetch_single_market(session, slug)
                    if not market_data:
                        log(f"  → Failed to fetch market data, skipping")
                        continue

                if not market_data.get('closed'):
                    log(f"  → Market is not closed according to API, skipping")
                    await conn.execute("""
                        UPDATE market_resolution_cache
                        SET is_closed = 0, last_checked = ?
                        WHERE market_slug = ?
                    """, (int(time.time()), slug))
                    await conn.commit()
                    continue

                winner = None
                if cached_winner:
                    winner = str(cached_winner).upper().strip()
                    log(f"  → Using cached winner: {winner}")

                if not winner and market_data.get('winningOutcome'):
                    winner = str(market_data.get('winningOutcome')).upper().strip()
                    log(f"  → Winner from winningOutcome field: {winner}")

                if not winner:
                    winner = detect_market_winner(market_data)
                    if winner:
                        log(f"  → Winner from detect_market_winner: {winner}")

                if not winner:
                    log(f"  → No winner determined, skipping")
                    continue

                log(f"  → Winner determined: {winner}")
                market_data['winningOutcome'] = winner

                try:
                    insiders = await scan_market(session, conn, market_data)

                    if insiders:
                        log(f"  → Found {len(insiders)} insiders")
                        all_insiders.extend(insiders)

                        for sniper in [x for x in insiders if 'time_to_pump_mins' in x]:
                            existing_cursor = await conn.execute(
                                "SELECT wallet_addr FROM trader_performance WHERE wallet_addr = ? AND is_insider = 1",
                                (sniper['wallet'],)
                            )
                            existing = await existing_cursor.fetchone()

                            if not existing:
                                market_title = market_data.get('title', slug)
                                await send_sniper_alert(session, conn, sniper, market_title)
                                await asyncio.sleep(1)
                            else:
                                log(f"    → Skipping sniper alert for {sniper['wallet'][:10]}... (already in DB)")

                    try:
                        await conn.execute("""
                            INSERT OR REPLACE INTO market_cache (slug, resolved, winner, last_checked)
                            VALUES (?, 1, ?, ?)
                        """, (slug, winner, int(time.time())))
                    except Exception as cache_err:
                        log(f"  → Failed to update market_cache: {cache_err}")

                except Exception as e:
                    log(f"  → Error: {e}")

                await asyncio.sleep(0.5)

            for insider in all_insiders:
                try:
                    if 'time_to_pump_mins' in insider:
                        reason = f"Sniper: {insider['time_to_pump_mins']:.1f}m early"
                    else:
                        reason = f"High ROI: +{insider.get('roi_pct', 0):.0f}% (${insider.get('pnl_usd', 0):,.0f})"

                    await conn.execute("""
                        INSERT INTO trader_performance
                        (wallet_addr, is_insider, insider_reason, last_updated)
                        VALUES (?, 1, ?, ?)
                        ON CONFLICT(wallet_addr) DO UPDATE SET
                            is_insider = 1,
                            insider_reason = CASE
                                WHEN trader_performance.insider_reason LIKE '%Sniper%'
                                THEN trader_performance.insider_reason
                                ELSE excluded.insider_reason
                            END,
                            last_updated = CASE
                                WHEN excluded.insider_reason LIKE '%Sniper%'
                                THEN excluded.last_updated
                                ELSE trader_performance.last_updated
                            END
                    """, (
                        insider['wallet'],
                        reason,
                        int(time.time())
                    ))
                except Exception as db_err:
                    log(f"  → Failed to update trader_performance: {db_err}")

            await conn.commit()
    finally:
        await conn.close()

    if all_insiders:
        # Send aggregated Retro Insider report instead of spamming individual alerts
        # FIX: Pass session to fetch LIVE predictions from Polymarket Data API
        await send_retro_insider_report(all_insiders, markets_scanned=len(markets_to_scan), session=session)
        log(f"Sent Retro Insider report with {len(all_insiders)} insiders from {len(all_insiders)} traders")

    elapsed = time.time() - start_time
    log(f"Scan completed in {elapsed:.1f}s")


async def main():
    """
    Main entry point with STATEFUL CRON logic.
    Uses database-backed state management to track last run times.
    Oracle Pulse: Runs every 6 hours | Retro Sniper: Runs every 12 hours
    """
    # First things first — check the environment
    from config import check_required_env
    check_required_env()

    log("🔍 Retro Sniper Worker V16 (Stateful Cron) Started")
    log("📡 Oracle Pulse Enabled - API Verified PnL/ROI - Scans every 6 hours")
    log("=" * 60)
    log(f"Retro scan interval: {SCAN_INTERVAL_HOURS} hours")
    log(f"Oracle Pulse interval: {ORACLE_PULSE_INTERVAL_HOURS} hours")
    log(f"ROI threshold: >{ROI_THRESHOLD}%")
    log(f"Min USD size: >=${MIN_USD_SIZE}")
    log(f"Oracle Min USD size: >=${ORACLE_MIN_USD_SIZE}")
    log(f"Pump threshold: >{PUMP_THRESHOLD*100}%")
    log(f"Snipe window: <{SNIPE_WINDOW_MINS} minutes")
    log(f"Bot filter: RETRO_MAX_TRADES = {RETRO_MAX_TRADES}")
    log("=" * 60)

    # Define intervals in seconds
    ORACLE_INTERVAL = ORACLE_PULSE_INTERVAL_HOURS * 3600  # 6 hours
    RETRO_INTERVAL = SCAN_INTERVAL_HOURS * 3600  # 12 hours

    conn = await get_async_db_connection()

    while True:
        try:
            current_time = int(time.time())

            # ==========================================
            # ORACLE PULSE CHECK (Every 6 hours)
            # ==========================================
            oracle_last_run = await get_cron_state_async(conn, 'oracle_pulse')
            time_since_oracle = current_time - oracle_last_run

            if time_since_oracle >= ORACLE_INTERVAL:
                # 🔒 FILE-BASED LOCK: Prevent PM2 race condition
                if acquire_lock('oracle_pulse'):
                    log(f"📡 Running Oracle Pulse (last run: {time_since_oracle // 3600}h ago) [LOCK ACQUIRED]")
                    try:
                        # Update DB state as secondary lock
                        await set_cron_state_async(conn, 'oracle_pulse', current_time)
                        log(f"[STATE] last_run set to {current_time}")
                        await run_oracle_pulse()
                        log(f"[STATE] Successfully completed oracle_pulse scan")
                        log(f"✅ Oracle Pulse completed, next run in {ORACLE_PULSE_INTERVAL_HOURS}h")
                    except Exception as e:
                        log(f"❌ Oracle Pulse failed: {e}")
                    finally:
                        # Always release lock, even if scan crashes
                        release_lock('oracle_pulse')
                        log(f"[LOCK] Released oracle_pulse lock")
                else:
                    log(f"⏭️ Oracle Pulse skipped - locked by another process")
            else:
                remaining = ORACLE_INTERVAL - time_since_oracle
                log(f"⏱ Oracle Pulse: Next run in {remaining // 60}m")

            # ==========================================
            # RETRO SNIPER CHECK (Every 12 hours)
            # ==========================================
            retro_last_run = await get_cron_state_async(conn, 'retro_scan')
            time_since_retro = current_time - retro_last_run

            if time_since_retro >= RETRO_INTERVAL:
                # 🔒 FILE-BASED LOCK: Prevent PM2 race condition
                if acquire_lock('retro_scan'):
                    log(f"🔍 Running Retro Sniper Scan (last run: {time_since_retro // 3600}h ago) [LOCK ACQUIRED]")
                    try:
                        # Update DB state as secondary lock
                        await set_cron_state_async(conn, 'retro_scan', current_time)
                        log(f"[STATE] last_run set to {current_time}")
                        await run_retro_scan()
                        log(f"[STATE] Successfully completed retro_scan scan")
                        log(f"✅ Retro Sniper completed, next run in {SCAN_INTERVAL_HOURS}h")
                    except Exception as e:
                        log(f"❌ Retro Sniper failed: {e}")
                    finally:
                        # Always release lock, even if scan crashes
                        release_lock('retro_scan')
                        log(f"[LOCK] Released retro_scan lock")
                else:
                    log(f"⏭️ Retro Sniper skipped - locked by another process")
            else:
                remaining = RETRO_INTERVAL - time_since_retro
                log(f"⏱ Retro Scan: Next run in {remaining // 60}m")

            # ==========================================
            # SLEEP: Check clock every 60 seconds (like a real cron)
            # ==========================================
            await asyncio.sleep(60)

        except Exception as e:
            crash_log("retro_worker cron_loop", e, traceback.format_exc())
            await asyncio.sleep(60)
        finally:
            # Keep connection open for next iteration
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("Stopped by user")