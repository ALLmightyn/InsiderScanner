
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
import traceback
from typing import Optional, Dict, Tuple, List, Any
import aiohttp
import aiofiles
import aiosqlite
from datetime import datetime, timezone
import alert_manager

# ==========================================
# ⚙️ IMPORT CONFIG - V12 STANDARD
# ==========================================
from config import (
    BOT_TRADE_THRESHOLD,
    SLOW_CLUSTER_MIN_USD,
    SLOW_CLUSTER_MIN_WALLETS,
    INSTANT_ATTACK_MIN_USD,
    FRESH_CLUSTER_MIN_TRADES,
    FRESH_CLUSTER_MIN_USD,
    DORMANT_THRESHOLD_BETS,
    normalize_outcome,
    TOKEN_CONTRACT,
    NULL_ADDR,
    POLYMARKET_CONTRACTS,
    execute_query_with_retry,
    execute_query_with_retry_async,
    format_pnl,
    get_async_db_connection,
    crash_log,
    get_latest_sync_block_async,
    redis_client,
    polygonscan_limiter,
    get_polymarket_predictions,
    BASE_DIR,
    DB_FILENAME,
    CUSTOM_LABELS_FILE,
)

# NOTE: POLYMARKET_CONTRACTS is now imported from config.py (central registry)

# --- API CONFIG ---
POLYGONSCAN_API_KEY = os.getenv("POLYGONSCAN_API_KEY")
V2_API_URL = "https://api.etherscan.io/v2/api"
POLYMARKET_GAMMA_API = "https://gamma-api.polymarket.com/markets"
CHAIN_ID = "137"
USDC_CONTRACT = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"

# --- AI FILTER CONFIG ---
# SPRINT 2: AI filter now uses markets_whitelist table (market_discovery.py)
# Import USE_AI_FILTER from config instead of defining locally
from config import USE_AI_FILTER

BATCH_SIZE = 50
MAX_DEPTH = 5

# ==========================================
# 📊 TASK 6: GLOBAL COUNTER FOR HARD TIME FILTER
# ==========================================
_signals_skipped_count = 0

def get_skipped_signals_count():
    """Returns the total number of signals skipped due to the Hard Time Filter."""
    return _signals_skipped_count

def increment_skipped_signals_count():
    """Increments the skipped signals counter."""
    global _signals_skipped_count
    _signals_skipped_count += 1

# ==========================================
# 🛡️ IN-MEMORY DEDUPLICATION - Prevent concurrent processing of same signal
# ==========================================
PROCESSING_NOW = set()

def get_processing_count():
    """Returns the number of signals currently being processed."""
    return len(PROCESSING_NOW)

# Базовые адреса (чтобы работало даже без файла)
KNOWN_CONTRACTS = {
    "cex_wallets": {
        "0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be": "Binance: Hot Wallet",
        "0x503828976d22510aad0201ac7ec88293211d23da": "Kraken",
        "0x71660c4005ba85c37ccec55d0c4493e66fe775d3": "Coinbase",
        "0x5f6dc3714cad3f972049e3e07293a38c2813524e": "OKX",
        "0x4b16c5de96eb2117bbe5fd171e4d203624b014aa": "Bitget",
        "0x1f9090aae28b8a3dceadf281b0f12828e676c326": "Gate.io",
    },
    "mixers": {
        "0x722122df12d4e14e13ac3b6895a86e84145b6967": "Tornado Cash",
    },
    "bridges": {
        "0x40ec5b33f54e0e8a33a975908c5ba1c14e5bbbdf": "Polygon Bridge",
        "0xf70da97812cb96acdf810712aa562db8dfa3dbef": "Relay.link",
    }
}

# --- DATABASE HELPERS ---
def log(msg):
    t = datetime.now().strftime("%H:%M:%S")
    print(f"[{t}] {msg}")



# --- CUSTOM LABELS LOADER (Async version) ---
async def load_all_labels_async():
    """Объединяет встроенные метки и файл custom_labels.json (async version)"""
    combined = {}

    # 1. Загружаем встроенные
    for cat, items in KNOWN_CONTRACTS.items():
        for addr, name in items.items():
            combined[addr.lower()] = {"cat": cat.capitalize().rstrip("s"), "name": name}

    # 2. Загружаем файл асинхронно (перезаписывает встроенные, если совпадает)
    if os.path.exists(CUSTOM_LABELS_FILE):
        if os.path.getsize(CUSTOM_LABELS_FILE) > 0:
            try:
                async with aiofiles.open(CUSTOM_LABELS_FILE, 'r', encoding='utf-8') as f:
                    content = await f.read()
                    custom = json.loads(content)
                    for addr, name in custom.items():
                        cat = "CEX" if "Binance" in name or "Exchange" in name else "Custom"
                        combined[addr.lower()] = {"cat": cat, "name": name}
            except json.JSONDecodeError as e:
                log(f"Warning: custom_labels.json is malformed, using empty labels: {e}")
            except Exception as e:
                log(f"Error loading custom labels: {e}")

    return combined

# Global cache - will be populated in main()
LABELS_CACHE = {}


# ==========================================
# 🕸️ PHASE 3: CLUSTER ANALYSIS (V12 STANDARD)
# ==========================================

async def get_cluster_siblings_for_market(conn, funder_address: str, exclude_wallet: str, market_slug: str, target_outcome: str = None) -> List[Dict]:
    """
    Phase 3 V12: Finds all wallets funded by the same source THAT BET ON THE SAME MARKET.

    CRITICAL FIXES:
    1. Outcome Matching: Uses LOWER(REPLACE(REPLACE(outcome, '🟢 ', ''), '🔴 ', ''))
    2. Wash Trade Exclusion: Excludes wallets if COUNT(DISTINCT outcome) > 1
    3. Live Trade Count: Uses MAX(s.predictions) from signals table
    4. Optional outcome filter: If target_outcome provided, only returns siblings who bet on same outcome
    5. FIX: Removed DATA_CLEANUP_CUTOFF - don't block old cluster signals
    6. TASK 1: LIVE MODE - Only looks at last 60 minutes
    7. TASK 2: Use Unix timestamp comparison to eliminate timezone issues

    Returns list of dicts with wallet data sorted by volume DESC.
    """
    # Normalize target outcome for matching
    outcome_normalized = normalize_outcome(target_outcome) if target_outcome else None

    # 🔍 OUTCOME NORMALIZATION AUDIT: Debug logging for emoji encoding issues
    if target_outcome:
        log(f"[OUTCOME AUDIT] Original: '{target_outcome}' | Normalized: '{outcome_normalized}' | Slug: {market_slug[:40]}...")

    # TASK 2: Calculate Unix timestamp for 60 minutes ago
    current_unix_time = int(time.time())
    sixty_minutes_ago = current_unix_time - 3600

    if outcome_normalized:
        # Filter by specific outcome (for coordinated attack detection)
        # TASK 1: LIVE MODE - Only look at last 60 minutes
        # TASK 1: Added usd_size >= 100 filter to remove "dust" bets ($5 ladder patterns)
        # TASK 2: Use CAST(timestamp AS INTEGER) >= (? - 3600) for timezone-safe comparison
        query = """
            SELECT
                fs.wallet_addr,
                fs.source_label,
                fs.funding_ts,
                COALESCE(market_vol.total_volume, 0) as total_volume,
                COALESCE(market_vol.max_predictions, 0) as total_trades,
                COALESCE(market_vol.outcome_count, 0) as outcome_count
            FROM funding_sources fs
            LEFT JOIN (
                SELECT
                    trader_addr,
                    SUM(usd_size) as total_volume,
                    MAX(predictions) as max_predictions,
                    COUNT(DISTINCT LOWER(REPLACE(REPLACE(outcome, '🟢 ', ''), '🔴 ', ''))) as outcome_count
                FROM signals
                WHERE slug = ?
                  AND LOWER(REPLACE(REPLACE(outcome, '🟢 ', ''), '🔴 ', '')) = ?
                  AND usd_size >= 100
                  AND CAST(timestamp AS INTEGER) >= ?
                GROUP BY trader_addr
                HAVING outcome_count <= 1
            ) market_vol ON fs.wallet_addr = market_vol.trader_addr
            WHERE fs.funder_address = ?
              AND fs.wallet_addr != ?
              AND EXISTS (
                  SELECT 1 FROM signals s
                  WHERE s.trader_addr = fs.wallet_addr
                    AND s.slug = ?
                    AND LOWER(REPLACE(REPLACE(s.outcome, '🟢 ', ''), '🔴 ', '')) = ?
                    AND s.usd_size >= 100
                    AND CAST(s.timestamp AS INTEGER) >= ?
              )
              AND (
                  SELECT COUNT(DISTINCT LOWER(REPLACE(REPLACE(s2.outcome, '🟢 ', ''), '🔴 ', '')))
                  FROM signals s2
                  WHERE s2.trader_addr = fs.wallet_addr AND s2.slug = ?
                    AND s2.usd_size >= 100
                    AND CAST(s2.timestamp AS INTEGER) >= ?
              ) <= 1
            GROUP BY fs.wallet_addr
            ORDER BY market_vol.total_volume DESC
            LIMIT 10
        """
        async with await conn.execute(query, (
            market_slug, outcome_normalized, sixty_minutes_ago,
            funder_address, exclude_wallet,
            market_slug, outcome_normalized, sixty_minutes_ago,
            market_slug, sixty_minutes_ago
        )) as cursor:
            rows = await cursor.fetchall()
    else:
        # No outcome filter (for general cluster detection)
        # TASK 1: LIVE MODE - Only look at last 60 minutes
        # TASK 1: Added usd_size >= 100 filter to remove "dust" bets ($5 ladder patterns)
        # TASK 2: Use CAST(timestamp AS INTEGER) >= (? - 3600) for timezone-safe comparison
        query = """
            SELECT
                fs.wallet_addr,
                fs.source_label,
                fs.funding_ts,
                COALESCE(market_vol.total_volume, 0) as total_volume,
                COALESCE(market_vol.max_predictions, 0) as total_trades,
                COALESCE(market_vol.outcome_count, 0) as outcome_count
            FROM funding_sources fs
            LEFT JOIN (
                SELECT
                    trader_addr,
                    SUM(usd_size) as total_volume,
                    MAX(predictions) as max_predictions,
                    COUNT(DISTINCT LOWER(REPLACE(REPLACE(outcome, '🟢 ', ''), '🔴 ', ''))) as outcome_count
                FROM signals
                WHERE slug = ?
                  AND usd_size >= 100
                  AND CAST(timestamp AS INTEGER) >= ?
                GROUP BY trader_addr
                HAVING outcome_count <= 1
            ) market_vol ON fs.wallet_addr = market_vol.trader_addr
            WHERE fs.funder_address = ?
              AND fs.wallet_addr != ?
              AND EXISTS (
                  SELECT 1 FROM signals s
                  WHERE s.trader_addr = fs.wallet_addr AND s.slug = ?
                    AND s.usd_size >= 100
                    AND CAST(s.timestamp AS INTEGER) >= ?
              )
              AND (
                  SELECT COUNT(DISTINCT LOWER(REPLACE(REPLACE(s2.outcome, '🟢 ', ''), '🔴 ', '')))
                  FROM signals s2
                  WHERE s2.trader_addr = fs.wallet_addr AND s2.slug = ?
                    AND s2.usd_size >= 100
                    AND CAST(s2.timestamp AS INTEGER) >= ?
              ) <= 1
            GROUP BY fs.wallet_addr
            ORDER BY market_vol.total_volume DESC
            LIMIT 10
        """
        async with await conn.execute(query, (
            market_slug, sixty_minutes_ago, funder_address, exclude_wallet,
            market_slug, sixty_minutes_ago, market_slug, sixty_minutes_ago
        )) as cursor:
            rows = await cursor.fetchall()

    return [
        {
            'wallet_addr': row['wallet_addr'],
            'source_label': row['source_label'],
            'funding_ts': row['funding_ts'],
            'total_volume': row['total_volume'],
            'total_trades': row['total_trades'],
            'outcome_count': row['outcome_count']
        }
        for row in rows
    ]


async def calculate_cluster_metrics_for_market(conn, funder_address: str, market_slug: str, target_outcome: str = None) -> Dict[str, Any]:
    """
    Phase 3 V12: Calculates aggregated metrics for cluster ON A SPECIFIC MARKET.

    CRITICAL FIXES:
    1. Uses MAX(predictions) from signals for live trade count
    2. Includes wash trade exclusion filter
    3. Optional outcome filter for same-outcome clusters
    4. TASK 1: LIVE MODE - Only looks at last 60 minutes of activity
    5. TASK 2: Use Unix timestamp comparison to eliminate timezone issues

    Returns: {
        'wallet_count': int,
        'total_volume': float,
        'avg_total_trades': float,
        'total_trades': int
    }
    """
    outcome_normalized = normalize_outcome(target_outcome) if target_outcome else None

    # TASK 2: Calculate Unix timestamp for 60 minutes ago
    current_unix_time = int(time.time())
    sixty_minutes_ago = current_unix_time - 3600

    if outcome_normalized:
        # TASK 2: Use CAST(timestamp AS INTEGER) >= ? for timezone-safe comparison
        query = """
            SELECT
                COUNT(DISTINCT fs.wallet_addr) as wallet_count,
                COALESCE(SUM(market_vol.total_volume), 0) as total_volume,
                COALESCE(AVG(market_vol.max_predictions), 0) as avg_total_trades,
                COALESCE(SUM(market_vol.max_predictions), 0) as total_trades
            FROM funding_sources fs
            LEFT JOIN (
                SELECT
                    trader_addr,
                    SUM(usd_size) as total_volume,
                    MAX(predictions) as max_predictions
                FROM signals
                WHERE slug = ?
                  AND LOWER(REPLACE(REPLACE(outcome, '🟢 ', ''), '🔴 ', '')) = ?
                  AND usd_size >= 100
                  AND CAST(timestamp AS INTEGER) >= ?
                GROUP BY trader_addr
                HAVING COUNT(DISTINCT LOWER(REPLACE(REPLACE(outcome, '🟢 ', ''), '🔴 ', ''))) <= 1
            ) market_vol ON fs.wallet_addr = market_vol.trader_addr
            WHERE fs.funder_address = ?
              AND EXISTS (
                  SELECT 1 FROM signals s
                  WHERE s.trader_addr = fs.wallet_addr
                    AND s.slug = ?
                    AND LOWER(REPLACE(REPLACE(s.outcome, '🟢 ', ''), '🔴 ', '')) = ?
                    AND s.usd_size >= 100
                    AND CAST(s.timestamp AS INTEGER) >= ?
              )
              AND (
                  SELECT COUNT(DISTINCT LOWER(REPLACE(REPLACE(s2.outcome, '🟢 ', ''), '🔴 ', '')))
                  FROM signals s2
                  WHERE s2.trader_addr = fs.wallet_addr AND s2.slug = ?
                    AND s2.usd_size >= 100
                    AND CAST(s2.timestamp AS INTEGER) >= ?
              ) <= 1
        """
        async with await conn.execute(query, (
            market_slug, outcome_normalized, sixty_minutes_ago,
            funder_address, market_slug, outcome_normalized, sixty_minutes_ago,
            market_slug, sixty_minutes_ago
        )) as cursor:
            row = await cursor.fetchone()
    else:
        # TASK 2: Use CAST(timestamp AS INTEGER) >= ? for timezone-safe comparison
        query = """
            SELECT
                COUNT(DISTINCT fs.wallet_addr) as wallet_count,
                COALESCE(SUM(market_vol.total_volume), 0) as total_volume,
                COALESCE(AVG(market_vol.max_predictions), 0) as avg_total_trades,
                COALESCE(SUM(market_vol.max_predictions), 0) as total_trades
            FROM funding_sources fs
            LEFT JOIN (
                SELECT
                    trader_addr,
                    SUM(usd_size) as total_volume,
                    MAX(predictions) as max_predictions
                FROM signals
                WHERE slug = ?
                  AND usd_size >= 100
                  AND CAST(timestamp AS INTEGER) >= ?
                GROUP BY trader_addr
                HAVING COUNT(DISTINCT LOWER(REPLACE(REPLACE(outcome, '🟢 ', ''), '🔴 ', ''))) <= 1
            ) market_vol ON fs.wallet_addr = market_vol.trader_addr
            WHERE fs.funder_address = ?
              AND EXISTS (
                  SELECT 1 FROM signals s
                  WHERE s.trader_addr = fs.wallet_addr AND s.slug = ?
                    AND s.usd_size >= 100
                    AND CAST(s.timestamp AS INTEGER) >= ?
              )
              AND (
                  SELECT COUNT(DISTINCT LOWER(REPLACE(REPLACE(s2.outcome, '🟢 ', ''), '🔴 ', '')))
                  FROM signals s2
                  WHERE s2.trader_addr = fs.wallet_addr AND s2.slug = ?
                    AND s2.usd_size >= 100
                    AND CAST(s2.timestamp AS INTEGER) >= ?
              ) <= 1
        """
        async with await conn.execute(query, (
            market_slug, sixty_minutes_ago, funder_address,
            market_slug, sixty_minutes_ago, market_slug, sixty_minutes_ago
        )) as cursor:
            row = await cursor.fetchone()

    return {
        'wallet_count': row['wallet_count'] or 0,
        'total_volume': row['total_volume'] or 0,
        'avg_total_trades': row['avg_total_trades'] or 0,
        'total_trades': row['total_trades'] or 0
    }


def detect_sybil_pattern(siblings: List[Dict]) -> bool:
    """
    Phase 3: Detects Sybil/farm pattern by analyzing volume distribution.
    Sybil indicator: All wallets have similar volumes (within 25% tolerance)
    """
    if len(siblings) < 2:
        return False

    volumes = [s['total_volume'] for s in siblings if s['total_volume'] > 0]

    if len(volumes) < 2:
        return False

    max_vol = max(volumes)
    min_vol = min(volumes)

    if max_vol > 0 and (min_vol / max_vol) > 0.75:
        return True

    return False


# ==========================================
# 🚨 V12 COORDINATED ATTACK DETECTION
# ==========================================

async def check_coordinated_attack(session, conn, wallet: str, res: Dict, market_slug: str, outcome: str, market_title: str = None, trigger_usd_size: float = 0, trigger_predictions: int = 0):
    """
    V15 LIVE MODE: Detects coordinated attacks with INSTANT alerts for new wallets.

    KEY CHANGES:
    1. Allow ALL funders (Relay, CEX, etc.) - no source restriction
    2. Calculate 4 quality flags and require >= 3 to send alert (or 2/3 for DIAMOND)
    3. Whale Override: total_volume > $50,000 sends alert regardless of flags
    4. TASK 2: NEW WALLET TRACKING - Instant alerts for new wallets, 10-min cooldown for accumulation only

    FLAGS:
    - Flag 1 (Low Activity): avg_trades < 100
    - Flag 2 (Price Sync): (max_price - min_price) / max_price < 0.05 (5% diff)
    - Flag 3 (Time Sync): timing_window_mins < 60
    - Flag 4 (Size Uniformity): max_size / avg_size <= 2.5 (real syndicates use similar amounts)

    CRITICAL RULE: If (Flag1 + Flag2 + Flag3 + Flag4) >= 3, then SEND ALERT.
    DIAMOND STATUS: Requires Flag4 (Size Uniformity) to be TRUE.

    TASK 2 LOGIC:
    - Track known_wallets per cluster_key in cluster_state table
    - If current wallet is NEW -> SEND ALERT IMMEDIATELY
    - If current wallet is KNOWN (accumulation) -> Apply 10-minute cooldown
    
    NEW PARAMETERS:
    - trigger_usd_size: USD size of the current signal that triggered the cluster detection
    - trigger_predictions: Number of predictions (trades) for the trigger wallet
    """
    funder = res.get('funder')
    if not funder:
        return  # No funder = no cluster possible

    # ==========================================
    # 🕐 DB SYNC JITTER: Give SQLite a moment to finish the transaction
    # ==========================================
    # The signal was just inserted by maintest.py, but SQLite may not have
    # committed/indexed it yet. This tiny delay ensures the cluster queries
    # see the complete data (volume, trades, etc.) for the trigger wallet.
    # ==========================================
    await asyncio.sleep(0.5)

    # ==========================================
    # TASK 2: NEW WALLET DETECTION (REPLACES OLD COOLDOWN)
    # ==========================================
    cluster_key = f"{funder}_{market_slug}_{normalize_outcome(outcome)}"
    current_time = int(datetime.now(timezone.utc).timestamp())
    accumulation_cooldown_period = 10 * 60  # 10 minutes for accumulation

    # Get current cluster state
    async with await conn.execute("""
        SELECT known_wallets, last_accumulation_alert
        FROM cluster_state
        WHERE cluster_key = ?
    """, (cluster_key,)) as cursor:
        cluster_state = await cursor.fetchone()

    current_wallet = wallet.lower()
    is_new_wallet = True

    if cluster_state:
        known_wallets = json.loads(cluster_state['known_wallets']) if cluster_state['known_wallets'] else []
        last_accumulation_alert = cluster_state['last_accumulation_alert']

        if current_wallet in known_wallets:
            # This wallet was already alerted - check accumulation cooldown
            is_new_wallet = False
            # TASK 4: Strict cooldown enforcement - if wallet is known, always enforce 10-min cooldown
            if last_accumulation_alert is None or (current_time - last_accumulation_alert) < accumulation_cooldown_period:
                if last_accumulation_alert:
                    time_since_last = current_time - last_accumulation_alert
                    print(f"[V15 ACCUMULATION COOLDOWN] Wallet {current_wallet[:10]}... already known, alerted {time_since_last // 60} mins ago")
                else:
                    print(f"[V15 ACCUMULATION COOLDOWN] Wallet {current_wallet[:10]}... already known, no accumulation timestamp set")
                return
        else:
            # New wallet in the cluster - will alert immediately
            is_new_wallet = True
            print(f"[V15 NEW WALLET] {current_wallet[:10]}... is NEW to cluster {cluster_key[:40]}...")
    else:
        # First time seeing this cluster
        known_wallets = []
        print(f"[V15 NEW CLUSTER] First alert for cluster {cluster_key[:40]}...")

    # ==========================================
    # 1. GET SIBLINGS WITH OUTCOME MATCHING
    # ==========================================
    siblings = await get_cluster_siblings_for_market(
        conn, funder, current_wallet, market_slug, target_outcome=outcome
    )

    # ==========================================
    # TASK 3: HARDEN CLUSTER GATE (STRICT 3+)
    # ==========================================
    # Calculate total unique count: siblings + current wallet (trigger)
    total_unique_count = len(siblings) + 1
    if total_unique_count < 3:
        print(f"[V16 BLOCKED] Cluster gate: only {total_unique_count} wallets found (need 3+)")
        return

    # ==========================================
    # 2. CALCULATE CLUSTER METRICS
    # ==========================================
    cluster_metrics = await calculate_cluster_metrics_for_market(
        conn, funder, market_slug, target_outcome=outcome
    )

    total_size = cluster_metrics['total_volume']
    unique_wallets = cluster_metrics['wallet_count']
    total_trades = cluster_metrics['total_trades']
    avg_trades = cluster_metrics['avg_total_trades']

    # ==========================================
    # 🔧 FIX: Recalculate trades in Python to handle Fast-Fail 0s
    # maintest.py skips API calls for trades < $500 to save limits (writes 0).
    # We must fetch the real count here for accurate Flag 1 calculation.
    # ==========================================
    if trigger_predictions == 0:
        trigger_predictions = await get_polymarket_predictions(session, current_wallet)
        print(f"[CLUSTER FIX] {current_wallet[:10]}... | Fetched trigger predictions: {trigger_predictions}")
        
    for s in siblings:
        if s['total_trades'] == 0:
            s['total_trades'] = await get_polymarket_predictions(session, s['wallet_addr'])
            print(f"[CLUSTER FIX] {s['wallet_addr'][:10]}... | Fetched sibling predictions: {s['total_trades']}")
             
    # Recalculate true average trades for accurate Flag 1 calculation
    total_trades = trigger_predictions + sum(s['total_trades'] for s in siblings)
    avg_trades = total_trades / (len(siblings) + 1)
    print(f"[CLUSTER FIX] Recalculated avg_trades: {avg_trades:.1f} (was {cluster_metrics['avg_total_trades']:.1f})")

    # ==========================================
    # 🔧 FIX: Calculate timing window from TRADE timestamps, not funding timestamps
    # Previous bug: Used funding_ts (when wallet received funds) which could be months apart
    # Fixed: Use actual trade timestamps from signals table to measure coordination
    # ==========================================
    wallet_addrs_for_ts = [s['wallet_addr'] for s in siblings]

    if len(wallet_addrs_for_ts) > 0:
        # 🔧 FIX: Use direct timestamp column (INTEGER Unix epoch) - strftime('%s', X) returns NULL for integers
        trade_ts_query = """
            SELECT MIN(timestamp) as first_trade, MAX(timestamp) as last_trade
            FROM signals
            WHERE trader_addr IN ({})
              AND slug = ?
              AND LOWER(REPLACE(REPLACE(outcome, '🟢 ', ''), '🔴 ', '')) = ?
        """.format(','.join('?' * len(wallet_addrs_for_ts)))

        trade_ts_row = await (await conn.execute(trade_ts_query, wallet_addrs_for_ts + [market_slug, normalize_outcome(outcome)])).fetchone()

        if trade_ts_row and trade_ts_row['first_trade'] and trade_ts_row['last_trade']:
            timing_window_mins = float(trade_ts_row['last_trade']) - float(trade_ts_row['first_trade'])
            timing_window_mins = timing_window_mins / 60
        else:
            # Fallback: Try using meta_data ts from signals
            # 🔧 FIX: Ensure all paths return Unix timestamps as integers (removed strftime)
            meta_ts_query = """
                SELECT MIN(
                    COALESCE(
                        CAST(json_extract(meta_data, '$.ts') AS INTEGER),
                        timestamp
                    )
                ) as first_trade,
                MAX(
                    COALESCE(
                        CAST(json_extract(meta_data, '$.ts') AS INTEGER),
                        timestamp
                    )
                ) as last_trade
                FROM signals s
                WHERE trader_addr IN ({})
                  AND slug = ?
            """.format(','.join('?' * len(wallet_addrs_for_ts)))

            meta_ts_row = await (await conn.execute(meta_ts_query, wallet_addrs_for_ts + [market_slug])).fetchone()
            if meta_ts_row and meta_ts_row['first_trade'] and meta_ts_row['last_trade']:
                timing_window_mins = float(meta_ts_row['last_trade']) - float(meta_ts_row['first_trade'])
                timing_window_mins = timing_window_mins / 60
            else:
                timing_window_mins = 0.0  # No valid timestamps, allow cluster through
    else:
        timing_window_mins = 0.0

    # 🔍 DEBUG: Log timing window calculation for cluster analysis
    log(f"[CLUSTER TIMING] Slug: {market_slug[:40]}... | Wallets: {unique_wallets} | Timing window: {timing_window_mins:.1f} mins | Volume: ${total_size:,.0f}")

    # ==========================================
    # 🚨 EMERGENCY KILL-SWITCHES (GHOST CLUSTER BUG FIX)
    # ==========================================
    # Gate 1: Unique Wallets - A cluster of 1 or 2 is NOT a cluster
    if unique_wallets < 3:
        return

    # Gate 2: Zero Volume - Ignore ghosts (also block <$1k to avoid noise)
    if total_size <= 1000:
        print(f"[V13 BLOCKED] Volume too low: ${total_size:,.0f} < $1,000 | Wallets: {unique_wallets} | Funder: {funder[:10]}...")
        return

    # Gate 3: Time Sanity - Ignore anything spanning more than 7 days (10080 mins)
    # FIX: Now uses trade timestamps instead of funding timestamps
    if timing_window_mins > 10080:
        print(f"[V13 BLOCKED] Time window too wide: {timing_window_mins:.0f} mins > 7 days | Wallets: {unique_wallets}")
        return

    # ==========================================
    # 3. FETCH PRICES FOR PRICE SYNC CHECK
    # ==========================================
    # FIX: Validate siblings list before building query
    wallet_addrs = [s['wallet_addr'] for s in siblings]
    
    if len(wallet_addrs) == 0:
        print(f"[V13 BLOCKED] No sibling wallets found for price check | Funder: {funder[:10]}...")
        return
    
    # Get entry prices from signals for all cluster members on this market
    price_query = """
        SELECT MIN(price) as min_price, MAX(price) as max_price
        FROM signals
        WHERE slug = ?
          AND LOWER(REPLACE(REPLACE(outcome, '🟢 ', ''), '🔴 ', '')) = ?
          AND trader_addr IN ({})
          AND price > 0
    """.format(','.join('?' * len(wallet_addrs)))

    price_row = await (await conn.execute(price_query, [market_slug, normalize_outcome(outcome)] + wallet_addrs)).fetchone()

    min_price = price_row['min_price'] if price_row and price_row['min_price'] else 0
    max_price = price_row['max_price'] if price_row and price_row['max_price'] else 0

    # Calculate price sync: (max - min) / max < 0.05 means prices are within 5%
    price_sync = False
    if max_price > 0 and min_price > 0:
        price_diff_pct = (max_price - min_price) / max_price
        price_sync = price_diff_pct < 0.05
    else:
        # No valid prices found - log for debugging
        print(f"[V14 DEBUG] No valid prices found for cluster | min={min_price} | max={max_price}")

    # ==========================================
    # TASK 2: SIZE UNIFORMITY CHECK
    # ==========================================
    # A real syndicate uses similar amounts. Calculate coefficient of variation.
    # Query individual trade sizes for each sibling wallet on this market/outcome
    size_query = """
        SELECT trader_addr, SUM(usd_size) as wallet_size
        FROM signals
        WHERE slug = ?
          AND LOWER(REPLACE(REPLACE(outcome, '🟢 ', ''), '🔴 ', '')) = ?
          AND trader_addr IN ({})
          AND usd_size >= 100
        GROUP BY trader_addr
    """.format(','.join('?' * len(wallet_addrs)))

    size_row = await (await conn.execute(size_query, [market_slug, normalize_outcome(outcome)] + wallet_addrs)).fetchall()
    
    wallet_sizes = [row['wallet_size'] for row in size_row if row['wallet_size'] and row['wallet_size'] > 0]
    
    # Calculate size uniformity metrics
    if len(wallet_sizes) > 0:
        avg_size = sum(wallet_sizes) / len(wallet_sizes)
        max_size = max(wallet_sizes)
        min_size = min(wallet_sizes)
        
        # Size uniformity: max_size / avg_size <= 2.5 means sizes are consistent
        # If ratio > 3, this is a "Ladder" pattern (not a clean syndicate)
        size_ratio = max_size / avg_size if avg_size > 0 else float('inf')
        flag_size_uniformity = size_ratio <= 2.5
        
        # Log for debugging
        log(f"[SIZE UNIFORMITY] Avg: ${avg_size:,.0f} | Max: ${max_size:,.0f} | Min: ${min_size:,.0f} | Ratio: {size_ratio:.2f}x")
    else:
        avg_size = 0
        max_size = 0
        min_size = 0
        size_ratio = float('inf')
        flag_size_uniformity = False
        log(f"[SIZE UNIFORMITY] No valid sizes found for cluster")

    # ==========================================
    # TASK 3: WEIGHTED AVERAGE PRICE
    # ==========================================
    # Calculate weighted_avg_price = Sum(usd_size * price) / Sum(usd_size)
    weighted_price_query = """
        SELECT 
            SUM(usd_size * price) as weighted_sum,
            SUM(usd_size) as total_usd
        FROM signals
        WHERE slug = ?
          AND LOWER(REPLACE(REPLACE(outcome, '🟢 ', ''), '🔴 ', '')) = ?
          AND trader_addr IN ({})
          AND usd_size >= 100
          AND price > 0
    """.format(','.join('?' * len(wallet_addrs)))

    wp_row = await (await conn.execute(weighted_price_query, [market_slug, normalize_outcome(outcome)] + wallet_addrs)).fetchone()
    
    if wp_row and wp_row['total_usd'] and wp_row['total_usd'] > 0:
        weighted_avg_price = wp_row['weighted_sum'] / wp_row['total_usd']
    else:
        weighted_avg_price = (min_price + max_price) / 2 if min_price > 0 and max_price > 0 else 0

    # ==========================================
    # 4. CALCULATE 4 FLAGS (3-OUT-OF-4 LOGIC)
    # ==========================================
    # Flag 1: Low Activity Profile (avg_trades < 100)
    flag_low_activity = avg_trades < 100

    # Flag 2: Price Correlation (prices within 5%)
    flag_price_sync = price_sync

    # Flag 3: Time Sync (timing window < 60 mins)
    flag_time_sync = timing_window_mins < 60

    # Flag 4: Size Uniformity (max_size / avg_size <= 2.5)
    # flag_size_uniformity already calculated above

    # Count how many flags are met
    flags_met = sum([flag_low_activity, flag_price_sync, flag_time_sync, flag_size_uniformity])

    # ==========================================
    # 5. DECISION: 3-OUT-OF-4 LOGIC
    # ==========================================
    # Only proceed if Flags >= 3. Volume alone should never trigger an alert.
    if flags_met < 3:
        print(f"[V14 BLOCKED] Quality score too low | Flags: {flags_met}/4 | Volume: ${total_size:,.0f} | Wallets: {unique_wallets}")
        print(f"   Flag1 (Low Activity <100): {'✅' if flag_low_activity else '❌'} (avg_trades={avg_trades:.0f})")
        print(f"   Flag2 (Price Sync <5%): {'✅' if flag_price_sync else '❌'} (min={min_price:.3f}, max={max_price:.3f})")
        print(f"   Flag3 (Time Sync <60m): {'✅' if flag_time_sync else '❌'} (timing={timing_window_mins:.1f}m)")
        print(f"   Flag4 (Size Uniformity): {'✅' if flag_size_uniformity else '❌'} (ratio={size_ratio:.2f}x)")
        return

    # ==========================================
    # 7. DETECT SYBIL PATTERN
    # ==========================================
    is_sybil = detect_sybil_pattern(siblings)

    # ==========================================
    # 8. SEND ALERT
    # ==========================================
    # Determine alert type based on pattern
    if is_sybil and unique_wallets >= 2:
        alpha_reason = f"🕸️ SYBIL (x{unique_wallets})"
        print(f"[V14 ALERT] Sybil cluster | Flags: {flags_met}/4 | Wallets: {unique_wallets} | Volume: ${total_size:,.0f}")
    elif unique_wallets >= 3:
        alpha_reason = f"🔥 FARM (x{unique_wallets})"
        print(f"[V14 ALERT] Farm cluster | Flags: {flags_met}/4 | Wallets: {unique_wallets} | Volume: ${total_size:,.0f}")
    else:
        return  # Not enough wallets

    # Check for instant sync
    is_instant = timing_window_mins < 0.1

    # DIAMOND status: requires flag_size_uniformity to be TRUE
    is_diamond = flag_size_uniformity and flags_met >= 3

    print(f"   Flag1 (Low Activity <100): {'✅' if flag_low_activity else '❌'}")
    print(f"   Flag2 (Price Sync <5%): {'✅' if flag_price_sync else '❌'}")
    print(f"   Flag3 (Time Sync <60m): {'✅' if flag_time_sync else '❌'}")
    print(f"   Flag4 (Size Uniformity): {'✅' if flag_size_uniformity else '❌'} (ratio={size_ratio:.2f}x)")
    if is_diamond:
        print(f"   💎 DIAMOND STATUS: Size uniformity confirmed!")

    # ==========================================
    # 🤖 AI VERIFICATION — SPRINT 2: Check whitelist table instead of calling AI
    # ==========================================
    # P3.1: Check whitelist table instead of calling AI directly
    _wl_slug = market_slug or ""
    if _wl_slug and USE_AI_FILTER:
        try:
            _wl_conn = await get_async_db_connection()
            _wl_row = await (await _wl_conn.execute(
                "SELECT is_approved FROM markets_whitelist WHERE slug = ?",
                (_wl_slug,)
            )).fetchone()
            await _wl_conn.close()
            if _wl_row is not None and not _wl_row['is_approved']:
                print(f"[V16 BLOCKED] Cluster on whitelist-rejected market: {_wl_slug[:50]}...")
                return None
        except Exception as e:
            log(f"[WHITELIST CHECK] DB error: {e} — allowing through")

    # ==========================================
    # TASK 4: FIX WALLET DISPLAY - Include trigger wallet with correct stats
    # ==========================================
    # siblings does NOT include current_wallet (the trigger) because
    # get_cluster_siblings_for_market() explicitly excludes it via "WHERE fs.wallet_addr != ?"
    # 
    # BUG FIX: Previously, the trigger wallet showed "Trades: 0 | Vol: $0" because:
    # 1. all_wallets was built correctly with [current_wallet] + siblings
    # 2. But wallet_details=siblings was passed, which doesn't include trigger wallet
    # 3. alert_manager.py initializes trigger wallet with 0/0, then loops through
    #    wallet_details to update stats - but trigger wallet is never found!
    #
    # FIX: Create trigger_wallet_detail with the current signal's data and prepend
    # it to the wallet_details list before passing to alert_cex_coordinated_attack().
    
    # Build all_wallets for display order
    all_wallets = [current_wallet] + [s['wallet_addr'] for s in siblings]
    
    # Build all_sizes including trigger wallet's current signal
    all_sizes = [trigger_usd_size] + [s['total_volume'] for s in siblings]
    
    # Create trigger wallet detail with current signal data
    trigger_wallet_detail = {
        'wallet_addr': current_wallet,
        'total_volume': trigger_usd_size,
        'total_trades': trigger_predictions,
        'entry_ts': int(time.time())
    }
    
    # Prepend trigger wallet to siblings for complete wallet_details list
    all_wallet_details = [trigger_wallet_detail] + siblings

    await alert_manager.alert_cex_coordinated_attack(
        wallets=all_wallets,
        market_title=market_title if market_title else market_slug,
        outcome=outcome,
        sizes=all_sizes,
        market_slug=market_slug,
        timing_window_mins=timing_window_mins,
        avg_size=avg_size,  # Use calculated avg_size from size uniformity check
        funding_source=funder,
        is_instant_sync=is_instant,
        instant_sync_count=len(siblings) if is_instant else 0,
        wallet_details=all_wallet_details,
        slippage_pct=None,
        # Pass flag states for alert formatting
        flag_low_activity=flag_low_activity,
        flag_price_sync=flag_price_sync,
        flag_time_sync=flag_time_sync,
        flag_size_uniformity=flag_size_uniformity,
        # TASK 3: Weighted average price
        weighted_avg_price=weighted_avg_price,
        # TASK 2: Size consistency info
        size_ratio=size_ratio,
        # TASK 3: Timing info for "Current Lag" - convert to int
        first_trade_ts=int(trade_ts_row['first_trade']) if trade_ts_row and trade_ts_row['first_trade'] else None,
        # TASK 2: New wallet tracking
        is_new_wallet=is_new_wallet,
        known_wallets_count=len(known_wallets) if known_wallets else 0
    )

    # ==========================================
    # TASK 2: UPDATE CLUSTER STATE
    # ==========================================
    # Update known_wallets and last_accumulation_alert
    try:
        if is_new_wallet:
            # Add new wallet to the known list
            known_wallets.append(current_wallet)
            known_wallets_json = json.dumps(known_wallets)

            await conn.execute("""
                INSERT INTO cluster_state (cluster_key, known_wallets, last_accumulation_alert)
                VALUES (?, ?, ?)
                ON CONFLICT(cluster_key) DO UPDATE SET
                    known_wallets = excluded.known_wallets
                WHERE cluster_key = ?
            """, (cluster_key, known_wallets_json, None, cluster_key))
            print(f"[V15 CLUSTER STATE] Added new wallet {current_wallet[:10]}... to cluster | Total known: {len(known_wallets)}")
        else:
            # Update last_accumulation_alert for known wallet
            await conn.execute("""
                UPDATE cluster_state
                SET last_accumulation_alert = ?
                WHERE cluster_key = ?
            """, (current_time, cluster_key))
            print(f"[V15 CLUSTER STATE] Updated accumulation timestamp for wallet {current_wallet[:10]}...")

        await conn.commit()
    except Exception as state_err:
        print(f"[V15 CLUSTER STATE] Failed to update cluster state: {state_err}")

    return alpha_reason


# ==========================================
# 📊 PHASE 2: PNL & WALLET CLASSIFICATION
# ==========================================

def safe_timestamp(ts) -> int:
    """
    STAGE 1 FIX: Simple timestamp converter - handles INTEGER/float only.
    Returns 0 for invalid/missing timestamps.
    """
    if ts is None or ts == 0:
        return 0
    if isinstance(ts, (int, float)):
        ts_int = int(ts)
        return ts_int if ts_int >= 1577836800 else 0  # Year 2020+ sanity check
    if isinstance(ts, str):
        try:
            # Try parsing as integer string only (no float fallback)
            ts_int = int(ts)
            return ts_int if ts_int >= 1577836800 else 0
        except (ValueError, TypeError):
            return 0
    return 0


async def fetch_market_data_cached(session, slug, market_cache):
    if slug in market_cache:
        return market_cache[slug]

    try:
        async with session.get(POLYMARKET_GAMMA_API, params={"slug": slug}, timeout=10) as r:
            if r.status == 200:
                data = await r.json()
                
                # Handle both list and dict responses
                if isinstance(data, list):
                    if len(data) == 0:
                        return None
                    market = data[0]
                elif isinstance(data, dict):
                    market = data
                else:
                    return None
                
                # Validate market is a dict before accessing
                if not isinstance(market, dict):
                    log(f"Error: market data is not a dict for {slug}")
                    return None

                current_prices = {}
                try:
                    outcomes = json.loads(market.get("outcomes", "[]"))
                    prices = json.loads(market.get("outcomePrices", "[]"))
                    for i, out in enumerate(outcomes):
                        if i < len(prices):
                            normalized_key = normalize_outcome(str(out))
                            current_prices[normalized_key] = float(prices[i])
                except Exception as e:
                    log(f"Error parsing prices for {slug}: {e}")

                market_data = {
                    'closed': market.get("closed", False),
                    'winner': market.get("winningOutcome"),
                    'current_prices': current_prices
                }
                market_cache[slug] = market_data
                return market_data
    except Exception as e:
        log(f"Error fetching market {slug}: {e}")

    return None


async def calculate_unrealized_pnl(session, wallet: str) -> Dict[str, Any]:
    conn = await get_async_db_connection()
    async with await conn.execute("""
        SELECT slug, outcome, price as entry_price, usd_size, meta_data
        FROM signals
        WHERE trader_addr = ?
    """, (wallet,)) as cursor:
        signals = await cursor.fetchall()
    await conn.close()

    if not signals:
        return {'total_pnl_usd': 0, 'total_pnl_pct': 0, 'total_invested': 0, 'positions': []}

    unique_slugs = list(set(s['slug'] for s in signals))
    market_cache = {}

    for slug in unique_slugs:
        await fetch_market_data_cached(session, slug, market_cache)
        await asyncio.sleep(0.1)

    total_pnl = 0.0
    total_invested = 0.0
    positions = []

    for s in signals:
        slug = s['slug']
        outcome_normalized = normalize_outcome(s['outcome'])
        entry_price = s['entry_price']
        usd_size = s['usd_size']

        market_info = market_cache.get(slug)
        if not market_info or market_info['closed']:
            continue

        current_prices = market_info['current_prices']
        current_price = current_prices.get(outcome_normalized)

        if current_price and entry_price > 0:
            shares = usd_size / entry_price
            pnl = (current_price - entry_price) * shares
            pnl_pct = ((current_price - entry_price) / entry_price) * 100 if entry_price > 0 else 0

            total_pnl += pnl
            total_invested += usd_size

            positions.append({
                'slug': slug,
                'outcome': outcome_normalized,
                'entry_price': entry_price,
                'current_price': current_price,
                'pnl_usd': pnl,
                'pnl_pct': pnl_pct
            })

    total_pnl_pct = (total_pnl / total_invested * 100) if total_invested > 0 else 0

    return {
        'total_pnl_usd': total_pnl,
        'total_pnl_pct': total_pnl_pct,
        'total_invested': total_invested,
        'positions': positions
    }


def detect_instant_funding(trade_ts: int, funding_ts: int) -> tuple:
    trade_ts = safe_timestamp(trade_ts)
    funding_ts = safe_timestamp(funding_ts)

    if trade_ts > 0 and funding_ts > 0:
        time_diff = trade_ts - funding_ts
        if 0 < time_diff < 600:
            return True, round(time_diff / 60, 1)
    return False, 0.0


def classify_wallet_status(wallet_age_hours: float, total_volume: float, predictions: int) -> str:
    if wallet_age_hours < 1 and predictions < 5:
        return "🔥 FRESH"
    elif total_volume > 50000:
        return "🐋 WHALE"
    elif wallet_age_hours < 24 and predictions < 20:
        return "🦅 EARLY BIRD"
    else:
        return "📊 VETERAN"

# --- API HELPERS ---

async def get_balance(session, address, token_contract=None):
    """
    Get MATIC or token balance for an address.
    Stage 4 Task 3: Uses polygonscan_limiter for rate limiting.
    """
    params = {
        "chainid": CHAIN_ID, "module": "account", "address": address, "tag": "latest",
        "apikey": POLYGONSCAN_API_KEY
    }
    if token_contract:
        params["action"] = "tokenbalance"
        params["contractaddress"] = token_contract
    else:
        params["action"] = "balance"

    try:
        # Stage 4: Use token bucket rate limiter instead of manual sleep
        await polygonscan_limiter.consume()
        async with session.get(V2_API_URL, params=params, timeout=5) as r:
            data = await r.json()
            val = int(data.get("result", 0))
            decimals = 6 if token_contract else 18
            return val / (10 ** decimals)
    except Exception as e:
        log(f"[get_balance] {address[:10]}... error: {type(e).__name__}: {e}")
        return 0


async def get_nonce(session, address):
    """Get transaction count (nonce) for an address."""
    url = "https://polygon-rpc.com"
    payload = {"jsonrpc":"2.0","method":"eth_getTransactionCount","params":[address,"latest"],"id":1}
    try:
        async with session.post(url, json=payload, timeout=5) as r:
            res = await r.json()
            return int(res.get('result', '0x0'), 16)
    except Exception as e:
        log(f"[get_nonce] {address[:10]}... error: {type(e).__name__}: {e}")
        return 0


async def fetch_v2(session, action, address):
    """
    Fetch transaction list from PolygonScan API.
    Stage 4 Task 3: Uses polygonscan_limiter for rate limiting.
    """
    params = {
        "chainid": CHAIN_ID, "module": "account", "action": action, "address": address,
        "startblock": 0, "endblock": 99999999, "sort": "asc", "page": 1, "offset": 10, "apikey": POLYGONSCAN_API_KEY
    }
    try:
        # Stage 4: Use token bucket rate limiter instead of manual sleep
        await polygonscan_limiter.consume()
        async with session.get(V2_API_URL, params=params, timeout=10) as resp:
            data = await resp.json()
            return data.get("result") if isinstance(data.get("result"), list) else []
    except Exception as e:
        log(f"[fetch_v2] {action}/{address[:10]}... error: {type(e).__name__}: {e}")
        return []


async def get_market_creation_time(session, slug):
    """Get market creation timestamp from Gamma API."""
    try:
        async with session.get(f"{POLYMARKET_GAMMA_API}/{slug}", timeout=5) as r:
            if r.status == 200:
                d = await r.json()
                created_at = d.get("createdAt")
                if created_at:
                    dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                    return int(dt.timestamp())
    except Exception as e:
        log(f"[get_market_creation_time] {slug}: {type(e).__name__}: {e}")
    return 0

# --- CORE FORENSICS ---

async def get_latest_funding_tx(session, wallet_addr, trade_ts):
    """
    V13 FIX: Finds the LATEST funding transaction INTO the wallet BEFORE the trade.

    CRITICAL: Excludes Polymarket contract transactions to avoid picking up
    the trade itself as the "funding" transaction.

    We want to find USDC/MATIC transfers from Exchanges/Bridges that happened
    BEFORE the trade, selecting the most recent one (latest timestamp < trade_ts).

    Filter criteria:
    - to.lower() == wallet_addr.lower() (Incoming only)
    - from_addr NOT in POLYMARKET_CONTRACTS
    - value > 50 (50 * 10^6 for USDC, 50 * 10^18 for MATIC/Internal)
    - int(timeStamp) < trade_ts (Only funding that happened BEFORE the trade)
    """
    addr = wallet_addr.lower()
    txs = await fetch_v2(session, "txlist", addr)
    tokens = await fetch_v2(session, "tokentx", addr)
    internals = await fetch_v2(session, "txlistinternal", addr)

    # Debug: Log how many transactions we found
    total_found = len(txs) + len(tokens) + len(internals)

    all_candidates = []
    excluded_pm = 0  # Count excluded Polymarket transactions
    excluded_below_threshold = 0  # Count excluded transactions below value threshold
    excluded_after_trade = 0  # Count excluded transactions after trade timestamp

    for t in (txs + tokens + internals):
        to_addr = t.get("to", "").lower()
        from_addr = t.get("from", "").lower()

        # Must be incoming transaction TO this wallet
        if to_addr != addr:
            continue

        # Skip if no timestamp
        if not t.get("timeStamp"):
            continue

        ts = int(t.get("timeStamp", 0))

        # ==========================================
        # 🛑 EXCLUDE TRANSACTIONS AFTER THE TRADE
        # ==========================================
        # Only consider funding that happened BEFORE the trade
        if ts >= trade_ts:
            excluded_after_trade += 1
            continue

        # ==========================================
        # 🛑 EXCLUDE POLYMARKET CONTRACT TRANSACTIONS
        # ==========================================
        # Skip if FROM address is a Polymarket contract
        # This prevents the trade itself from being counted as funding
        if from_addr in POLYMARKET_CONTRACTS:
            excluded_pm += 1
            continue

        # Skip if TO contract is Polymarket (outgoing to PM)
        contract_addr = t.get("contractAddress", "").lower()
        if contract_addr in POLYMARKET_CONTRACTS:
            excluded_pm += 1
            continue

        # Skip if transaction is related to Polymarket contracts
        tx_to = t.get("to", "").lower()
        if tx_to in POLYMARKET_CONTRACTS:
            excluded_pm += 1
            continue

        # ==========================================
        # 🛑 VALUE THRESHOLD CHECK
        # ==========================================
        # Filter for value > 50 (adjust for decimals)
        raw_val = float(t.get("value", 0))
        is_token = len(t.get("contractAddress", "")) > 0
        token_symbol = t.get("tokenSymbol", "").upper()

        # Determine decimals: USDC uses 6, others use 18
        if is_token and "USDC" in token_symbol:
            decimals = 6
        else:
            decimals = 18

        amount = raw_val / (10 ** decimals)

        # Skip if value <= 50
        if amount <= 50:
            excluded_below_threshold += 1
            continue

        all_candidates.append(t)

    # ==========================================
    # 🛠️ DEBUG: Log what we found
    # ==========================================
    if total_found > 0:
        relevant_count = len(all_candidates)
        print(f"[GET_LATEST_FUNDING_TX] {wallet_addr[:10]}... | Total: {total_found} | Relevant: {relevant_count} | Excluded PM: {excluded_pm} | Below $50: {excluded_below_threshold} | After Trade: {excluded_after_trade}")
        if len(all_candidates) > 0:
            # Return latest (highest timestamp) before trade
            latest = max(all_candidates, key=lambda x: int(x["timeStamp"]))
            ts = int(latest.get("timeStamp", 0))
            from_addr = latest.get("from", "Unknown")[:10]
            tx_time = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") if ts > 0 else "N/A"
            print(f"[GET_LATEST_FUNDING_TX] → Latest (before trade): {tx_time} | From: {from_addr}... | TS: {ts}")
        elif excluded_pm > 0:
            print(f"[GET_LATEST_FUNDING_TX] ⚠️ WARNING: All transactions excluded (only Polymarket trades found)")
    else:
        print(f"[GET_LATEST_FUNDING_TX] {wallet_addr[:10]}... | ⚠️ NO TRANSACTIONS FOUND")

    if not all_candidates:
        return None

    # Return latest transaction by timestamp (most recent funding before trade)
    return max(all_candidates, key=lambda x: int(x["timeStamp"]))

def is_round_gas(amount, is_token=False):
    if amount <= 0: return False
    round_numbers = [1.0, 5.0, 10.0, 20.0, 50.0, 100.0, 500.0, 1000.0]
    if is_token:
        return any(abs(amount - r) < 0.01 for r in round_numbers) or (amount % 100 == 0)
    return any(abs(amount - r) < 0.001 for r in round_numbers)

async def classify_sender(session, sender_addr, tx_amount=0, is_token=False):
    s = sender_addr.lower()

    if s in LABELS_CACHE:
        info = LABELS_CACHE[s]
        return info['cat'], info['name'], 0.99

    conn = await get_async_db_connection()
    async with await conn.execute("SELECT COUNT(*) FROM funding_sources WHERE funder_address = ?", (s,)) as cursor:
        row = await cursor.fetchone()
        count = row[0]
    await conn.close()
    if count > 10:
        return "Service", f"High Volume Distributor ({count} kids)", 0.95

    nonce = await get_nonce(session, s)
    if nonce > 20000: return "CEX", "High Activity Exchange", 0.90
    if nonce > 2000: return "Service", "Active Service", 0.70

    if is_round_gas(tx_amount, is_token) and nonce > 100:
        return "Service", "Suspected Hot Wallet (Round Amount)", 0.65

    if nonce < 500:
        bal_usdc = await get_balance(session, s, USDC_CONTRACT)
        if bal_usdc > 100000:
            return "Whale", "Rich Private Wallet / Fund", 0.80

    return "Private", f"Wallet ({s[:6]})", 0.5

async def trace_funding_source(session, wallet, trade_ts, depth=MAX_DEPTH):
    """
    V13 FIX: Traces funding source and returns CORRECT timestamp.

    CRITICAL: Uses get_latest_funding_tx to find the most recent funding
    event BEFORE the trade, not the earliest transaction ever.

    The timestamp returned is used for "Funded: X mins ago" calculations.

    PARAMS:
        session: aiohttp session
        wallet: wallet address to trace
        trade_ts: timestamp of the Polymarket trade (used to filter funding before trade)
        depth: maximum recursion depth for tracing
    """
    current_wallet = wallet.lower()
    first_funder = None
    first_ts = 0
    visited = {current_wallet}

    for hop in range(depth):
        tx = await get_latest_funding_tx(session, current_wallet, trade_ts)

        # If no transaction found at first hop, this is a genesis wallet
        if not tx:
            if hop == 0:
                # No incoming transactions - this wallet has no funding history
                # FALLBACK: Use trade_ts (time of Polymarket trade) as the funding timestamp
                print(f"[TRACE] {wallet[:10]}... | ⚠️ No funding history found - using trade time")
                return {"source": "Genesis", "label": "Fresh Wallet", "funder": None, "ts": trade_ts, "conf": 0.3}
            break

        sender = tx.get("from", "").lower()
        ts = int(tx.get("timeStamp", 0))
        tx_hash = tx.get("hash")

        raw_val = float(tx.get("value", 0))
        is_token = len(tx.get("contractAddress", "")) > 0
        amount = raw_val / (10**6 if is_token and "USDC" in tx.get("tokenSymbol", "") else 10**18)

        # CRITICAL FIX: Always capture first hop timestamp and funder
        # This is the actual funding transaction we care about
        if hop == 0:
            first_funder = sender
            first_ts = ts
            # ==========================================
            # 🛠️ DEBUG: Log what we found
            # ==========================================
            if ts > 0:
                funding_time = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
                mins_ago = (int(time.time()) - ts) // 60
                print(f"[TRACE] {wallet[:10]}... | Latest funding: {funding_time} ({mins_ago} mins ago) | From: {sender[:10]}... | Amount: ${amount:,.2f}")
            else:
                print(f"[TRACE] {wallet[:10]}... | ⚠️ WARNING: Zero timestamp from get_latest_funding_tx - using current time!")
                first_ts = int(time.time())  # Fallback to current time

        source, label, conf = await classify_sender(session, sender, amount, is_token)

        # If we found a CEX/Mixer/Bridge/Service/Whale, return immediately
        if source in ["CEX", "Mixer", "Bridge", "Service", "Whale", "Custom"]:
            return {
                "source": source, "label": label, "hash": tx_hash,
                "funder": first_funder, "ts": first_ts, "conf": conf
            }

        # If we've seen this sender before, stop to avoid loops
        if sender in visited:
            break
        visited.add(sender)
        current_wallet = sender

    # ==========================================
    # FINAL FALLBACK: Return Private with first hop data
    # ==========================================
    # Even for Private wallets, we MUST return the first funding timestamp
    if first_ts == 0:
        first_ts = trade_ts  # Fallback to trade time if no timestamp found
    return {"source": "Private", "label": f"Wallet ({first_funder[:6] if first_funder else 'Unknown'})", "hash": tx_hash, "funder": first_funder, "ts": first_ts, "conf": 0.5}

# --- MAIN WORKER LOOP ---

async def upsert_result(wallet, res):
    """
    🚀 SURVIVAL PATCH: Upsert funding source with retry logic for DB locks.
    """
    conn = await get_async_db_connection()
    try:
        async with await conn.execute("SELECT ultimate_source FROM funding_sources WHERE wallet_addr = ?", (wallet.lower(),)) as cursor:
            curr = await cursor.fetchone()

        if curr and curr[0] in ['CEX', 'Service', 'Bridge'] and res['source'] == 'Private':
            await conn.close()
            return

        # FIX: Use retry wrapper for INSERT/UPDATE to handle "database is locked"
        await execute_query_with_retry_async(conn, """
            INSERT INTO funding_sources (wallet_addr, ultimate_source, source_label, first_tx_hash, funder_address, funding_ts, last_updated, confidence_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(wallet_addr) DO UPDATE SET
            ultimate_source=excluded.ultimate_source, source_label=excluded.source_label,
            funder_address=excluded.funder_address, funding_ts=excluded.funding_ts, last_updated=excluded.last_updated
        """, (wallet.lower(), res['source'], res['label'], res.get('hash'), res['funder'], res['ts'], int(time.time()), res.get('conf', 0.5)))
        await conn.commit()
    except aiosqlite.OperationalError as e:
        print(f"[DB ERROR] upsert_result failed for {wallet[:10]}...: {e}")
    finally:
        await conn.close()


# ==========================================
# 🚨 ANALYZE AND ALERT - V12 STANDARD
# ==========================================

async def analyze_and_alert(session, conn, wallet, res, sig_id=None, signal_data=None):
    """
    V12 STANDARD: Master alert logic with ALL mandatory filters.

    PRIORITY 1: Coordinated Attack Detection (with all V12 filters)
    PRIORITY 2: Single Wallet Alerts (Mixer, Instant, Early Bird, Fresh, Elite, Sleeper)

    MANDATORY FILTERS FOR COORDINATED ATTACKS:
    1. Outcome Matching: Siblings MUST have bet on SAME outcome
    2. Wash Trade Exclusion: Exclude wallets with >1 distinct outcomes on same market
    3. Anti-Bot Filter: If ANY wallet has trades > BOT_TRADE_THRESHOLD, silence unless volume > $50k
    4. Slow Cluster Filter: If timing > 15 min, requires volume >= SLOW_CLUSTER_MIN_USD AND wallets >= SLOW_CLUSTER_MIN_WALLETS
    5. Instant Attack Filter: If same-block, requires volume >= INSTANT_ATTACK_MIN_USD

    PARAMS:
        sig_id: Optional signal ID passed from process_batch(). If provided, uses this ID
                instead of querying for the last unprocessed signal.
        signal_data: Optional dict from Redis containing all signal data. If provided,
                     uses this data directly instead of querying DB.
    """
    try:
        # ==========================================
        # BOT CHECK - Skip if already flagged
        # ==========================================
        perf = await (await conn.execute("SELECT is_bot, is_high_performer, win_rate, total_trades FROM trader_performance WHERE wallet_addr = ?", (wallet.lower(),))).fetchone()
        if perf and perf[0]:
            return  # Skip bots

        # ==========================================
        # 🚨 CRITICAL FIX: USE signal_data IF PROVIDED
        # ==========================================
        # If signal_data is provided from process_single_signal(), use it directly
        # Otherwise, fallback to querying DB (for backward compatibility)
        # ==========================================
        if signal_data is not None:
            # Use data from Redis signal directly - no DB query needed
            m_q = signal_data.get('market_title', '')
            m_out = signal_data.get('outcome', '')
            m_usd = signal_data.get('usd_size', 0)
            m_slug = signal_data.get('market_slug', '')
            m_price = signal_data.get('price', 0.0)
            signal_type = signal_data.get('alpha_tag', '')
            signal_alpha_tag = signal_data.get('alpha_tag', '')
            signal_timestamp = signal_data.get('timestamp', '')
            market_title = signal_data.get('market_title', m_q)
            total_bets = signal_data.get('predictions', 0)
            # 🔧 FIX: Fetch missing predictions from API if 0 (Fast-Fail recovery)
            if total_bets == 0:
                total_bets = await get_polymarket_predictions(session, wallet)
                print(f"[PREDICTIONS FIX] {wallet[:10]}... | Fetched from API: {total_bets} predictions")
            wallet_age_hours = -1.0  # Not available from Redis, will be calculated if needed
            meta_data_raw = json.dumps(signal_data.get('meta_dict', {}))
            sig_id = None  # Will be looked up if needed
            
            print(f"[SIGNAL] {wallet[:10]}... | Using Redis data | Type: {signal_type} | Alpha: {signal_alpha_tag}")
        else:
            # Fallback: Get the last UNPROCESSED signal with SOLO alert type
            # PROBLEM: Wallets can have multiple signals (e.g., FRESH ENTRY → BOT)
            # If we get the LAST signal (BOT), we miss the SOLO alert (FRESH/WHALE)
            #
            # SOLUTION: Get the last UNPROCESSED signal with SOLO alert type
            # Priority: FRESH ENTRY > WHALE > ACCUMULATION > PROVEN
            last_sig = await (await conn.execute("""
                SELECT id, market_q, outcome, usd_size, slug, meta_data, price,
                       total_bets, wallet_age_hours, market_title, timestamp, type, alpha_tag
                FROM signals
                WHERE trader_addr = ?
                  AND is_processed = 0
                  AND (type LIKE '%FRESH ENTRY%' OR type LIKE '%WHALE%'
                       OR type LIKE '%ACCUMULATION%' OR type LIKE '%PROVEN%')
                ORDER BY id DESC LIMIT 1
            """, (wallet.lower(),))).fetchone()

            # If no unprocessed solo signal found, try to get any unprocessed signal
            if not last_sig:
                last_sig = await (await conn.execute("""
                    SELECT id, market_q, outcome, usd_size, slug, meta_data, price,
                           total_bets, wallet_age_hours, market_title, timestamp, type, alpha_tag
                    FROM signals
                    WHERE trader_addr = ?
                      AND is_processed = 0
                    ORDER BY id DESC LIMIT 1
                """, (wallet.lower(),))).fetchone()

            if not last_sig:
                return

            sig_id = last_sig['id']
            m_q = last_sig['market_q']
            m_out = last_sig['outcome']
            m_usd = last_sig['usd_size']
            m_slug = last_sig['slug']
            m_price = last_sig['price'] or 0.0
            signal_timestamp = last_sig['timestamp']  # ISO format from DB
            signal_type = last_sig['type'] or ""  # e.g. "🆕 FRESH ENTRY", "🐋 WHALE BET"
            signal_alpha_tag = last_sig['alpha_tag'] or ""  # e.g. "👑 PROVEN"

            # Debug: Log what we got
            print(f"[SIGNAL] {wallet[:10]}... | Signal ID: {sig_id} | Type: {signal_type} | Alpha: {signal_alpha_tag}")

            # Get enriched data from DB
            total_bets = last_sig['total_bets'] or 0
            # 🔧 FIX: Fetch missing predictions from API if 0 (Fast-Fail recovery)
            if total_bets == 0:
                total_bets = await get_polymarket_predictions(session, wallet)
                print(f"[PREDICTIONS FIX] {wallet[:10]}... | Fetched from API: {total_bets} predictions")
            wallet_age_hours = last_sig['wallet_age_hours'] or 0.0
            market_title = last_sig['market_title'] or m_q
            meta_data_raw = last_sig['meta_data']  # For slippage extraction

        # ==========================================
        # 🛠️ MARK SIGNAL AS PROCESSED
        # Prevents re-processing the same signal on next run
        # ==========================================
        if sig_id is not None and isinstance(sig_id, int):
            try:
                await conn.execute("UPDATE signals SET is_processed = 1 WHERE id = ?", (sig_id,))
                await conn.commit()
                print(f"[PROCESS] Signal #{sig_id} marked as processed (is_processed=1)")
            except Exception as mark_err:
                print(f"[ERROR] Failed to mark signal #{sig_id} as processed: {mark_err}")

        # ==========================================
        # 🛑 TASK 1: GLOBAL INSIDER DETECTION
        # ==========================================
        # Check if this wallet is a proven insider based on alpha_tag
        # This flag will be used for ALL alert routing decisions
        # SHARD 3: Trust the is_insider flag from Redis (set by scanner)
        is_insider = signal_data.get('is_insider', False) if signal_data else (
            "PROVEN" in signal_alpha_tag or "INSIDER" in signal_alpha_tag
        )
        if is_insider:
            print(f"[ROUTING] Wallet {wallet} identified as PROVEN INSIDER - VIP routing enabled")

        # Parse trade timestamp
        # ==========================================
        # TASK 3: Robust parsing - handle both new INTEGER timestamps and old STRING formats
        # maintest.py now saves Unix timestamps as integers
        # Old records may still have "YYYY-MM-DD HH:MM:SS" string format
        # ==========================================
        trade_ts = 0

        # First try to parse from signals.timestamp
        if signal_data is not None:
            # Use timestamp from Redis signal_data
            ts_from_redis = signal_data.get('timestamp', '')
            if ts_from_redis:
                try:
                    trade_ts = int(ts_from_redis)
                    if trade_ts <= 0:
                        raise ValueError("Timestamp must be positive integer")
                    print(f"[TRADE_TS FIX] {wallet[:10]}... | Using Redis timestamp: {trade_ts}")
                except (ValueError, TypeError):
                    # Try meta_dict['ts'] as fallback
                    meta_dict = signal_data.get('meta_dict', {})
                    if isinstance(meta_dict, dict) and meta_dict.get('ts'):
                        trade_ts = int(meta_dict.get('ts', 0))
                        print(f"[TRADE_TS FIX] {wallet[:10]}... | Using meta_dict.ts: {trade_ts}")
        elif signal_timestamp:
            try:
                # STAGE 1 FIX: Simple integer parsing only - no fallback to float/string
                trade_ts = int(signal_timestamp)
                if trade_ts <= 0:
                    raise ValueError("Timestamp must be positive integer")
                print(f"[TRADE_TS FIX] {wallet[:10]}... | Using INTEGER timestamp: {trade_ts}")
            except (ValueError, TypeError) as e:
                # CRITICAL: Invalid timestamp - mark as processed and skip to prevent queue poisoning
                print(f"[TRADE_TS FIX] ⚠️ INVALID timestamp '{signal_timestamp}' for signal #{sig_id} - marking as processed and skipping")
                try:
                    await conn.execute("UPDATE signals SET is_processed = 1 WHERE id = ?", (sig_id,))
                    await conn.commit()
                except Exception:
                    pass
                return

        # Fallback to meta_data['ts'] only if DB timestamp is missing
        if trade_ts == 0 and meta_data_raw:
            try:
                meta = json.loads(meta_data_raw)
                trade_ts = safe_timestamp(meta.get('ts', 0))
                if trade_ts > 0:
                    print(f"[TRADE_TS FIX] {wallet[:10]}... | Fallback to meta_ts: {trade_ts}")
            except (json.JSONDecodeError, TypeError) as json_err:
                print(f"   [WARN] Failed to parse meta_data for signal #{sig_id}: {json_err}")

        # ==========================================
        # 🛑 TASK 3: HARD TIME FILTER - Skip signals older than 30 minutes
        # ==========================================
        # If maintest.py is lagging, we don't want to see trades from 3 hours ago.
        # This filter ensures we only process recent signals.
        # TASK 4: Use int(time.time()) for consistency with Unix timestamp storage
        # ==========================================
        current_time = int(time.time())
        if trade_ts > 0:
            time_gap_seconds = current_time - trade_ts
            time_gap_mins = time_gap_seconds / 60
            if time_gap_seconds > 1800:  # 30 minutes = 1800 seconds
                increment_skipped_signals_count()
                print(f"[V15 SKIP] Signal #{sig_id} is too old ({time_gap_mins:.0f} mins gap). Skipping to catch up. [Total skipped: {_signals_skipped_count}]")
                # Mark signal as processed to avoid re-processing
                try:
                    await conn.execute("UPDATE signals SET is_processed = 1 WHERE id = ?", (sig_id,))
                    await conn.commit()
                except Exception:
                    pass  # Best effort to mark as processed
                return

        # ==========================================
        # 🛠️ EXTRACT SLIPPAGE FROM META_DATA
        # maintest.py calculates on-chain slippage and saves it to meta_data
        # TASK 2: Use true_slippage_pct (reference price check) instead of usdc_diff_pct
        # TASK 3: Sanity Gate - mute impossible slippage on high-liquidity markets
        # ==========================================
        slippage_pct = None
        dominance = 0.0  # For sanity gate check
        accumulated_usd = m_usd  # Default to single trade size if not available
        if meta_data_raw:
            try:
                meta = json.loads(meta_data_raw)
                
                # ==========================================
                # 🛠️ TASK: EXTRACT ACCUMULATED SIZE
                # maintest.py saves total accumulated volume under 'accumulated' key
                # Use this for display to show true position size
                # ==========================================
                accumulated_usd = meta.get('accumulated', m_usd)
                
                on_chain = meta.get('on_chain', {})
                if on_chain:
                    # TASK 2: Use True Slippage (reference price check)
                    slippage_pct = on_chain.get('true_slippage_pct')

                    # Fallback to old usdc_diff_pct if true_slippage_pct not available
                    if slippage_pct is None:
                        slippage_pct = on_chain.get('usdc_diff_pct')

                    # ==========================================
                    # 🛡️ SLIPPAGE ALERT THRESHOLD
                    # Only alert on significant slippage for meaningful trades
                    # - Trades > $500: Alert if slippage > 2%
                    # - Trades < $500: Alert only if slippage > 10%
                    # ==========================================
                    if slippage_pct and slippage_pct > 2.0:
                        if m_usd >= 500 or slippage_pct > 10.0:
                            print(f"[💎 ON-CHAIN] {wallet[:10]}... | Slippage: {slippage_pct:.2f}% | Size: ${m_usd:,.0f}")

                    # ==========================================
                    # 🛡️ TASK 3: SANITY GATE (Anti-Ghost Filter)
                    # If slippage > 15% on high-liquidity market (dominance near 0%), mute slippage display
                    # Reason: Mathematically impossible to have 15% slippage on liquid market for small trade
                    # ==========================================
                    # Extract dominance from meta_data for sanity check
                    m_vol = meta.get('vol', 0)
                    if m_vol > 0:
                        dominance = (m_usd / (m_vol + m_usd)) * 100

                    if slippage_pct is not None and slippage_pct > 15.0 and dominance < 1.0:
                        print(f"[🚫 SANITY GATE] {wallet[:10]}... | Slippage {slippage_pct:.1f}% on high-liq market (dom={dominance:.2f}%) - MUTING (likely parsing error)")
                        slippage_pct = None  # Mute the slippage display
            except Exception as slippage_err:
                print(f"[SLIPPAGE EXTRACT] {wallet[:10]}... | Error: {slippage_err}")

        # ==========================================
        # 🛠️ TASK: CREATE DISPLAY SIZE VARIABLE
        # Use accumulated size if it's larger than single trade
        # This ensures alerts show true position size for multi-trade accumulation
        # ==========================================
        display_usd = max(m_usd, accumulated_usd)

        # Get market creation time
        market_created_ts = await get_market_creation_time(session, m_slug)

        # ==========================================
        # INSTANT FUNDING DETECTION
        # ==========================================
        funding_ts = safe_timestamp(res.get('ts', 0))
        is_instant_funding, instant_time_mins = detect_instant_funding(trade_ts, funding_ts)

        # ==========================================
        # CALCULATE UNREALIZED PNL
        # ==========================================
        pnl_data = await calculate_unrealized_pnl(session, wallet)
        unrealized_pnl_usd = pnl_data['total_pnl_usd']
        unrealized_pnl_pct = pnl_data['total_pnl_pct']

        # ==========================================
        # WALLET STATUS CLASSIFICATION
        # ==========================================
        wallet_status = classify_wallet_status(
            wallet_age_hours=wallet_age_hours,
            total_volume=pnl_data['total_invested'],
            predictions=total_bets
        )

        alpha_reason = None

        # 🛡️ ATOMIC ALERT LOCK (Deduplication) - GLOBAL CHECK
        # Создаем уникальный ключ для алерта (кошелек + рынок + тип сигнала)
        # Эта проверка предотвращает дубликаты для ВСЕХ типов алертов
        alert_key = f"alert_lock:{wallet}_{m_slug}_{signal_type}"
        # Пытаемся установить ключ в Redis на 60 секунд. 
        # Если ключ уже есть, значит алерт по этой сделке уже ушел.
        is_new = await redis_client.set(alert_key, "1", ex=60, nx=True)
        if not is_new:
            print(f"[DEDUP] Alert already sent for {wallet[:10]} on {m_slug} ({signal_type})")
            return

        # ==========================================
        # PRIORITY 1: DIRECT SOLO ALERT BRIDGE
        # ==========================================
        # If signal_type is FRESH ENTRY or WHALE, send alert immediately
        # without waiting for complex cluster analysis
        if signal_type and ("FRESH ENTRY" in signal_type or "WHALE" in signal_type):
            print(f"[DIRECT SOLO BRIDGE] {wallet[:10]}... | Type: {signal_type} | Sending immediate alert")

            source_label = res.get('label', 'Unknown')
            time_since_funding = round((trade_ts - funding_ts) / 60, 1) if trade_ts > 0 and funding_ts > 0 else 0

            # ==========================================
            # 🛠️ TASK 4: FIX DOMINANCE FALLBACK
            # Extract real market volume from meta_data
            # If m_vol is missing or 0, set dominance = 0.0 and log
            # ==========================================
            dominance = 0.0  # Default to 0.0 if volume not available
            m_vol = 0
            if meta_data_raw:
                try:
                    meta = json.loads(meta_data_raw)
                    m_vol = meta.get('vol', 0)
                    if m_vol > 0:
                        dominance = (m_usd / (m_vol + m_usd)) * 100
                    else:
                        log(f"[DOMINANCE] Market volume not available for signal {sig_id}, setting dominance=0.0")
                except (json.JSONDecodeError, TypeError) as vol_err:
                    log(f"[DOMINANCE] Failed to parse volume from meta_data for signal {sig_id}: {vol_err}")

            if "WHALE" in signal_type:
                # ==========================================
                # 🐋 WHALE ALERT - Raised threshold to $10,000
                # ==========================================
                if display_usd >= 10000:
                    alpha_reason = "🐋 WHALE"
                    print(f"[WHALE ALERT] {wallet[:10]}... | Size: ${display_usd:,.0f} | Preds: {total_bets}")
                    await alert_manager.alert_single_trader(
                        wallet=wallet,
                        wallet_status="🐋 WHALE",
                        market_title=market_title,
                        outcome=m_out,
                        entry_price=m_price,
                        usd_size=display_usd,
                        dominance=dominance,
                        funding_source=source_label,
                        unrealized_pnl_usd=unrealized_pnl_usd,
                        unrealized_pnl_pct=unrealized_pnl_pct,
                        is_instant_funding=is_instant_funding,
                        market_slug=m_slug,
                        slippage_pct=slippage_pct,
                        predictions=total_bets,  # Add predictions count
                        is_insider=is_insider  # TASK: Route to VIP group
                    )
                else:
                    print(f"[WHALE SKIP] {wallet[:10]}... | Size: ${display_usd:,.0f} < $10,000 threshold")
                    alpha_reason = None

            elif "FRESH ENTRY" in signal_type:
                # ==========================================
                # ⚡ SNIPER + 🧟‍♂️ SLEEPER AWAKENED LOGIC
                # ==========================================
                SNIPER_MAX_AGE_MINS = 1440  # 24 hours

                if time_since_funding < SNIPER_MAX_AGE_MINS and time_since_funding >= 0:
                    # Real Sniper: Fresh money, immediate action
                    alpha_reason = "⚡ FRESH CAPITAL SNIPER"
                    print(f"[FRESH SNIPER] {wallet[:10]}... | Funded {time_since_funding} mins ago | Size: ${display_usd:,.0f} | Preds: {total_bets}")
                    # ==========================================
                    # 🛠️ TASK 2: DYNAMIC ALERT HEADER
                    # Pass alert_header parameter to show correct title
                    # ==========================================
                    await alert_manager.alert_fresh_capital(
                        wallet=wallet,
                        mins_ago=time_since_funding,
                        market_title=market_title,
                        outcome=m_out,
                        size=display_usd,
                        source=source_label,
                        price=m_price,
                        market_slug=m_slug,
                        entry_time=trade_ts,
                        current_price=m_price,
                        pnl=unrealized_pnl_usd,
                        trades=total_bets,
                        dominance=dominance,
                        slippage_pct=slippage_pct,
                        alert_header="⚡ FRESH CAPITAL SNIPER",
                        is_insider=is_insider  # TASK: Route to VIP group
                    )
                elif total_bets < DORMANT_THRESHOLD_BETS:
                    # 🧟‍♂️ SLEEPER AWAKENED: Old wallet with low activity suddenly betting
                    alpha_reason = "🧟‍♂️ SLEEPER AWAKENED"
                    funding_time_str = f"{time_since_funding / 60:.1f} hours ago"
                    print(f"[SLEEPER AWAKENED] {wallet[:10]}... | Funded {funding_time_str} | Preds: {total_bets} (dormant wallet active again)")
                    # ==========================================
                    # 🛠️ TASK 2: DYNAMIC ALERT HEADER
                    # Pass alert_header parameter to show correct title for sleeper
                    # ==========================================
                    await alert_manager.alert_fresh_capital(
                        wallet=wallet,
                        mins_ago=time_since_funding,
                        market_title=market_title,
                        outcome=m_out,
                        size=display_usd,
                        source=source_label,
                        price=m_price,
                        market_slug=m_slug,
                        entry_time=trade_ts,
                        current_price=m_price,
                        pnl=unrealized_pnl_usd,
                        trades=total_bets,
                        dominance=dominance,
                        slippage_pct=slippage_pct,
                        alert_header="🧟‍♂️ SLEEPER AWAKENED",
                        is_insider=is_insider  # TASK: Route to VIP group
                    )
                else:
                    # Old wallet with >= 5 bets - skip (active trader, not a sleeper)
                    funding_time_str = f"{time_since_funding / 60:.1f} hours ago"
                    print(f"[FRESH SKIP] {wallet[:10]}... | Funded {funding_time_str} | Preds: {total_bets} - old active wallet, skipping")
                    alpha_reason = None

            # Update alpha_tag and exit early ONLY if alert was sent
            if alpha_reason:
                try:
                    await execute_query_with_retry_async(conn, "UPDATE signals SET alpha_tag = ? WHERE id = ?", (alpha_reason, sig_id))
                    await conn.commit()
                    print(f"   [TAGGED] Signal #{sig_id} -> {alpha_reason}")
                except Exception as db_err:
                    print(f"   [ERROR] Failed to update alpha_tag: {db_err}")
                return  # Exit early - solo alert sent, no cluster analysis needed
            else:
                # No solo alert sent - proceed to Priority 2 (Coordinated Attack Detection)
                print(f"[DIRECT SOLO BRIDGE] {wallet[:10]}... | No solo alert sent (alpha_reason=None), proceeding to cluster check")

        # ==========================================
        # PRIORITY 2: COORDINATED ATTACK DETECTION (V13 STANDARD)
        # ==========================================
        # V13 FIX: Allow ALL funders (CEX, Bridge, Relay, Private, etc.)
        # The 2-out-of-3 quality check inside check_coordinated_attack
        # will filter out noise. Alert manager handles generic sources.
        if res.get('funder'):
            # FIX: Ensure outcome is valid before checking for clusters
            if not m_out or m_out.strip() == '':
                print(f"[CLUSTER SKIP] Empty outcome for signal #{sig_id}")
            else:
                # Check for coordinated attack with 2-out-of-3 logic
                # m_out is always passed to ensure same-outcome matching
                # PASS trigger_usd_size (display_usd) and trigger_predictions (total_bets) for correct display
                alpha_reason = await check_coordinated_attack(
                    session, conn, wallet, res, m_slug, m_out, market_title,
                    trigger_usd_size=display_usd,
                    trigger_predictions=total_bets
                )

                # If coordinated attack detected, skip single wallet alerts
                if alpha_reason:
                    print(f"   [TAGGED] Signal #{sig_id} -> {alpha_reason}")
                    try:
                        # FIX: Use retry wrapper for UPDATE to handle "database is locked"
                        await execute_query_with_retry_async(conn, "UPDATE signals SET alpha_tag = ? WHERE id = ?", (alpha_reason, sig_id))
                        await conn.commit()
                    except aiosqlite.OperationalError as db_err:
                        print(f"   [ERROR] Failed to update alpha_tag (DB locked): {db_err}")
                    except Exception as db_err:
                        print(f"   [ERROR] Failed to update alpha_tag: {db_err}")
                    return  # Exit after coordinated attack

        # ==========================================
        # PRIORITY 2: SINGLE WALLET ALERTS
        # ==========================================

        # A. MIXER
        if "Tornado" in str(res.get('label', '')) or "Mixer" in str(res.get('source', '')):
            alpha_reason = "🌪️ MIXER"
            await alert_manager.alert_mixer_detected(
                wallet=wallet,
                source=res['label'],
                market=m_q,
                outcome=m_out,
                size=display_usd,
                market_slug=m_slug,
                entry_time=trade_ts,
                price=m_price,
                current_price=m_price,
                pnl=unrealized_pnl_usd,
                trades=total_bets,
                dominance=dominance,
                slippage_pct=slippage_pct,
                is_insider=is_insider  # TASK: Route to VIP group
            )

        # ==========================================
        # B. MAINTEST DETECTED ALERTS (PROVEN only - WHALE/FRESH handled above)
        # ==========================================
        elif signal_type:
            source_label = res.get('label', 'Unknown')
            time_since_funding = round((trade_ts - funding_ts) / 60, 1) if trade_ts > 0 and funding_ts > 0 else 0

            if "PROVEN" in signal_type:
                alpha_reason = "👑 PROVEN INSIDER"
                print(f"[PROVEN ALERT] {wallet[:10]}... | Size: ${display_usd:,.0f} | Preds: {total_bets} | Slippage: {slippage_pct}%")
                # TASK 3: is_insider flag already set at top of function
                if is_insider:
                    print(f"[ROUTING] Sending high-priority alert to VIP group for wallet {wallet}")
                await alert_manager.alert_single_trader(
                    wallet=wallet,
                    wallet_status="👑 PROVEN INSIDER",
                    market_title=market_title,
                    outcome=m_out,
                    entry_price=m_price,
                    usd_size=display_usd,
                    dominance=dominance,
                    funding_source=source_label,
                    unrealized_pnl_usd=unrealized_pnl_usd,
                    unrealized_pnl_pct=unrealized_pnl_pct,
                    is_instant_funding=is_instant_funding,
                    market_slug=m_slug,
                    slippage_pct=slippage_pct,
                    predictions=total_bets,  # Add predictions count
                    is_insider=is_insider  # TASK 3: Route to VIP group
                )
            # Skip WHALE and FRESH ENTRY here - already handled in PRIORITY 1
            # ACCUMULATION is skipped - no alerts for accumulation anymore

        # C. INSTANT FUNDING
        elif is_instant_funding:
            alpha_reason = f"⚡ INSTANT ({instant_time_mins}m)"
            # ==========================================
            # 🛠️ DEBUG: Log exact values before sending alert
            # ==========================================
            print(f"[INSTANT ALERT] {wallet[:10]}... | trade_ts={trade_ts} | funding_ts={funding_ts} | instant_time_mins={instant_time_mins}")
            await alert_manager.alert_instant_funding(
                wallet=wallet,
                mins_ago=instant_time_mins,
                source=res['label'],
                market_title=market_title,
                outcome=m_out,
                size=display_usd,
                price=m_price,
                market_slug=m_slug,
                entry_time=trade_ts,
                current_price=m_price,
                pnl=unrealized_pnl_usd,
                trades=total_bets,
                dominance=dominance,
                slippage_pct=slippage_pct,
                is_insider=is_insider  # TASK: Route to VIP group
            )

        # C. EARLY BIRD
        elif market_created_ts > 0 and trade_ts > 0 and (trade_ts - market_created_ts) < 600:
            delta_s = trade_ts - market_created_ts
            alpha_reason = f"🦅 EARLY BIRD (+{delta_s}s)"
            await alert_manager.alert_fresh_capital(
                wallet=wallet,
                mins_ago=0,
                market_title=market_title,
                outcome=m_out,
                size=display_usd,
                source=f"Sniped in {delta_s}s after listing",
                price=m_price,
                market_slug=m_slug,
                entry_time=trade_ts,
                current_price=m_price,
                pnl=unrealized_pnl_usd,
                trades=total_bets,
                dominance=dominance,
                slippage_pct=slippage_pct,
                is_insider=is_insider  # TASK: Route to VIP group
            )

        # D. FRESH CAPITAL
        elif res.get('ts', 0) > 0 and trade_ts > 0:
            diff = trade_ts - res['ts']
            if 0 < diff < 3600 and display_usd > 100:
                alpha_reason = "⚡ FRESH WALLET"
                mins_ago = round(diff/60, 1)
                # ==========================================
                # 🛠️ DEBUG: Log exact values before sending alert
                # ==========================================
                print(f"[FRESH ALERT] {wallet[:10]}... | trade_ts={trade_ts} | funding_ts={res['ts']} | diff={diff}s | mins_ago={mins_ago}")
                await alert_manager.alert_fresh_capital(
                    wallet=wallet,
                    mins_ago=mins_ago,
                    market_title=market_title,
                    outcome=m_out,
                    size=display_usd,
                    source=res['label'],
                    price=m_price,
                    market_slug=m_slug,
                    entry_time=trade_ts,
                    current_price=m_price,
                    pnl=unrealized_pnl_usd,
                    trades=total_bets,
                    dominance=dominance,
                    slippage_pct=slippage_pct,
                    is_insider=is_insider  # TASK: Route to VIP group
                )

        # E. ELITE
        if not alpha_reason and perf and perf[1]:
            alpha_reason = "💎 ELITE"
            await alert_manager.alert_elite_move(
                wallet=wallet,
                win_rate=perf[2],
                market_title=market_title,
                outcome=m_out,
                size=display_usd,
                price=m_price,
                market_slug=m_slug,
                entry_time=trade_ts,
                current_price=m_price,
                pnl=unrealized_pnl_usd,
                trades=perf[3],  # FIX: Pass total_trades from DB
                dominance=dominance,
                slippage_pct=slippage_pct,
                is_insider=is_insider  # TASK: Route to VIP group
            )

        # ==========================================
        # UPDATE ALPHA TAG
        # ==========================================
        if alpha_reason:
            try:
                # FIX: Use retry wrapper for UPDATE to handle "database is locked"
                await execute_query_with_retry_async(conn, "UPDATE signals SET alpha_tag = ? WHERE id = ?", (alpha_reason, sig_id))
                await conn.commit()
                print(f"   [TAGGED] Signal #{sig_id} -> {alpha_reason}")
            except aiosqlite.OperationalError as db_err:
                print(f"   [ERROR] Failed to update alpha_tag for signal #{sig_id} (DB locked): {db_err}")
            except Exception as db_err:
                print(f"   [ERROR] Failed to update alpha_tag for signal #{sig_id}: {db_err}")

    except Exception as e:
        crash_log("analyze_and_alert", e, traceback.format_exc())


async def process_batch():
    conn = await get_async_db_connection()
    # ==========================================
    # 🚨 SPRINT 2.2: SQL HARDENING - Clean Queue
    # ==========================================
    # Only process signals that:
    # 1. is_processed = 0 (not yet traced)
    # 2. is_analyzed = 1 (passed maintest Hybrid Gatekeeper)
    # 3. type NOT LIKE '%🤖%' (exclude bot signals)
    # 4. usd_size >= 100 (minimum volume threshold)
    # EXCEPTION: Proven insiders (alpha_tag LIKE '%PROVEN%') bypass usd_size check
    # ==========================================
    # FIX C: Add 48-hour cutoff to avoid processing stale signals
    cutoff_ts = int(time.time()) - (48 * 3600)  # 48 hours ago
    async with await conn.execute("""
        SELECT id, trader_addr, timestamp, usd_size, alpha_tag FROM signals
        WHERE is_processed = 0
          AND is_analyzed = 1
          AND timestamp >= ?
          AND (
              alpha_tag LIKE '%PROVEN%'
              OR type LIKE '%FRESH ENTRY%'
              OR type LIKE '%WHALE%'
              OR type LIKE '%ACCUMULATION%'
          )
        ORDER BY
            CASE WHEN alpha_tag LIKE '%PROVEN%' THEN 0 ELSE 1 END,
            id
        LIMIT 50
    """, (cutoff_ts,)) as cursor:
        rows = await cursor.fetchall()

    if not rows:
        await conn.close()
        return False

    log(f"🚀 Processing {len(rows)} signals from queue (is_processed = 0)")

    # TASK 3: Queue status log - show remaining signals and lag
    async with await conn.execute(
        "SELECT COUNT(*) FROM signals WHERE is_processed = 0 AND is_analyzed = 1"
    ) as cursor:
        remaining_count = (await cursor.fetchone())[0]
    if remaining_count > 0:
        async with await conn.execute("""
            SELECT timestamp FROM signals
            WHERE is_processed = 0 AND is_analyzed = 1
            ORDER BY id ASC LIMIT 1
        """) as cursor:
            oldest_signal = await cursor.fetchone()
        if oldest_signal and oldest_signal[0]:
            try:
                # TASK 4: Handle both INTEGER and STRING representations of Unix timestamps
                oldest_ts_val = oldest_signal[0]

                # Try to parse as integer (handles both int and string representations)
                try:
                    oldest_timestamp = int(oldest_ts_val)
                except (ValueError, TypeError):
                    # Fallback: try float conversion
                    oldest_timestamp = int(float(oldest_ts_val))

                now_ts = int(time.time())
                lag_secs = now_ts - oldest_timestamp
                lag_mins = lag_secs / 60
                log(f"[QUEUE STATUS] Remaining (approved): {remaining_count} signals | Lag: {lag_secs}s ({lag_mins:.1f} mins)")
            except Exception as e:
                log(f"[QUEUE STATUS] Remaining (approved): {remaining_count} signals | Could not calculate lag: {e}")
        else:
            log(f"[QUEUE STATUS] Remaining (approved): {remaining_count} signals")

    # Кэш трейсинга для текущего батча — предотвращает дублирование
    _batch_trace_cache = {}

    async with aiohttp.ClientSession() as session:
        for r in rows:
            sig_id = r[0]
            w = r[1]
            signal_timestamp = r[2]  # INTEGER (Unix timestamp) or old ISO string
            usd_size = r[3]
            alpha_tag = r[4]

            # ==========================================
            # 🛑 ALPHA_TAG FILTER: Skip signals without valid alpha_tag
            # If alpha_tag is NULL/empty, check usd_size threshold again
            # Only process signals that are "Proven" or meet USD threshold
            # ==========================================
            if not alpha_tag or alpha_tag.strip() == "":
                if usd_size < 100:
                    # Mark as processed (ignore junk) and skip
                    await execute_query_with_retry_async(conn, """
                        UPDATE signals SET is_processed = 1
                        WHERE id = ?
                    """, (sig_id,))
                    await conn.commit()
                    continue

            # Parse trade timestamp from signal
            # STAGE 1 FIX: Simple integer parsing only - no fallback to float/string
            trade_ts = 0
            if signal_timestamp:
                try:
                    # Try integer parsing only
                    trade_ts = int(signal_timestamp)
                    if trade_ts <= 0:
                        raise ValueError("Timestamp must be positive integer")
                except (ValueError, TypeError):
                    # Skip this signal - invalid timestamp
                    print(f"[PROCESS_BATCH] ⚠️ INVALID timestamp '{signal_timestamp}' for signal #{sig_id} - skipping")
                    await execute_query_with_retry_async(conn, """
                        UPDATE signals SET is_processed = 1
                        WHERE id = ?
                    """, (sig_id,))
                    await conn.commit()
                    continue

            try:
                # Проверяем кэш текущего батча (дубли в одном батче)
                w_lower = w.lower()
                if w_lower in _batch_trace_cache:
                    res = _batch_trace_cache[w_lower]
                    log(f"   [CACHE HIT] {w[:8]}.. — reusing trace from this batch")
                else:
                    # Проверяем кэш БД (недавно трейсили этот кошелёк)
                    async with await conn.execute(
                        "SELECT ultimate_source, source_label, funder_address, funding_ts, confidence_score "
                        "FROM funding_sources WHERE wallet_addr = ? AND last_updated > ?",
                        (w_lower, int(time.time()) - 3600)  # кэш на 1 час
                    ) as cursor:
                        cached_db = await cursor.fetchone()

                    if cached_db:
                        res = {
                            "source": cached_db["ultimate_source"],
                            "label": cached_db["source_label"],
                            "funder": cached_db["funder_address"],
                            "ts": cached_db["funding_ts"] or 0,
                            "conf": cached_db["confidence_score"] or 0.5,
                        }
                        log(f"   [DB CACHE] {w[:8]}.. — reusing recent trace from DB")
                    else:
                        res = await trace_funding_source(session, w, trade_ts)
                        await upsert_result(w, res)

                    _batch_trace_cache[w_lower] = res

                await analyze_and_alert(session, conn, w, res, sig_id)
                log(f"   {w[:8]}.. <- {res['label']} (Signal #{sig_id})")

                # ==========================================
                # 🚨 MARK AS PROCESSED: Update is_processed by signal ID (NOT trader_addr)
                # FIX: Use WHERE id = ? to mark only this specific signal as processed
                # ==========================================
                await execute_query_with_retry_async(conn, """
                    UPDATE signals SET is_processed = 1
                    WHERE id = ?
                """, (sig_id,))
                await conn.commit()

                # TASK 3: Real-time mode - Low latency mode
                # Safe for PolygonScan free tier (5 calls/sec limit)
                # One trace ≈ 3 API calls, 50 signals × 3 = 150 calls / (0.6s × 50) = 5 req/sec
                await asyncio.sleep(0.6)
            except Exception as e:
                crash_log(f"process_batch signal #{sig_id}", e, traceback.format_exc())

    await conn.close()
    return True


async def watchdog_loop():
    """
    Health monitor for the entire system.
    Checks two things every 5 minutes:

    1. Block Height Staleness — if the sync_status table hasn't updated
       in 5+ minutes, maintest.py is dead (not just quiet).
       Polygon produces ~2 blocks/second, so ANY 5-minute gap = scanner crash.
       FIX 4: Uses get_latest_sync_block() from config.py.

    2. Queue Overflow — if >500 signals are waiting to be processed,
       funding_tracer is falling behind and needs attention.

    Avoids false alarms from "quiet market" periods by using block height
    instead of signal count as the liveness indicator.
    """
    log("💓 Watchdog started — checking every 5 minutes")
    # Wait 10 minutes on startup before first check — let maintest.py fully initialize
    await asyncio.sleep(600)

    last_known_block = 0  # Track last known block for change detection

    while True:
        try:
            conn = await get_async_db_connection()
            now = int(time.time())

            # ── Check 1: Block height staleness (FIX 4: use get_latest_sync_block_async) ─────
            current_block = await get_latest_sync_block_async()
            scanner_alive = True

            if current_block > 0:
                age_minutes = (now - last_known_block) / 60 if last_known_block > 0 else 0

                # Check if block has changed since last check
                if current_block == last_known_block and last_known_block > 0:
                    # Block hasn't changed - check how long
                    if age_minutes > 5:
                        scanner_alive = False
                        log(f"[WATCHDOG] 🚨 Scanner appears dead! Block {current_block} unchanged for {age_minutes:.0f} min")
                        await alert_manager.send_telegram(
                            f"🚨 <b>WATCHDOG: maintest.py мёртв?</b>\n\n"
                            f"Последний блок в БД: <b>{current_block}</b>\n"
                            f"Блок не обновляется: <b>{age_minutes:.0f} мин</b>\n\n"
                            f"Polygon даёт ~2 блока/сек. Если прошло >5 мин — сканер упал.\n"
                            f"Проверь: <code>pm2 logs maintest</code>"
                        )
                else:
                    # Block has changed - scanner is alive
                    last_known_block = current_block

            else:
                # No sync_status yet — system just started, not an error
                log("[WATCHDOG] No sync_status yet — system starting up")

            # ── Check 2: Queue overflow ───────────────────────────────────────
            async with await conn.execute(
                "SELECT COUNT(*) FROM signals WHERE is_processed=0 AND is_analyzed=1"
            ) as cursor:
                queue_size = (await cursor.fetchone())[0]

            await conn.close()

            if queue_size > 1000:
                log(f"[WATCHDOG] ⚠️ Queue overflow: {queue_size} unprocessed signals")
                await alert_manager.send_telegram(
                    f"⚠️ <b>WATCHDOG: Очередь переполнена</b>\n\n"
                    f"<b>{queue_size}</b> одобренных сигналов ждут трейсинга (is_analyzed=1).\n"
                    f"Возможные причины:\n"
                    f"— Всплеск активности на рынке\n"
                    f"— PolygonScan API лагает\n"
                    f"— funding_tracer завис\n\n"
                    f"Проверь: <code>pm2 logs funding_tracer_test</code>"
                )

            if scanner_alive and queue_size <= 1000:
                log(f"[WATCHDOG] ✅ System healthy. Block: {current_block} | Queue: {queue_size}")

        except Exception as e:
            crash_log("watchdog_loop", e, traceback.format_exc())

        await asyncio.sleep(300)  # Check every 5 minutes


async def main():
    global LABELS_CACHE

    # Первым делом — проверка окружения
    from config import check_required_env
    check_required_env()

    # STAGE 2: Schema migrations handled by standalone migrate.py
    # Run 'python migrate.py' ONCE before starting workers

    # ==========================================
    # 🏷️ LOAD CUSTOM LABELS (once at startup)
    # ==========================================
    LABELS_CACHE = await load_all_labels_async()
    log(f"[LABELS] Loaded {len(LABELS_CACHE)} custom labels")

    # ==========================================
    # 🧹 STARTUP CLEANUP — 3-уровневая стратегия
    # ==========================================
    conn = await get_async_db_connection()

    # Уровень 1: Очистить stale (>48h) — они уже не актуальны
    stale_cutoff = int(time.time()) - (48 * 3600)
    async with await conn.execute(
        "SELECT COUNT(*) FROM signals WHERE is_processed = 0 AND timestamp < ?",
        (stale_cutoff,)
    ) as cursor:
        stale_count = (await cursor.fetchone())[0]
    if stale_count > 0:
        await conn.execute(
            "UPDATE signals SET is_processed = 1 WHERE is_processed = 0 AND timestamp < ?",
            (stale_cutoff,)
        )
        await conn.commit()
        log(f"[🧹 CLEANUP] Cleared {stale_count} stale signals (>48h old).")

    # Уровень 2: Очистить средний бэклог (30min - 48h) — старые сигналы рынок уже ушёл
    backlog_cutoff = int(time.time()) - 1800  # 30 минут
    async with await conn.execute(
        "SELECT COUNT(*) FROM signals WHERE is_processed = 0 AND timestamp < ? AND timestamp >= ?",
        (backlog_cutoff, stale_cutoff)
    ) as cursor:
        backlog_count = (await cursor.fetchone())[0]
    if backlog_count > 0:
        await conn.execute(
            "UPDATE signals SET is_processed = 1 WHERE is_processed = 0 AND timestamp < ?",
            (backlog_cutoff,)
        )
        await conn.commit()
        log(f"[🧹 CLEANUP] Cleared {backlog_count} backlog signals (30min-48h). Recent signals PRESERVED.")
    else:
        log("[🧹 CLEANUP] No backlog. System is clean.")

    # Уровень 3: Сигналы моложе 30 минут — НЕ ТРОГАТЬ. Они будут обработаны нормально.
    async with await conn.execute(
        "SELECT COUNT(*) FROM signals WHERE is_processed = 0 AND timestamp >= ?",
        (backlog_cutoff,)
    ) as cursor:
        fresh_count = (await cursor.fetchone())[0]
    if fresh_count > 0:
        log(f"[🧹 CLEANUP] {fresh_count} recent signals preserved (< 30min old) — will process normally.")
    await conn.close()

    # ==========================================
    # 💓 HEARTBEAT ALERT: Notify Telegram on startup
    # ==========================================
    try:
        await alert_manager.send_telegram("🚀 <b>System Restarted:</b> Monitoring LIVE trades only.")
        log("💓 Heartbeat alert sent to Telegram")
    except Exception as e:
        log(f"⚠️ WARNING: Failed to send heartbeat to Telegram: {e}")

    log("🕵️ Funding Tracer V15 (Redis Consumer Mode) Started")

    # 💓 Start watchdog health monitor
    asyncio.create_task(watchdog_loop())

    # ==========================================
    # 🚀 STAGE 4 TASK 1: REDIS CONSUMER (FIX-1: Pub/Sub)
    # Replace DB polling with Redis Pub/Sub subscriber
    # ==========================================
    log("[REDIS] Starting signals_broadcast Pub/Sub subscriber...")

    # Subscribe to broadcast channel
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("signals_broadcast")
    log("[REDIS] Subscribed to signals_broadcast channel")

    # Create shared aiohttp session for all HTTP requests
    async with aiohttp.ClientSession() as session:
        # Start DB backlog loop in background with shared session
        asyncio.create_task(db_backlog_loop(session))
        
        while True:
            try:
                # Listen for messages from Pub/Sub
                async for message in pubsub.listen():
                    if message["type"] != "message":
                        continue

                    signal_data = json.loads(message["data"])

                    # Process the signal directly from Redis data (no DB SELECT needed)
                    try:
                        conn = await get_async_db_connection()
                        # Pass session and signal_data dict to process_single_signal
                        await process_single_signal(conn, session, signal_data, LABELS_CACHE)
                        await conn.close()
                    except Exception as process_err:
                        log(f"[REDIS] Error processing signal: {process_err}")
                        crash_log("funding_tracer_redis_process", process_err, traceback.format_exc())

            except Exception as e:
                log(f"[REDIS] Consumer error: {e}")
                crash_log("funding_tracer_redis_consumer", e, traceback.format_exc())
                await asyncio.sleep(1)

# ==========================================
# 🚀 STAGE 4 TASK 1: SINGLE SIGNAL PROCESSOR
# Extracted from process_batch for Redis consumer
# ==========================================
async def process_single_signal(conn, session, signal_data, labels_cache):
    """
    Process a single signal from Redis queue.

    signal_data is a dict from Redis containing:
    - wallet, market_slug, market_title, outcome, usd_size, price,
      predictions, dominance, alpha_tag, is_insider, timestamp, meta_dict
    """
    wallet = signal_data.get('wallet', '')
    slug = signal_data.get('market_slug', '')
    timestamp = signal_data.get('timestamp', '')
    
    # ==========================================
    # 🛡️ DEDUPLICATION CHECK - Prevent concurrent processing
    # ==========================================
    task_key = f"{wallet}_{timestamp}"
    if task_key in PROCESSING_NOW:
        log(f"[DEDUP] Signal already processing: {task_key[:50]}...")
        return
    PROCESSING_NOW.add(task_key)
    
    try:
        market_title = signal_data.get('market_title', '')
        outcome = signal_data.get('outcome', '')
        usd_size = signal_data.get('usd_size', 0)
        price = signal_data.get('price', 0)
        predictions = signal_data.get('predictions', 0)
        dominance = signal_data.get('dominance', 0)
        alpha_tag = signal_data.get('alpha_tag', '')
        is_insider = signal_data.get('is_insider', False)
        meta_dict = signal_data.get('meta_dict', {})

        # ==========================================
        # 🛑 ALPHA_TAG FILTER
        # ==========================================
        if not alpha_tag or alpha_tag.strip() == "":
            if usd_size < 100:
                log(f"[SKIP] Signal for {wallet[:10]}... - no alpha_tag and usd_size < $100")
                return

        log(f"[PROCESS] Processing signal for {wallet[:10]}... | ${usd_size:,.0f} | {alpha_tag} | {slug[:40]}...")

        # ==========================================
        # 1. TRACE FUNDING SOURCE
        # ==========================================
        # Parse trade timestamp from meta_dict or use current time
        trade_ts = 0
        if isinstance(meta_dict, dict) and meta_dict.get('ts'):
            trade_ts = int(meta_dict.get('ts', 0))
        if trade_ts <= 0:
            trade_ts = int(time.time())

        res = await trace_funding_source(session, wallet, trade_ts)

        # Save to funding_sources table
        await upsert_result(wallet, res)

        # ==========================================
        # 2. ANALYZE AND ALERT
        # ==========================================
        # Pass all data directly - no need to query DB again
        # We create a synthetic sig_id for tracking (uses wallet+timestamp hash)
        temp_sig_id = f"{wallet}_{timestamp}"

        # For analyze_and_alert, we need to ensure signal exists in DB first
        # Try to find or create the signal record
        sig_id = None
        try:
            async with await conn.execute("""
                SELECT id FROM signals
                WHERE trader_addr = ? AND timestamp = ?
                ORDER BY id DESC LIMIT 1
            """, (wallet, timestamp)) as cursor:
                row = await cursor.fetchone()
                if row:
                    sig_id = row['id']
        except Exception:
            pass  # Signal may not exist yet - backlog will handle it

        # Call analyze_and_alert with all data passed directly
        await analyze_and_alert(session, conn, wallet, res, sig_id, signal_data)

        # ==========================================
        # 3. MARK AS PROCESSED (best effort)
        # ==========================================
        # Try to update is_processed flag - if signal doesn't exist yet, ignore
        # The db_backlog_loop will mark it later
        try:
            await execute_query_with_retry_async(conn, """
                UPDATE signals SET is_processed = 1 
                WHERE trader_addr = ? AND timestamp = ? AND is_processed = 0
            """, (wallet, timestamp))
            await conn.commit()
        except Exception:
            pass  # Signal may not exist yet - backlog will handle it
            
    finally:
        # ==========================================
        # 🛡️ CLEANUP - Remove from processing set
        # ==========================================
        PROCESSING_NOW.discard(task_key)


async def db_backlog_loop(shared_session=None):
    """
    Background task: Process signals with is_processed = 0 from DB.
    Runs every 5-10 seconds as insurance against Redis failures.
    
    shared_session: Optional aiohttp session from main(). If not provided, creates own.
    """
    log("[BACKLOG] DB backlog loop started - checking every 5 seconds")

    # Use shared session if provided, otherwise create own
    own_session = None
    if shared_session is None:
        own_session = aiohttp.ClientSession()
    
    session = shared_session if shared_session else own_session

    while True:
        try:
            await asyncio.sleep(5)

            conn = await get_async_db_connection()

            # Find unprocessed signals (is_processed = 0 AND is_analyzed = 1)
            # Only process signals approved by AI (is_analyzed = 1)
            async with await conn.execute("""
                SELECT id, trader_addr, timestamp, usd_size, alpha_tag, slug, outcome,
                       price, predictions, meta_data, market_q, market_title
                FROM signals
                WHERE is_processed = 0 AND is_analyzed = 1
                ORDER BY id ASC
                LIMIT 20
            """) as cursor:
                rows = await cursor.fetchall()

            if not rows:
                await conn.close()
                continue

            log(f"[BACKLOG] Processing {len(rows)} signals from DB backlog")

            for row in rows:
                try:
                    sig_id = row['id']
                    wallet = row['trader_addr']
                    timestamp = row['timestamp']
                    alpha_tag = row['alpha_tag']
                    usd_size = row['usd_size']

                    # Skip if no alpha_tag and below threshold
                    if (not alpha_tag or alpha_tag.strip() == "") and usd_size < 100:
                        await execute_query_with_retry_async(conn, """
                            UPDATE signals SET is_processed = 1 WHERE id = ?
                        """, (sig_id,))
                        await conn.commit()
                        continue

                    # Build signal_data dict from DB row
                    signal_data = {
                        'wallet': wallet,
                        'market_slug': row['slug'],
                        'market_title': row['market_title'] or row['market_q'],
                        'outcome': row['outcome'],
                        'usd_size': usd_size,
                        'price': row['price'] or 0,
                        'predictions': row['predictions'] or 0,
                        'alpha_tag': alpha_tag or '',
                        'timestamp': timestamp,
                        'meta_dict': json.loads(row['meta_data']) if row['meta_data'] else {}
                    }

                    # Process the signal with shared session
                    await process_single_signal(conn, session, signal_data, LABELS_CACHE)

                except Exception as e:
                    crash_log("db_backlog_process", e, traceback.format_exc())

            await conn.close()

        except Exception as e:
            log(f"[BACKLOG] Error: {e}")
            await crash_log("db_backlog_loop", e, traceback.format_exc())
            await asyncio.sleep(5)
    
    # Cleanup own session if we created one
    if own_session:
        await own_session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
