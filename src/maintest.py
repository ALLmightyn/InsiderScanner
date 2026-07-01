
# --- PATH BOOTSTRAP ---
import sys as _sys, os as _os
_SRC_DIR = _os.path.dirname(_os.path.abspath(__file__))
_PROJECT_DIR = _os.path.dirname(_SRC_DIR)
for _p in [_SRC_DIR, _os.path.join(_PROJECT_DIR, 'config')]:
    if _p not in _sys.path:
        _sys.path.insert(0, _p)
del _sys, _os, _SRC_DIR, _PROJECT_DIR, _p
import asyncio
import aiohttp
import aiofiles
import sys
import json
import os
import time
import traceback
from datetime import datetime, timezone
from cachetools import TTLCache
import alert_manager
from config import (
    crash_log, crash_log_async, CRASH_LOG_PATH, update_sync_status_async, CASINO_KEYWORDS,
    get_async_db_connection, get_persistent_async_db_connection, redis_client,
    USD_LIMIT_NEW, USD_LIMIT_OLD, PRED_LIMIT_NEW,
    BASE_DIR, DB_FILENAME,
)

# ==========================================
# 🎨 UI COLORS
# ==========================================
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    CYAN = '\033[96m'
    MAGENTA = '\033[95m'
    BOLD = '\033[1m'
    RESET = '\033[0m'
    CLEAR = '\033[K'

# ==========================================
# ⚙️ SETTINGS
# ==========================================

# 🔥 ПРОКСИ (Только для AI запросов)
# AI_PROXY = "http://<REDACTED-ROTATED>"
#AI_PROXY = "http://<REDACTED-ROTATED>"

#RPC_LIST = [
#    "https://polygon-mainnet.g.alchemy.com/v2/<REDACTED-ROTATED>"
#]

# ==========================================
# 🔗 RPC LOAD BALANCER - DOUBLE ENGINE
# ==========================================
_rpc_env = os.getenv("RPC_LIST", "")
RPC_LIST = [u.strip() for u in _rpc_env.split(",") if u.strip()]
if not RPC_LIST:
    # Fallback на публичные если env не задан (для локальной разработки)
    print("[WARNING] RPC_LIST env not set — using public RPCs (slower)")
    RPC_LIST = [
        "https://polygon-rpc.com",
        "https://rpc.ankr.com/polygon",
    ]

# Global RPC index for round-robin load balancing
current_rpc_idx = 0

# Task 2: Track disabled RPCs (401 auth failures) for the session
DISABLED_RPC_SET = set()

# ==========================================
# 🔒 SAFE MULTI-FETCHER SETTINGS
# ==========================================
RPC_SEMAPHORE = asyncio.Semaphore(10)  # Task 1: 4 concurrent requests (2 private keys + 2 public fallbacks)

# ==========================================
# ⛓️ STAGE 3: HISTORICAL CATCH-UP
# ==========================================
MAX_CATCHUP_BLOCKS = 500  # Maximum blocks to process per iteration (prevents RPC overload)

# ==========================================
# 🛡️ API SHIELD - RATE LIMITING
# ==========================================
DATA_API_SEMAPHORE = asyncio.Semaphore(10)  # Task 2: Single concurrent Data API request

# BASE_DIR and DB_FILENAME are imported from config below

# Настройки анализа
CONCURRENT_ANALYSIS = 30
CLUSTER_MIN_PEOPLE = 3
CLUSTER_WINDOW = 3600
MAX_PRICE_FILTER = 0.9
CLUSTER_ENTRY_USD = 100
SCAN_MIN_USD = 10

# AI Filter - SPRINT 2: Now uses markets_whitelist table (market_discovery.py)
USE_AI_FILTER = True  # Used for whitelist check, not direct API calls

# System Params
WALLET_CACHE_TTL = 3600
TOKEN_CONTRACT = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"
NULL_ADDR = "0x0000000000000000000000000000000000000000"
POLYMARKET_API_URL = "https://gamma-api.polymarket.com/markets"
DATA_API_URL = "https://data-api.polymarket.com/traded"
TOPIC_TRANSFER_SINGLE = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
TOPIC_TRANSFER_BATCH = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f755"

# Polymarket Contracts
POLYMARKET_CTF = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e".lower()  # CTF Exchange
POLYMARKET_CONDITIONS = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045".lower()  # Conditions
# POLYMARKET_ROUTER removed — was placeholder 0x1234...5678, never used in real logic

# ==========================================
# 🚀 PREDICTIONS CACHE - TTLCache (Stage 4)
# ==========================================
# Cache wallet predictions for 5 minutes to avoid hammering Polymarket API
# Using TTLCache for automatic expiration and LRU eviction
PREDICTIONS_CACHE = TTLCache(maxsize=10000, ttl=300)  # 5 minutes TTL

# ==========================================
# 🔗 ON-CHAIN PRICE CALCULATION
# ==========================================
# USDC Contracts on Polygon (both Native and Bridged)
USDC_CONTRACTS = [
    "0x2791bca1f2de4661ed88a30c99a7a9449aa84174".lower(),  # Native USDC
    "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359".lower(),  # Bridged USDC
    "0x1a1A3b2ff016332E866787B311fcB63928464509".lower(),  # EURC on Polygon
    "0x6e834E9D04ad6CD281341418F597E5333b60F966".lower(),  # PYUSD on Polygon
]
TOPIC_TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"  # ERC20 Transfer

# --- Global State ---
ACCUMULATORS = {}  # Not a cache, but accumulator for batch detection - keep as dict
WATCHLIST = {}
TRADER_COOLDOWNS = {}
# Stage 4: Using TTLCache for automatic memory management
WALLET_CACHE = TTLCache(maxsize=15000, ttl=3600)  # 1 hour TTL
MARKET_CACHE = TTLCache(maxsize=5000, ttl=3600)  # 1 hour TTL for market titles
CACHE_TTL = 3600  # Phase 1: 1 hour cache (for reference, TTLCache handles this internally)
CURRENT_BLOCK = 0
LAST_PROCESSED_BLOCK = 0  # Track last processed block for lag calculation
SEARCH_START_TIME = time.time()
ANALYSIS_QUEUE = None  # Initialized in main() after event loop starts
KNOWN_INSIDERS = set()
LAST_SCAN = "Waiting..."
API_SESSION = None

# ==========================================
# 🚀 STAGE 4 TASK 4: DB WRITE QUEUE
# ==========================================
# Batch database writes to reduce disk I/O and WAL journal pressure
DB_WRITE_QUEUE = None  # Initialized in main() after event loop starts
DB_BATCH_SIZE = 50  # Write to DB every 50 signals
DB_BATCH_TIMEOUT = 5.0  # Or every 5 seconds, whichever comes first

# ==========================================
# 📊 ON-CHAIN PRICE STATISTICS
# ==========================================
ONCHAIN_STATS = {
    "total_trades": 0,
    "onchain_success": 0,
    "slippage_detected": 0,
    "avg_slippage_pct": 0.0,
    "total_slippage_sum": 0.0
}

# ==========================================
# 📊 DATABASE SYSTEM
# ==========================================


async def init_storage_async():
    """
    STAGE 2: Initialize database with basic CREATE TABLE statements using async DB.
    
    IMPORTANT: All ALTER TABLE migrations have been moved to migrate.py.
    Run 'python migrate.py' ONCE before starting workers.
    
    This function now only creates tables if they don't exist.
    """
    print(f"{Colors.CYAN}📁 Database Path: {DB_FILENAME}{Colors.RESET}")
    conn = await get_async_db_connection()
    
    # Basic tables only - no ALTER TABLE (migrations handled by migrate.py)
    await conn.execute('''CREATE TABLE IF NOT EXISTS signals
    (id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER, type TEXT, market_q TEXT, outcome TEXT,
    trader_addr TEXT, usd_size REAL, dominance REAL,
    predictions INTEGER, price REAL,
    risk_score INTEGER, tx_hash TEXT, slug TEXT,
    meta_data TEXT, block_number INTEGER, token_id TEXT, nonce INTEGER,
    alpha_tag TEXT)''')

    # === Phase 1 forensic tables ===
    await conn.execute('''CREATE TABLE IF NOT EXISTS cluster_candidates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cluster_key TEXT UNIQUE,
        wallet_addresses TEXT,
        market_slug TEXT,
        detected_at INTEGER,
        enriched BOOLEAN DEFAULT 0
    )''')

    await conn.execute('''CREATE TABLE IF NOT EXISTS funding_sources (
        wallet_addr TEXT PRIMARY KEY,
        ultimate_source TEXT,
        source_label TEXT,
        first_tx_hash TEXT,
        confidence_score REAL,
        last_updated INTEGER,
        funder_address TEXT,
        funding_ts INTEGER
    )''')

    await conn.execute('''CREATE TABLE IF NOT EXISTS market_price_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        market_slug TEXT,
        outcome TEXT,
        price REAL,
        timestamp INTEGER
    )''')

    await conn.execute('''CREATE TABLE IF NOT EXISTS wallet_relationships (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cluster_key TEXT,
        source_wallet TEXT,
        target_wallet TEXT,
        relationship_type TEXT,
        tx_hash TEXT,
        amount_eth REAL,
        computed_at INTEGER
    )''')

    await conn.execute('''CREATE TABLE IF NOT EXISTS trader_performance (
        wallet_addr TEXT PRIMARY KEY,
        total_trades INTEGER,
        winning_trades INTEGER,
        losing_trades INTEGER,
        total_pnl_usd REAL,
        avg_precision_score REAL,
        last_updated INTEGER
    )''')

    await conn.commit()
    await conn.close()
    print(f"{Colors.GREEN}[+] Database tables initialized (migrations handled by migrate.py){Colors.RESET}")

async def db_save_signal_async(data):
    """
    Stage 4 Task 4: Queue-based signal saving.
    Instead of writing directly to DB, put data in queue for batch processing.
    
    Args:
        data: Tuple with 22 elements for signal INSERT
    """
    try:
        # Put signal data in queue for batch writer
        DB_WRITE_QUEUE.put_nowait(data)
    except asyncio.QueueFull:
        # Queue is full - this should rarely happen
        print(f"{Colors.RED}[{get_time_str()}] [⚠️ DB_QUEUE_FULL] Signal queue overflow!{Colors.RESET}")
        # Fallback: write directly to DB (should not happen under normal conditions)
        try:
            conn = await get_async_db_connection()
            await conn.execute('''INSERT INTO signals
            (timestamp, type, market_q, outcome, trader_addr, usd_size, dominance,
            predictions, price, risk_score, tx_hash, slug,
            meta_data, block_number, token_id, nonce, alpha_tag,
            market_title, current_price, total_bets, wallet_age_hours, is_analyzed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', data)
            await conn.commit()
            await conn.close()
        except Exception as db_err:
            await crash_log_async("db_save_signal_fallback", db_err, traceback.format_exc())

async def db_writer_loop():
    """
    Stage 4 Task 4: Batch DB writer.
    Aggregates signals from DB_WRITE_QUEUE and writes them in batches.
    
    Benefits:
    - Reduces disk I/O by batching multiple INSERTs
    - Minimizes WAL journal pressure
    - Prevents database locks during high-volume trading
    """
    conn = await get_async_db_connection()
    batch = []
    last_write_time = time.time()
    
    print(f"{Colors.GREEN}[{get_time_str()}] [📝 DB_WRITER] Batch writer started (batch_size={DB_BATCH_SIZE}, timeout={DB_BATCH_TIMEOUT}s){Colors.RESET}")
    
    while True:
        try:
            # Wait for new data or timeout
            try:
                data = await asyncio.wait_for(DB_WRITE_QUEUE.get(), timeout=DB_BATCH_TIMEOUT)
                batch.append(data)
            except asyncio.TimeoutError:
                # Timeout reached - write batch if we have data
                pass
            
            # Write batch if:
            # 1. Batch is full, OR
            # 2. Timeout reached AND we have data
            if len(batch) >= DB_BATCH_SIZE or (batch and (time.time() - last_write_time) >= DB_BATCH_TIMEOUT):
                # Execute batch INSERT
                if batch:
                    await conn.executemany('''INSERT INTO signals
                    (timestamp, type, market_q, outcome, trader_addr, usd_size, dominance,
                    predictions, price, risk_score, tx_hash, slug,
                    meta_data, block_number, token_id, nonce, alpha_tag,
                    market_title, current_price, total_bets, wallet_age_hours, is_analyzed)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', batch)
                    await conn.commit()
                    print(f"{Colors.CYAN}[{get_time_str()}] [📝 DB_WRITER] Wrote {len(batch)} signals to DB{Colors.RESET}")
                    batch = []
                    last_write_time = time.time()

            # Mark task as done (for queue management)
            if not DB_WRITE_QUEUE.empty():
                DB_WRITE_QUEUE.task_done()

        except Exception as e:
            await crash_log_async("db_writer_loop", e, traceback.format_exc())
            # Clear batch on error to prevent corruption
            batch = []
            await asyncio.sleep(1)

# ==========================================
# 🛠️ HELPERS
# ==========================================
def get_time_str():
    """
    TASK 1: Return integer Unix timestamp instead of formatted string.
    This eliminates timezone offset issues in SQLite queries.
    """
    return int(time.time())

# ==========================================
# 🏷️ PHASE 1: MARKET TITLE CACHING
# ==========================================
async def fetch_market_title_cached(session, slug):
    """
    Fetch market title with TTLCache to avoid API spam.
    TTLCache handles expiration automatically via ttl parameter.
    
    MARKET_CACHE now stores: slug -> title (simple string)
    """
    # Check cache first (TTLCache automatically expires old entries)
    if slug in MARKET_CACHE:
        return MARKET_CACHE[slug]

    # Cache miss - fetch from API
    try:
        async with session.get(POLYMARKET_API_URL, params={"slug": slug}, timeout=5) as r:
            if r.status == 200:
                data = await r.json()
                if isinstance(data, list) and len(data) > 0:
                    title = data[0].get('title', slug)
                    # TTLCache stores simple value - expiration handled automatically
                    MARKET_CACHE[slug] = title
                    return title
    except Exception as e:
        print(f"{Colors.RED}[!] Market Title Fetch Error: {e}{Colors.RESET}")

    # Fallback to slug
    return slug

async def load_insiders_async():
    """
    STAGE 2: Async version - Load proven insiders from SQLite table.
    Uses aiosqlite for non-blocking I/O.
    """
    global KNOWN_INSIDERS
    from config import load_proven_insiders_from_db_async
    KNOWN_INSIDERS.clear()

    # Load from DB (primary source after P2.3 migration)
    db_insiders = await load_proven_insiders_from_db_async()
    if db_insiders:
        KNOWN_INSIDERS.update(db_insiders)
        print(f"{Colors.CYAN}[{get_time_str()}] [INSIDERS] Loaded {len(KNOWN_INSIDERS)} from DB{Colors.RESET}")
        return

    # Fallback: file still exists (before migration completes)
    insiders_file = os.path.join(BASE_DIR, "support", "insiders.txt")
    if os.path.exists(insiders_file):
        try:
            with open(insiders_file, "r", encoding="utf-8") as f:
                for line in f:
                    val = line.strip().lower()
                    if val.startswith("0x"):
                        KNOWN_INSIDERS.add(val)
            print(f"{Colors.YELLOW}[{get_time_str()}] [INSIDERS] Loaded {len(KNOWN_INSIDERS)} from FILE (run migration!){Colors.RESET}")
        except Exception as e:
            await crash_log_async("load_insiders_file_fallback", e, traceback.format_exc())

def load_insiders():
    """
    Synchronous wrapper for backward compatibility.
    NEW CODE SHOULD USE load_insiders_async() instead.
    """
    global KNOWN_INSIDERS
    from config import load_proven_insiders_from_db
    KNOWN_INSIDERS.clear()

    # Load from DB (primary source after P2.3 migration)
    db_insiders = load_proven_insiders_from_db()
    if db_insiders:
        KNOWN_INSIDERS.update(db_insiders)
        print(f"{Colors.CYAN}[{get_time_str()}] [INSIDERS] Loaded {len(KNOWN_INSIDERS)} from DB{Colors.RESET}")
        return

    # Fallback: file still exists (before migration completes)
    insiders_file = os.path.join(BASE_DIR, "support", "insiders.txt")
    if os.path.exists(insiders_file):
        try:
            with open(insiders_file, "r", encoding="utf-8") as f:
                for line in f:
                    val = line.strip().lower()
                    if val.startswith("0x"):
                        KNOWN_INSIDERS.add(val)
            print(f"{Colors.YELLOW}[{get_time_str()}] [INSIDERS] Loaded {len(KNOWN_INSIDERS)} from FILE (run migration!){Colors.RESET}")
        except Exception as e:
            crash_log("load_insiders_file_fallback", e, traceback.format_exc())

def calculate_risk_score(usd, dominance, predictions, is_proven):
    """
    Расчет "Risk Score" (он же "Interest Score").
    100 = Proven Insider.
    99 = Максимально подозрительная активность (много денег + новый акк).
    """
    if is_proven: return 100
    
    score_dom = dominance * 1.5 
    score_new = 40 / (predictions + 1)
    score_usd = min(usd / 100, 50)
    
    total_score = score_dom + score_new + score_usd
    return int(min(total_score, 99))

# ==========================================
# 📡 TELEGRAM — use alert_manager.send_telegram() everywhere
# Local send_telegram() removed: it had a different TG_CHAT_ID fallback than
# alert_manager, causing alerts to go to the wrong chat.
# ==========================================

async def get_polymarket_predictions(wallet_address):
    """
    Получает точное число 'Predictions' (traded markets) из API.
    Task 2: API Shield - Rate limited with semaphore and 1.2s delay.
    Task 6: In-Memory Cache - Prevent API bans during batch processing.
    """
    # ==========================================
    # 🚀 TASK 6: CHECK CACHE FIRST
    # TTLCache automatically expires entries after 5 minutes (300 seconds)
    # ==========================================
    if wallet_address in PREDICTIONS_CACHE:
        return PREDICTIONS_CACHE[wallet_address]

    params = {"user": wallet_address}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Connection": "keep-alive"
    }

    # Если сессия еще не создана или закрыта - возвращаем 0, чтобы не крашнуть бота
    if API_SESSION is None or API_SESSION.closed:
        return 0

    # Task 2: API Shield - Rate limiting
    async with DATA_API_SEMAPHORE:
        try:
            # Используем глобальную API_SESSION
            async with API_SESSION.get(DATA_API_URL, params=params, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    count = data.get("traded", 0)
                    # ==========================================
                    # 🚀 TASK 6: SAVE TO CACHE (TTLCache handles TTL automatically)
                    # ==========================================
                    PREDICTIONS_CACHE[wallet_address] = count
                    await asyncio.sleep(0.6)  # Task 2: Rate limit - stable threshold (0.2s triggers 408 timeouts)
                    return count
                else:
                    print(f"{Colors.RED}[!] Data API Error {resp.status} for {wallet_address}{Colors.RESET}")
                    await asyncio.sleep(0.6)
                    return 0
        except Exception as e:
            print(f"{Colors.RED}[{get_time_str()}] [!] Data API Connection Fail: {e}{Colors.RESET}")
            await asyncio.sleep(0.6)
            return 0

async def fast_rpc_request(session, payload):
    """
    🔗 SMART RPC BALANCER (Primary-Fallback / Stateful)
    STAGE 3 TASK 2: Stick to Primary node until it fails.
    
    - NO round-robin increment at the start
    - Switch to next RPC ONLY on errors (401, 429, 500-504, Timeout, Exception)
    - Permanently disable RPCs that return 401 for the session
    """
    global current_rpc_idx, DISABLED_RPC_SET

    # Start from current RPC index (stick to primary until it fails)
    local_idx = current_rpc_idx

    max_retries = len(RPC_LIST) * 2
    timeout_seconds = 12

    for retry in range(max_retries):
        # Task 2: Skip disabled RPCs (401 auth failures)
        while True:
            rpc_url = RPC_LIST[local_idx % len(RPC_LIST)]
            if rpc_url not in DISABLED_RPC_SET:
                break
            local_idx += 1
            # If all RPCs are disabled, reset and try anyway
            if local_idx - current_rpc_idx >= len(RPC_LIST):
                local_idx = current_rpc_idx
                rpc_url = RPC_LIST[local_idx % len(RPC_LIST)]
                break

        for attempt in range(2):
            try:
                async with session.post(rpc_url, json=payload, timeout=timeout_seconds, ssl=False) as r:
                    if r.status == 200:
                        # SUCCESS: Stay on this RPC, update global index for next call
                        current_rpc_idx = local_idx % len(RPC_LIST)
                        return await r.json()
                    elif r.status == 401:
                        # Task 2: Auth failure - permanently disable this RPC for the session
                        print(f"{Colors.RED}[{get_time_str()}] [❌ AUTH ERROR] RPC Key is invalid or expired! Check your config. Disabling: {rpc_url[:50]}...{Colors.RESET}")
                        DISABLED_RPC_SET.add(rpc_url)
                        local_idx += 1
                        current_rpc_idx = local_idx % len(RPC_LIST)  # Update global index
                        break  # Break attempt loop to switch RPC
                    elif r.status in (429, 500, 502, 503, 504):
                        print(f"{Colors.YELLOW}[{get_time_str()}] [BALANCER] RPC {r.status}. Switching engine...{Colors.RESET}")
                        local_idx += 1
                        current_rpc_idx = local_idx % len(RPC_LIST)  # Update global index
                        break  # Break attempt loop to switch RPC
                    else:
                        await asyncio.sleep(0.5)
            except asyncio.TimeoutError:
                print(f"{Colors.YELLOW}[{get_time_str()}][BALANCER] Timeout >{timeout_seconds}s. Switching engine...{Colors.RESET}")
                local_idx += 1
                current_rpc_idx = local_idx % len(RPC_LIST)  # Update global index
                break  # Break attempt loop
            except Exception as e:
                print(f"{Colors.YELLOW}[{get_time_str()}] [BALANCER] Error. Switching engine...{Colors.RESET}")
                local_idx += 1
                current_rpc_idx = local_idx % len(RPC_LIST)  # Update global index
                break  # Break attempt loop

        # Continue retry loop with new local_idx
        continue

    return None


async def safe_fetch(session, start_block, end_block):
    """
    🔒 SAFE MULTI-FETCHER: Fetch logs with semaphore-controlled concurrency.

    Task 1: Wraps RPC call with semaphore and handles 429 errors with back-off.
    Task 3: Aggressive 20s timeout with immediate retry.

    Args:
        session: aiohttp session
        start_block: Start block number (int)
        end_block: End block number (int)

    Returns:
        dict: RPC response with 'result' containing logs, or None on failure
    """
    max_retries = 3

    async with RPC_SEMAPHORE:
        for attempt in range(max_retries):
            try:
                # Task 3: Aggressive timeout - 20 seconds max per chunk
                r_logs = await asyncio.wait_for(
                    fast_rpc_request(session, {
                        "jsonrpc": "2.0",
                        "method": "eth_getLogs",
                        "params": [{
                            "address": TOKEN_CONTRACT,
                            "fromBlock": hex(start_block),
                            "toBlock": hex(end_block),
                            "topics": [[TOPIC_TRANSFER_SINGLE, TOPIC_TRANSFER_BATCH]]
                        }],
                        "id": 1
                    }),
                    timeout=20
                )

                if r_logs and 'result' in r_logs:
                    return {'block_start': start_block, 'block_end': end_block, 'logs': r_logs['result']}
                return None

            except asyncio.TimeoutError:
                # Task 3: Timeout - retry immediately
                if attempt < max_retries - 1:
                    continue
                print(f"{Colors.YELLOW}[{get_time_str()}] [⚠️ safe_fetch] Timeout after 20s for blocks {start_block}-{end_block}{Colors.RESET}")
                return None
            except Exception as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)
                else:
                    print(f"{Colors.YELLOW}[{get_time_str()}] [⚠️ safe_fetch] Failed blocks {start_block}-{end_block}: {e}{Colors.RESET}")
                    return None

    return None


async def fetch_tx_usdc_spent(session, tx_hash, trader_addr=None):
    """
    🔗 ON-CHAIN PRICE CALCULATION: Fetch exact USDC spent from transaction receipt.

    STAGE 3 TASK 3: Precise USDC Parsing for Relay/Batch Transactions.
    
    - Iterate through ALL USDC transfer logs (NO break on first match)
    - Sum direct_usdc_spent: USDC sent FROM trader_addr
    - Sum ctf_usdc_spent: USDC sent TO Polymarket CTF contract (0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e)
    - Return priority: direct_usdc_spent > ctf_usdc_spent > None

    Args:
        session: aiohttp session
        tx_hash: Transaction hash (0x prefixed)
        trader_addr: The trader's wallet/proxy address that SPENT USDC (buyer)

    Returns:
        float: Exact USDC amount spent by trader_addr, or None if parsing fails
    """
    # RETRY LOGIC: Some nodes return null if receipt requested too quickly
    receipt = None
    max_retries = 3

    for attempt in range(max_retries):
        try:
            # Add small delay before first fetch to let node sync
            if attempt > 0:
                await asyncio.sleep(1.0)

            # Fetch transaction receipt
            receipt_payload = {
                "jsonrpc": "2.0",
                "method": "eth_getTransactionReceipt",
                "params": [tx_hash],
                "id": 1
            }

            receipt = await fast_rpc_request(session, receipt_payload)

            # CRITICAL FIX: Check for None result (node hasn't indexed tx yet)
            if not receipt or receipt.get('result') is None:
                if attempt < max_retries - 1:
                    continue  # Retry
                crash_log("fetch_tx_usdc_spent", Exception(f"Receipt not found after {max_retries} retries"), f"tx_hash={tx_hash}")
                return None  # All retries exhausted

            # Successfully got receipt
            break

        except Exception as fetch_err:
            if attempt < max_retries - 1:
                continue  # Retry on error
            crash_log("fetch_tx_usdc_spent", fetch_err, traceback.format_exc())
            return None  # All retries exhausted

    # Parse the receipt logs
    try:
        logs = receipt['result'].get('logs', [])

        # STRICT RPC VALIDATION: Check for missing or empty logs
        if not logs:
            crash_log("fetch_tx_usdc_spent", Exception("Empty logs array"), f"tx_hash={tx_hash}")
            return None

        # STAGE 3 TASK 3: Two-counter logic for Relay/Batch transactions
        trader_lower = trader_addr.lower() if trader_addr else None
        direct_usdc_spent = 0.0  # USDC sent FROM trader_addr
        ctf_usdc_spent = 0.0     # USDC sent TO Polymarket CTF contract

        for log in logs:
            # STRICT RPC VALIDATION: Validate log structure
            if log is None:
                continue
            if not isinstance(log, dict):
                crash_log("fetch_tx_usdc_spent", Exception(f"Invalid log format: {type(log)}"), f"tx_hash={tx_hash}")
                continue

            # Check if this is a USDC transfer (check BOTH Native and Bridged contracts)
            log_addr = log.get('address', '').lower()
            is_usdc = any(log_addr == usdc.lower() for usdc in USDC_CONTRACTS)

            if not is_usdc:
                continue

            topics = log.get('topics', [])
            # STRICT RPC VALIDATION: Check for missing or insufficient topics
            if not topics or len(topics) < 3:
                crash_log("fetch_tx_usdc_spent", Exception(f"Insufficient topics: {len(topics)}"), f"tx_hash={tx_hash}")
                continue
            if topics[0].lower() != TOPIC_TRANSFER:
                continue

            # Parse transfer log
            # topics[1] = from (sender) - who sent USDC
            # topics[2] = to (recipient) - who received USDC
            # data = amount
            try:
                from_addr = "0x" + topics[1][26:].lower() if len(topics) > 1 and len(topics[1]) >= 66 else None
                to_addr = "0x" + topics[2][26:].lower() if len(topics) > 2 and len(topics[2]) >= 66 else None
                amount_hex = log.get('data', '0x0')
                # STRICT RPC VALIDATION: Check for missing data field
                if amount_hex is None:
                    crash_log("fetch_tx_usdc_spent", Exception("Missing data field in log"), f"tx_hash={tx_hash}")
                    continue
                amount = int(amount_hex, 16) if amount_hex and amount_hex != '0x' else 0

                # USDC has 6 decimals
                amount_usdc = amount / 1_000_000

                # STAGE 3 TASK 3: Sum all USDC transfers (NO break!)
                if from_addr and to_addr:
                    # Priority 1: Direct USDC spend from trader
                    if trader_lower and from_addr.lower() == trader_lower:
                        direct_usdc_spent += amount_usdc
                    
                    # Priority 2: USDC flowing into Polymarket CTF (relay/proxy scenario)
                    if to_addr.lower() == POLYMARKET_CTF:
                        ctf_usdc_spent += amount_usdc

            except Exception as parse_err:
                continue

        # ==========================================
        # STAGE 3 TASK 3: RETURN LOGIC WITH PRIORITIES
        # ==========================================
        if direct_usdc_spent > 0:
            print(f"{Colors.CYAN}[{get_time_str()}] [💎 ON-CHAIN] {tx_hash[:10]}... | Trader: {trader_addr[:10]}... | Direct USDC: ${direct_usdc_spent:,.0f}{Colors.RESET}")
            return direct_usdc_spent
        
        if ctf_usdc_spent > 0:
            print(f"{Colors.CYAN}[{get_time_str()}] [💎 ON-CHAIN] {tx_hash[:10]}... | CTF Inflow: ${ctf_usdc_spent:,.0f} (relay/proxy){Colors.RESET}")
            return ctf_usdc_spent

        # No USDC transfers found - return None for API fallback
        if trader_addr:
            print(f"{Colors.YELLOW}[{get_time_str()}] [BATCH TX] {tx_hash[:10]}... | No USDC transfers found - using API fallback{Colors.RESET}")

        return None

    except Exception as e:
        # Debug log only - don't spam console for expected errors
        print(f"{Colors.MAGENTA}[{get_time_str()}] [USDC Parse] {tx_hash[:10]}...: {e}{Colors.RESET}")
        return None

async def batch_check_wallets(session, addresses):
    """
    Stage 4: Updated for TTLCache.
    WALLET_CACHE stores: addr -> {'nonce': int, 'is_contract': bool}
    TTLCache handles expiration automatically via ttl=3600 (1 hour)
    """
    # Filter addresses that need checking (TTLCache auto-expires old entries)
    to_check = [addr for addr in set(addresses) if addr not in WALLET_CACHE]
    if not to_check:
        return

    batch_payload = []
    addr_map = {}
    for i, addr in enumerate(to_check):
        n_id, c_id = i*2, i*2+1
        batch_payload.append({"jsonrpc":"2.0","method":"eth_getTransactionCount","params":[addr,"latest"],"id": n_id})
        batch_payload.append({"jsonrpc":"2.0","method":"eth_getCode","params":[addr,"latest"],"id": c_id})
        addr_map[n_id] = (addr, 'nonce'); addr_map[c_id] = (addr, 'code')

    try:
        results = await fast_rpc_request(session, batch_payload)
        if results:
            if not isinstance(results, list): results = [results]
            # Initialize cache entries for all addresses being checked
            for addr in to_check:
                if addr not in WALLET_CACHE:
                    WALLET_CACHE[addr] = {'nonce': 0, 'is_contract': False}
            # Update cache with RPC results
            for res in results:
                rid = res.get('id')
                addr, r_type = addr_map.get(rid, (None, None))
                if not addr or 'error' in res:
                    continue
                if 'result' in res:
                    if r_type == 'nonce':
                        WALLET_CACHE[addr]['nonce'] = int(res['result'], 16)
                    elif r_type == 'code' and len(res['result']) > 2:
                        WALLET_CACHE[addr]['is_contract'] = True
    except Exception as e:
        # RPC batch call failed — wallets won't be in cache, scanner will skip them
        print(f"{Colors.YELLOW}[{get_time_str()}] [batch_check_wallets] RPC error: {type(e).__name__}: {e}{Colors.RESET}")

async def analyzer_worker():
    while True:
        try:
            # PRIORITY QUEUE: Unpack priority, timestamp, and task data
            priority, queue_ts, task_data = await ANALYSIS_QUEUE.get()
            (tx, trader, tid, usd, m, label, nonce, block_num,
             counterparty, raw_shares, operator, tx_idx) = task_data

            now_ts = time.time()

            # ==========================================
            # 1️⃣ UNPACK & PRICE FILTER
            # ==========================================
            if m['p'] > MAX_PRICE_FILTER:
                ANALYSIS_QUEUE.task_done()
                continue

            # ==========================================
            # 2️⃣ ОПРЕДЕЛЯЕМ БАЗОВЫЙ СТАТУС И VIP
            # ==========================================
            is_insider = trader in KNOWN_INSIDERS or "PROVEN" in label
            acc_key = f"{trader}_{tid}"

            if acc_key not in ACCUMULATORS:
                ACCUMULATORS[acc_key] = {"total": 0, "start_ts": now_ts, "last_level": 0}
            elif (now_ts - ACCUMULATORS[acc_key]["start_ts"]) > 86400:
                ACCUMULATORS[acc_key] = {"total": 0, "start_ts": now_ts, "last_level": 0}

            ACCUMULATORS[acc_key]["total"] += usd
            total_accumulated = ACCUMULATORS[acc_key]["total"]

            # ==========================================
            # 3️⃣ УМНЫЙ ГЕЙТКИПЕР: СТРОГАЯ ПРОВЕРКА
            # ==========================================
            final_status = 0  # 0 = Pending, 1 = Approved, 2 = Rejected
            q_upper = m['q'].upper()
            is_casino = any(kw in q_upper for kw in CASINO_KEYWORDS)

            # FIX: Async проверка whitelist с использованием persistent connection
            # Проверяем есть ли рынок уже в базе
            _wl_status = None  # None = not found, 1 = approved, 0 = rejected
            try:
                # Используем persistent connection для скорости (кэшируется)
                _conn = await get_persistent_async_db_connection("maintest_whitelist")
                _cursor = await _conn.execute(
                    "SELECT is_approved FROM markets_whitelist WHERE slug = ?", 
                    (m['s'],)
                )
                _row = await _cursor.fetchone()
                if _row:
                    _wl_status = _row['is_approved']
                # Не закрываем — persistent connection переиспользуется
            except Exception as _wl_err:
                # Ошибка проверки whitelist не должна блокировать сигналы
                # Логим только если это не блокировка БД (оно норм при высокой нагрузке)
                if "locked" not in str(_wl_err).lower():
                    await crash_log_async("maintest_whitelist_check", _wl_err, "")

            # Применяем статус из whitelist если рынок найден
            if _wl_status is not None:
                # Рынок уже есть в базе - берем его статус
                if _wl_status == 1:
                    final_status = 2 if is_casino else 1  # Approved, но casino = rejected
                else:
                    final_status = 2  # Rejected
            else:
                # РЫНКА НЕТ В ВАЙТЛИСТЕ — новая заявка
                if is_casino:
                    final_status = 2  # Отсекаем по ключевым словам сразу
                elif is_insider:
                    final_status = 1  # Только проверенные кошельки проходят без ИИ
                else:
                    # ВСЕ ОСТАЛЬНЫЕ ждут market_discovery
                    final_status = 0

            # ==========================================
            # 4️⃣ FAST-FAIL (Пропускаем тяжелую on-chain логику для мелочи)
            # ==========================================
            if not is_insider and total_accumulated < USD_LIMIT_NEW:
                fast_meta = {
                    "vol": m['vol'], "ts": now_ts, "cp": counterparty, "shares": raw_shares,
                    "op": operator, "tx_pos": tx_idx, "is_bot": False, "age_h": -1,
                    "accumulated": int(total_accumulated),
                    "on_chain": {"usdc_exact": False, "usdc_diff_pct": None, "true_slippage_pct": None},
                    "price_api": float(m['p']), "price_onchain": float(m['p'])
                }

                await db_save_signal_async((
                    get_time_str(), label, m['q'], m['outcome'],
                    trader, usd, 0.0, 0, float(m['p']), 0, tx, m['s'],
                    json.dumps(fast_meta), block_num, str(tid), 0, None,
                    m['q'], float(m['p']), 0, 0.0, final_status
                ))
                ANALYSIS_QUEUE.task_done()
                continue

            # ==========================================
            # 5️⃣ DEEP ANALYSIS (Для крупных сделок и инсайдеров)
            # ==========================================
            real_usd = None
            real_price = None

            current_lag = CURRENT_BLOCK - LAST_PROCESSED_BLOCK if LAST_PROCESSED_BLOCK > 0 else 0
            if current_lag > 100: skip_threshold = 1000
            elif current_lag > 20: skip_threshold = 300
            else: skip_threshold = 100

            if usd >= skip_threshold or is_insider:
                ONCHAIN_STATS["total_trades"] += 1
                try:
                    usdc_amount = await asyncio.wait_for(
                        fetch_tx_usdc_spent(API_SESSION, tx, trader),
                        timeout=5.0
                    )
                    if usdc_amount is not None and usdc_amount > 0:
                        real_usd = usdc_amount
                        shares_in_tokens = raw_shares / 1_000_000
                        if shares_in_tokens > 0:
                            real_price = usdc_amount / shares_in_tokens
                            if real_price > 1.0 or real_price < 0.0001:
                                real_usd, real_price = None, None
                            else:
                                ONCHAIN_STATS["onchain_success"] += 1
                except Exception:
                    pass

            final_usd = real_usd if real_usd is not None else usd
            final_price = real_price if real_price is not None else float(m['p'])
            
            if final_price > MAX_PRICE_FILTER:
                ANALYSIS_QUEUE.task_done()
                continue

            m_vol = m['vol']
            dom = (final_usd / (m_vol + final_usd)) * 100 if (m_vol + final_usd) > 0 else 100

            # --- Slippage Calculation ---
            slippage_pct = None
            market_price = float(m['p']) if m.get('p') else 0.0
            if market_price > 0 and raw_shares > 0:
                execution_price = final_usd / (raw_shares / 1_000_000)
                price_diff = abs(execution_price - market_price)
                if price_diff > 0.02:
                    slippage_pct = ((price_diff - 0.02) / market_price) * 100
                else:
                    slippage_pct = 0.0

            if slippage_pct is not None and market_price < 0.10 and price_diff < 0.05:
                slippage_pct = None
            elif slippage_pct is not None and slippage_pct > 15.0 and dom < 1.0:
                slippage_pct = None

            # --- API Calls ---
            real_predictions = await get_polymarket_predictions(trader)
            m_age_hours = -1
            if m.get('created_at'):
                try:
                    c_dt = datetime.fromisoformat(m['created_at'].replace("Z", "+00:00")).replace(tzinfo=timezone.utc)
                    m_age_hours = (datetime.now(timezone.utc) - c_dt).total_seconds() / 3600
                except: pass

            on_chain_data = {
                "usdc_exact": real_usd is not None,
                "usdc_diff_pct": round(((real_usd - usd) / usd) * 100, 2) if real_usd and usd > 0 else None,
                "true_slippage_pct": round(slippage_pct, 2) if slippage_pct is not None else None
            }

            meta_dict = {
                "vol": m_vol, "ts": now_ts, "cp": counterparty, "shares": raw_shares,
                "op": operator, "tx_pos": tx_idx, "is_bot": (operator != trader),
                "age_h": round(m_age_hours, 2), "accumulated": int(total_accumulated),
                "on_chain": on_chain_data, "price_api": float(m['p']), "price_onchain": real_price
            }

            # ==========================================
            # 6️⃣ АЛЕРТЫ И СОХРАНЕНИЕ
            # ==========================================
            current_threshold = USD_LIMIT_NEW if real_predictions < PRED_LIMIT_NEW else USD_LIMIT_OLD
            current_level = int(total_accumulated // current_threshold)
            
            l_text = label
            alpha_tag = None

            if is_insider:
                l_text = "👑 PROVEN INSIDER"
                alpha_tag = "👑 PROVEN"
            elif ACCUMULATORS[acc_key].get("last_level", 0) == 0 and current_level >= 1:
                ACCUMULATORS[acc_key]["last_level"] = 1 
                l_text = f"🆕 FRESH ENTRY (> {USD_LIMIT_NEW}$)" if real_predictions < PRED_LIMIT_NEW else f"🐋 WHALE BET (> {USD_LIMIT_OLD}$)"
                alpha_tag = "🆕 FRESH ENTRY" if real_predictions < PRED_LIMIT_NEW else "🐋 WHALE"
            elif current_level > ACCUMULATORS[acc_key].get("last_level", 0):
                ACCUMULATORS[acc_key]["last_level"] = current_level
                l_text = f"⚡ ACCUMULATION (Lvl {current_level})"
                alpha_tag = "⚡ ACCUMULATION"

            risk_score = calculate_risk_score(total_accumulated, dom, real_predictions, is_insider)

            # Сохраняем сигнал. Используем async версию для неблокирующего I/O.
            await db_save_signal_async((
                get_time_str(), l_text, m['q'], m['outcome'],
                trader, final_usd, dom, real_predictions,
                final_price, risk_score, tx, m['s'],
                json.dumps(meta_dict), block_num, str(tid), 0, alpha_tag,
                m['q'], final_price, real_predictions, m_age_hours, final_status
            ))

            # Отправить алерт если сигнал одобрен и достиг порога
            # ==========================================
            # 📡 АЛЕРТЫ — отправка через alert_manager
            # Только для одобренных сигналов с alpha_tag
            # ==========================================
            if final_status == 1 and alpha_tag is not None:
                # ==========================================
                # 🚀 STAGE 4 TASK 1: REDIS PRODUCER
                # Push signal to Redis queue for instant worker processing
                # ==========================================
                signal_dict = {
                    "wallet": trader,
                    "market_slug": m['s'],
                    "market_title": m['q'],
                    "outcome": m['outcome'],
                    "usd_size": final_usd,
                    "price": final_price,
                    "dominance": dom,
                    "predictions": real_predictions,
                    "alpha_tag": alpha_tag,
                    "is_insider": is_insider,
                    "meta_dict": meta_dict,
                    "timestamp": get_time_str()
                }
                try:
                    # FIX-1: Use Pub/Sub instead of LPUSH/BRPOP to avoid race condition
                    # Both workers receive ALL signals (no 50% loss)
                    await redis_client.publish("signals_broadcast", json.dumps(signal_dict))
                    print(f"{Colors.GREEN}[{get_time_str()}] [📮 REDIS] Signal published to broadcast: {trader[:10]}... | ${final_usd:,.0f}{Colors.RESET}")
                except Exception as redis_err:
                    print(f"{Colors.RED}[{get_time_str()}] [❌ REDIS ERROR] Failed to publish signal: {redis_err}{Colors.RESET}")
                    await crash_log_async("redis_push_signal", redis_err, traceback.format_exc())

                # ==========================================
                # 🚀 PHASE 2 TASK 2.3: HYBRID DETECTOR INTEGRATION
                # Publish insider_signal to Redis for mm_worker.py Micro-Shield
                # ==========================================
                if is_insider or alpha_tag is not None:
                    # Confidence levels per strategy Section 3.2:
                    # HIGH: proven insider (is_insider=True)
                    # MEDIUM: on-chain confirmed, win_rate quality (ACCUMULATION signals)
                    # LOW: everything else (WHALE, FRESH ENTRY) — not insider quality
                    if is_insider:
                        confidence_level = "HIGH"
                    elif alpha_tag and "ACCUMULATION" in alpha_tag:
                        # Accumulation = repeated buys by same wallet = MEDIUM suspicion
                        confidence_level = "MEDIUM"
                    else:
                        # WHALE, FRESH ENTRY, MIXER without proven history = LOW
                        confidence_level = "LOW"
                    try:
                        await redis_client.publish('insider_signal', json.dumps({
                            'wallet': trader,
                            'slug': m['s'],
                            'confidence': confidence_level,
                            'ts': int(time.time())
                        }))
                    except Exception as _redis_err:
                        pass  # Redis errors must never crash maintest.py

            status_str = "✅ APPROVED" if final_status == 1 else "⏳ PENDING" if final_status == 0 else "❌ REJECTED"

            # Show market title for both approved and rejected signals
            market_short = m['q'][:60] + "..." if len(m['q']) > 60 else m['q']
            
            if final_status == 2:
                # Rejected
                print(f"{Colors.MAGENTA}[{get_time_str()}][📝 SAVED] {trader[:10]}... | ${final_usd:,.0f} | {status_str} | {market_short}{Colors.RESET}")
            elif final_status == 1:
                # Approved
                print(f"{Colors.GREEN}[{get_time_str()}][📝 SAVED] {trader[:10]}... | ${final_usd:,.0f} | {status_str} | {market_short}{Colors.RESET}")
            else:
                # Pending
                print(f"{Colors.GREEN}[{get_time_str()}][📝 SAVED] {trader[:10]}... | ${final_usd:,.0f} | {status_str}{Colors.RESET}")

            ANALYSIS_QUEUE.task_done()
        except Exception as e:
            crash_log("analyzer_worker", e, traceback.format_exc())
            ANALYSIS_QUEUE.task_done()
async def update_markets_loop():
    while True:
        try:
            connector = aiohttp.TCPConnector()
            async with aiohttp.ClientSession(connector=connector) as session:
                offset = 0; limit = 1000
                while True:
                    async with session.get(f"{POLYMARKET_API_URL}?active=true&closed=false&limit={limit}&offset={offset}", timeout=45) as resp:
                        if resp.status!=200: break
                        markets = await resp.json()
                        if not markets: break
                        for m in markets:
                            try:
                                q = m.get("question", "")
                                if not q:
                                    continue
                                if any(kw in (q + " " + m.get("slug", "")).upper() for kw in CASINO_KEYWORDS): continue
                                
                                t_raw = m.get("clobTokenIds", [])
                                tokens = json.loads(t_raw) if isinstance(t_raw, str) else t_raw

                                o_raw = m.get("outcomes", [])
                                outcomes = json.loads(o_raw) if isinstance(o_raw, str) else o_raw

                                p_raw = m.get("outcomePrices", [])
                                prices = json.loads(p_raw) if isinstance(p_raw, str) else p_raw
                                
                                c_at = m.get("createdAt", "") 
                                
                                for i, t in enumerate(tokens):
                                    try:
                                        # P1.6: validate price index exists before access
                                        if i >= len(prices) or i >= len(outcomes):
                                            continue
                                        price = float(prices[i])
                                        if price <= 0 or price > 1:
                                            continue
                                        oname = outcomes[i] if i < len(outcomes) else "Unk"
                                        label = "🟢 YES" if oname == "Yes" else "🔴 NO" if oname == "No" else f"🔵 {oname}"
                                        
                                        WATCHLIST[int(t)] = {
                                            "q": q, 
                                            "s": m.get("slug", ""), 
                                            "p": price, 
                                            "vol": float(m.get("volume", 0)), 
                                            "outcome": label,
                                            "created_at": c_at
                                        }
                                    except Exception as token_err:
                                        continue
                            except Exception as market_err:
                                continue
                        offset += limit
        except Exception as e:
            print(f"{Colors.RED}[{get_time_str()}] [update_markets_loop] Error: {type(e).__name__}: {e}{Colors.RESET}")
            await asyncio.sleep(10)
        await asyncio.sleep(120)  # Task 3: Market data doesn't change fast - save Compute Units

async def dashboard_loop():
    while True:
        try:
            if CURRENT_BLOCK > 0:
                now = time.time()
                elapsed = int(now - SEARCH_START_TIME)

                # On-chain stats
                onchain_coverage = (ONCHAIN_STATS["onchain_success"] / max(1, ONCHAIN_STATS["total_trades"])) * 100
                slippage_alerts = ONCHAIN_STATS["slippage_detected"]
                
                # Task 4: Latency Dashboard V2
                lag = CURRENT_BLOCK - LAST_PROCESSED_BLOCK
                sync_status = "Catching Up" if lag > 5 else "Synced"
                sync_color = Colors.YELLOW if lag > 5 else Colors.GREEN

                # Calculate active workers (total RPC capacity - available slots = active)
                total_rpc_capacity = 10
                active_workers = total_rpc_capacity - RPC_SEMAPHORE._value
                workers_display = f"{active_workers}/{total_rpc_capacity}"

                status = (f"\r{Colors.CYAN}●{Colors.RESET} Time: {elapsed//3600:02d}:{(elapsed%3600)//60:02d}:{elapsed%60:02d} | "
                          f"Blk: {Colors.GREEN}{CURRENT_BLOCK}{Colors.RESET} | "
                          f"Lag: {Colors.YELLOW if lag > 10 else Colors.GREEN}{lag}{Colors.RESET} blks | "
                          f"Workers: {Colors.CYAN}{workers_display}{Colors.RESET} | "
                          f"Status: {sync_color}{sync_status}{Colors.RESET} | "
                          f"Mkts: {Colors.CYAN}{len(WATCHLIST)}{Colors.RESET} | "
                          f"On-Chain: {Colors.GREEN}{ONCHAIN_STATS['onchain_success']}{Colors.RESET}/{ONCHAIN_STATS['total_trades']} "
                          f"({onchain_coverage:.0f}%) | "
                          f"Slippage: {Colors.YELLOW if slippage_alerts > 0 else Colors.GREEN}{slippage_alerts}{Colors.RESET} | "
                          f"Scan: {Colors.YELLOW}{LAST_SCAN}{Colors.RESET}")
                sys.stdout.write(status + " " * 10 + Colors.CLEAR); sys.stdout.flush()
            await asyncio.sleep(5.0)  # Updated to 5s for more responsive dashboard
        except Exception as e:
            # Dashboard errors are non-critical — log but keep running
            print(f"\n{Colors.YELLOW}[{get_time_str()}] [dashboard_loop] {type(e).__name__}: {e}{Colors.RESET}")
            await asyncio.sleep(5.0)

def decode_transfer_batch(data_hex):
    try:
        if not data_hex or len(data_hex) < 4:
            return [], []
        data = bytes.fromhex(data_hex[2:])
        ids_offset = int.from_bytes(data[0:32], 'big')
        values_offset = int.from_bytes(data[32:64], 'big')
        
        ids_len = int.from_bytes(data[ids_offset:ids_offset+32], 'big')
        ids = []
        for i in range(ids_len):
            start = ids_offset + 32 + (i * 32)
            ids.append(int.from_bytes(data[start:start+32], 'big'))
            
        values_len = int.from_bytes(data[values_offset:values_offset+32], 'big')
        values = []
        for i in range(values_len):
            start = values_offset + 32 + (i * 32)
            values.append(int.from_bytes(data[start:start+32], 'big'))
        return ids, values
    except Exception as e:
        # Malformed batch data from RPC — not critical, skip silently
        return [], []

async def scanner_loop():
    global CURRENT_BLOCK, LAST_PROCESSED_BLOCK
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)

    BLACKLIST = {
        "0x2b8102a0a382e753444265443213039d677d612e", 
        "0x8894e0a0c962cb723c1976a4421c95949be2d4e3",
        "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e",
        "0xc5d563a36ae78145c45a50134d48a1215220f80a",
        "0x4d97dcd97ec945f40cf65f87097ace5ea0476045",
        "0xd90e2f925da726b50c4ed8d0fb90ad053324f31b",
        "0xa6b71e26c5e0845f74c812102ca7114b6a896ab2",
        "0x0000000000000000000000000000000000000000"
    }

    print(f"{Colors.GREEN}📡 Connecting to Blockchain...{Colors.RESET}")

    async with aiohttp.ClientSession(connector=connector) as session:
        while CURRENT_BLOCK == 0:
            r = await fast_rpc_request(session, {"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1})

            if r and 'result' in r:
                CURRENT_BLOCK = int(r['result'], 16)
                print(f"{Colors.CYAN}📡 Start Block: {CURRENT_BLOCK}{Colors.RESET}")
                # Task 1: Fix Lag Initialization - set LAST_PROCESSED_BLOCK on first fetch
                LAST_PROCESSED_BLOCK = CURRENT_BLOCK
            else:
                await asyncio.sleep(2)

        last = CURRENT_BLOCK - 1

        while True:
            try:
                r = await fast_rpc_request(session, {"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1})
                if not r: await asyncio.sleep(2); continue
                new_head = int(r['result'], 16); CURRENT_BLOCK = new_head

                # Task 3: Sanity check for impossibly large lag
                if new_head - last > 50000:
                    print(f"{Colors.YELLOW}[{get_time_str()}] [⚠️ SANITY] Huge lag detected on startup, resetting to head of chain.{Colors.RESET}")
                    last = new_head - 1
                    LAST_PROCESSED_BLOCK = last
                    continue

                # Calculate lag for dashboard
                lag = new_head - last

                if lag <= 0: await asyncio.sleep(1.5); continue

                # ==========================================
                # ⛓️ STAGE 3: HISTORICAL CATCH-UP
                # Never drop blocks - process in MAX_CATCHUP_BLOCKS windows
                # ==========================================
                target_head = min(last + MAX_CATCHUP_BLOCKS, new_head)
                display_lag = new_head - last  # Show real lag for dashboard

                # ==========================================
                # 🔒 SAFE MULTI-FETCHER: Intelligent Back-off
                # ==========================================
                # Task 2: Sequential fetch for small lag (< 5 blocks)
                # Task 2: Multi-fetcher only for lag > 10 blocks
                # ==========================================

                all_logs = []  # Collect all logs from all chunks

                if lag < 5:
                    # Sequential mode: single request for small gaps
                    r_logs = await fast_rpc_request(session, {
                        "jsonrpc":"2.0",
                        "method":"eth_getLogs",
                        "params":[{
                            "address":TOKEN_CONTRACT,
                            "fromBlock":hex(last+1),
                            "toBlock":hex(target_head),
                            "topics":[[TOPIC_TRANSFER_SINGLE, TOPIC_TRANSFER_BATCH]]
                        }],
                        "id":1
                    })
                    if r_logs and 'result' in r_logs:
                        all_logs = [{'block_start': last+1, 'block_end': target_head, 'logs': r_logs['result']}]

                elif lag >= 10:
                    # Multi-fetcher mode: Dynamic chunk size based on lag
                    # Task 1: The Whale Bite - Increase throughput for large lag
                    if lag > 100:
                        CHUNK_SIZE = 50  # Extreme Catch-up
                    elif lag > 20:
                        CHUNK_SIZE = 20  # Fast Sync
                    else:
                        CHUNK_SIZE = 5   # Normal

                    chunks = []
                    start = last + 1

                    while start <= target_head:
                        end = min(start + CHUNK_SIZE - 1, target_head)
                        chunks.append((start, end))
                        start = end + 1

                    # Task 1: Use asyncio.gather with semaphore-controlled safe_fetch
                    fetch_tasks = [safe_fetch(session, chunk_start, chunk_end) for chunk_start, chunk_end in chunks]
                    results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

                    # Collect successful results
                    for result in results:
                        if result and isinstance(result, dict) and 'logs' in result:
                            all_logs.append(result)

                else:
                    # Medium gap (5-10 blocks): use sequential fetch
                    r_logs = await fast_rpc_request(session, {
                        "jsonrpc":"2.0",
                        "method":"eth_getLogs",
                        "params":[{
                            "address":TOKEN_CONTRACT,
                            "fromBlock":hex(last+1),
                            "toBlock":hex(target_head),
                            "topics":[[TOPIC_TRANSFER_SINGLE, TOPIC_TRANSFER_BATCH]]
                        }],
                        "id":1
                    })
                    if r_logs and 'result' in r_logs:
                        all_logs = [{'block_start': last+1, 'block_end': target_head, 'logs': r_logs['result']}]
                
                # ==========================================
                # Task 3: Memory & Order Protection
                # Sort all logs by block number before processing
                # ==========================================
                sorted_logs = []
                for chunk_result in all_logs:
                    if chunk_result and 'logs' in chunk_result:
                        for log in chunk_result['logs']:
                            try:
                                block_num = int(log.get('blockNumber', '0x0'), 16)
                                sorted_logs.append((block_num, log))
                            except Exception:
                                continue
                
                # Sort by block number (trades must be processed in order!)
                sorted_logs.sort(key=lambda x: x[0])
                
                # Extract just the logs after sorting
                final_logs = [log for _, log in sorted_logs]
                
                if final_logs:
                    block_trades = {}
                    for log in final_logs:
                        try:
                            # P1.6: Validate RPC log structure before any field access
                            if not log or not isinstance(log, dict):
                                continue
                            topics = log.get('topics')
                            if not topics or len(topics) < 4:
                                continue
                            log_data = log.get('data')
                            if not log_data or log_data == '0x':
                                continue

                            topic0 = topics[0].lower()
                            operator = "0x" + topics[1][26:].lower()
                            sender   = "0x" + topics[2][26:].lower()
                            trader   = "0x" + topics[3][26:].lower()

                            if trader == NULL_ADDR or trader in BLACKLIST: continue

                            is_mint = (sender == NULL_ADDR)

                            tx_hash = log['transactionHash']
                            tx_idx = int(log.get('transactionIndex', '0x0'), 16)

                            trades = []
                            if topic0 == TOPIC_TRANSFER_SINGLE:
                                tid = int(log_data[2:66], 16)
                                shares_cnt = int(log_data[66:], 16)
                                trades.append((tid, shares_cnt))
                            elif topic0 == TOPIC_TRANSFER_BATCH:
                                tids, shares_cnts = decode_transfer_batch(log_data)
                                for tid, cnt in zip(tids, shares_cnts):
                                    trades.append((tid, cnt))

                            for tid, shares_cnt in trades:
                                if tid not in WATCHLIST: continue
                                usd = (shares_cnt / 1_000_000) * WATCHLIST[tid]['p']

                                trade_key = (tx_hash, trader, tid)
                                if trade_key not in block_trades:
                                    block_trades[trade_key] = {
                                        "usd": 0, "shares": 0, "sender": sender,
                                        "operator": operator, "tx_idx": tx_idx,
                                        "is_mint": is_mint
                                    }

                                block_trades[trade_key]["usd"] += usd
                                block_trades[trade_key]["shares"] += shares_cnt
                                if is_mint: block_trades[trade_key]["is_mint"] = True

                        except Exception as log_err:
                            # Individual log parse error — skip this log, keep scanning
                            print(f"{Colors.YELLOW}[{get_time_str()}] [SCANNER] log parse error: {type(log_err).__name__}: {log_err}{Colors.RESET}")
                            continue

                    traders_to_check = set()
                    for (tx, tr, tid), data_obj in block_trades.items():
                        if data_obj["usd"] >= SCAN_MIN_USD or tr in KNOWN_INSIDERS:
                            traders_to_check.add(tr)

                    if traders_to_check:
                        await batch_check_wallets(session, list(traders_to_check))

                    for (tx, trader, tid), data_obj in block_trades.items():
                        global LAST_SCAN
                        if tid in WATCHLIST:
                            short_q = WATCHLIST[tid]['q'][:30].replace("\n", "")
                            LAST_SCAN = f"{short_q}... (${data_obj['usd']:.0f})"

                        usd = data_obj["usd"]
                        if usd < SCAN_MIN_USD and trader not in KNOWN_INSIDERS: continue

                        # FIX: We no longer skip trades if nonce is unavailable.
                        # Default to nonce=0 and treat as normal candidate.
                        # This prevents RPC lag from causing trades to be dropped.
                        cache = WALLET_CACHE.get(trader, {'nonce': 0, 'is_contract': False})

                        if trader in KNOWN_INSIDERS:
                            label = "👑 PROVEN"
                        elif data_obj.get("is_mint", False):
                            label = "🌱 MINT / SPLIT"
                        elif cache.get('is_contract', False):
                            label = "🤖 CONTRACT / BOT"
                        else:
                            label = "🕵️ CANDIDATE"

                        # Task 4: Increase Queue Buffer Warning threshold to 500 items
                        # Since we dump 150 blocks of data at once, warn at 500 instead of lower thresholds
                        queue_size = ANALYSIS_QUEUE.qsize()
                        if queue_size > 500:
                            print(f"{Colors.YELLOW}[{get_time_str()}] [⚠️ QUEUE WARNING] Queue at {queue_size}/5000 items - high throughput mode active{Colors.RESET}")

                        # ==========================================
                        # 🔥 PRIORITY QUEUE: Calculate priority (lower = higher priority)
                        # Priority 1: KNOWN_INSIDERS or trades >= $500 (WHALE/INSIDER - instant processing)
                        # Priority 10: Small trades < $500 (processed when queue is free)
                        # ==========================================
                        is_insider = trader in KNOWN_INSIDERS or "PROVEN" in label
                        priority = 1 if (is_insider or usd >= 500) else 10

                        # FIX: Handle QueueFull - skip trade if queue is at capacity
                        try:
                            # PRIORITY QUEUE: Put tuple of (priority, timestamp, task_data)
                            # timestamp is a tie-breaker to prevent Python 3 tuple comparison errors
                            # when comparing dicts (TypeError: '<' not supported between dict instances)
                            ANALYSIS_QUEUE.put_nowait((
                                priority,
                                time.time(),
                                (
                                    tx, trader, tid, usd,
                                    WATCHLIST[tid],
                                    label,
                                    cache['nonce'],
                                    CURRENT_BLOCK,
                                    data_obj["sender"],
                                    data_obj["shares"],
                                    data_obj["operator"],
                                    data_obj["tx_idx"]
                                )
                            ))
                        except asyncio.QueueFull:
                            print(f"{Colors.YELLOW}[{get_time_str()}] [⚠️ WARNING] Queue full (5000 items), skipping trade for {trader[:10]}... ${usd:,.0f}{Colors.RESET}")

                    last = target_head
                    # Task 4: Update last processed block for dashboard lag calculation
                    LAST_PROCESSED_BLOCK = last

                # FIX 3: Update sync_status at every block scan, even if no Polymarket
                # transactions were found. This prevents false watchdog alerts during
                # quiet market periods.
                await update_sync_status_async(target_head)
            except Exception as e:
                # CRITICAL: scanner_loop must never die silently.
                # Log to crash.log so you can diagnose morning-after failures.
                crash_log("scanner_loop", e, traceback.format_exc())
                await asyncio.sleep(0.5)

async def cleanup_loop():
    """
    Stage 4: Cleanup loop for memory and database maintenance.
    
    CHANGES:
    - TTLCache handles automatic expiration and LRU eviction (no manual cleanup needed)
    - Removed manual cache iteration and force-clear logic
    - Kept ACCUMULATORS and TRADER_COOLDOWNS cleanup (these are still regular dicts)
    - Kept 24-hour signal cleanup in DB
    """
    # Track last 12-hour cleanup time
    last_12h_cleanup = time.time()

    # Track last 24-hour signal cleanup
    last_24h_signal_cleanup = time.time()

    while True:
        try:
            now = time.time()
            # CLUSTERS dict removed - no cluster processing in maintest.py

            # ==========================================
            # 🚀 STAGE 4 TASK 2: TTLCache HANDLES CLEANUP AUTOMATICALLY
            # No manual iteration needed - TTLCache expires entries automatically
            # PREDICTIONS_CACHE, WALLET_CACHE, MARKET_CACHE all use TTLCache
            # ==========================================

            # Manual cleanup only for ACCUMULATORS (not a cache, but batch accumulator)
            for k in list(ACCUMULATORS.keys()):
                if (now - ACCUMULATORS[k]['start_ts']) > 86400:
                    del ACCUMULATORS[k]

            # ==========================================
            # 🛡️ RAM OVERFLOW PROTECTION (Only for ACCUMULATORS)
            # TTLCache handles its own size limits via maxsize parameter
            # ==========================================
            if len(ACCUMULATORS) > 10_000:
                ACCUMULATORS.clear()
                print(f"{Colors.YELLOW}[{get_time_str()}] [⚠️ RAM] ACCUMULATORS exceeded 10k entries — force-cleared{Colors.RESET}")

            # ==========================================
            # 🔄 12-HOUR CLEANUP: Reset accumulators and cooldowns
            # ==========================================
            if (now - last_12h_cleanup) > 43200:  # 12 hours = 43200 seconds
                ACCUMULATORS.clear()
                TRADER_COOLDOWNS.clear()
                last_12h_cleanup = now
                print(f"{Colors.CYAN}[{get_time_str()}] [🔄 CLEANUP] Cleared ACCUMULATORS and TRADER_COOLDOWNS (12-hour reset){Colors.RESET}")

            # ==========================================
            # 🧹 24-HOUR SIGNAL CLEANUP: Mark old unprocessed signals as rejected
            # ==========================================
            # Prevents DB from growing with signals that were never processed
            # Marks is_analyzed = 2 for signals older than 24 hours with is_analyzed = 0
            # ==========================================
            if (now - last_24h_signal_cleanup) > 86400:  # 24 hours = 86400 seconds
                try:
                    async with await get_async_db_connection() as conn_cleanup:
                        cutoff_ts = int(now - 86400)  # 24 hours ago

                        # Count signals that will be marked as rejected
                        async with conn_cleanup.execute("""
                            SELECT COUNT(*) as cnt FROM signals
                            WHERE is_analyzed = 0 AND timestamp < ?
                        """, (cutoff_ts,)) as cursor:
                            row = await cursor.fetchone()
                            count_before = row['cnt'] if row else 0

                        if count_before > 0:
                            # Mark old unprocessed signals as rejected (is_analyzed = 2)
                            await conn_cleanup.execute("""
                                UPDATE signals
                                SET is_analyzed = 2
                                WHERE is_analyzed = 0 AND timestamp < ?
                            """, (cutoff_ts,))
                            await conn_cleanup.commit()
                            print(f"{Colors.CYAN}[{get_time_str()}] [🧹 SIGNAL CLEANUP] Marked {count_before} old signals as rejected (is_analyzed = 2){Colors.RESET}")

                except Exception as e:
                    print(f"{Colors.RED}[{get_time_str()}] [!] Signal cleanup failed: {e}{Colors.RESET}")

                last_24h_signal_cleanup = now

            await asyncio.sleep(60)
        except Exception as e:
            crash_log("cleanup_loop", e, traceback.format_exc())
            await asyncio.sleep(60)

async def reload_insiders_loop():
    """
    Task 2: Auto-reload proven_insiders every 10 minutes using async DB.
    Ensures new insiders added via admin_bot are applied without restart.
    """
    reload_interval = 600  # 10 minutes = 600 seconds
    last_reload = time.time()

    while True:
        try:
            now = time.time()

            # Reload every 10 minutes
            if (now - last_reload) > reload_interval:
                old_count = len(KNOWN_INSIDERS)
                await load_insiders_async()
                new_count = len(KNOWN_INSIDERS)

                print(f"{Colors.CYAN}[{get_time_str()}] [SYSTEM] Reloaded proven_insiders from DB. Current count: {new_count}{Colors.RESET}")

                # Log if changes detected
                if new_count != old_count:
                    print(f"{Colors.GREEN}[{get_time_str()}] [SYSTEM] Insiders changed: {old_count} → {new_count}{Colors.RESET}")

                last_reload = now

            await asyncio.sleep(60)  # Check every minute
        except Exception as e:
            print(f"{Colors.RED}[{get_time_str()}] [ERROR] reload_insiders_loop failed: {e}{Colors.RESET}")
            await asyncio.sleep(60)

async def main():
    global API_SESSION, ANALYSIS_QUEUE, DB_WRITE_QUEUE

    # Очистка консоли
    os.system('cls' if os.name == 'nt' else 'clear')
    print(f"{Colors.CYAN}{Colors.BOLD}🛡️ POLYMARKET SNIPER v2.0 {Colors.RESET}")
    print(f"{Colors.GREEN}├─ 🔗 On-Chain Price Calculation: {Colors.BOLD}ENABLED{Colors.RESET}{Colors.GREEN}")
    print(f"│  USDC Contracts: Native + Bridged{Colors.RESET}")
    print(f"{Colors.GREEN}├─ 📊 Slippage Detection: {Colors.BOLD}ACTIVE{Colors.RESET}{Colors.GREEN} (>2% threshold)")
    print(f"│  RPC Endpoints: {len(RPC_LIST)}{Colors.RESET}")
    print(f"{Colors.GREEN}├─ 🎯 Batch TX Handler: {Colors.BOLD}ACTIVE{Colors.RESET}{Colors.GREEN}")
    print(f"│  Detects individual traders in relayer batches{Colors.RESET}")

    # Первым делом — проверка окружения
    from config import check_required_env
    check_required_env()

    # Initialize queues inside event loop (Python 3.12+ compatibility)
    ANALYSIS_QUEUE = asyncio.PriorityQueue(maxsize=5000)
    DB_WRITE_QUEUE = asyncio.Queue()

    # STAGE 2: Schema migrations handled by standalone migrate.py
    # Run 'python migrate.py' ONCE before starting workers

    # P2.3: One-time migration of proven_insiders.txt to SQLite
    from config import migrate_insiders_file_to_db
    insiders_file_path = os.path.join(BASE_DIR, "support", "insiders.txt")
    migrated = migrate_insiders_file_to_db(insiders_file_path)
    if migrated > 0:
        print(f"{Colors.GREEN}[MIGRATION] Imported {migrated} insiders from proven_insiders.txt to DB{Colors.RESET}")

    await init_storage_async()
    await load_insiders_async()

    # --- СОЗДАНИЕ ВЕЧНОЙ СЕССИИ (Fix connection issues) ---
    connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300)
    timeout_settings = aiohttp.ClientTimeout(total=30, connect=10)

    API_SESSION = aiohttp.ClientSession(connector=connector, timeout=timeout_settings)

    # Запуск фоновых задач
    # ==========================================
    # 🚀 STAGE 4 TASK 4: Start DB batch writer
    # ==========================================
    asyncio.create_task(db_writer_loop())
    asyncio.create_task(update_markets_loop())
    asyncio.create_task(dashboard_loop())
    asyncio.create_task(cleanup_loop())
    asyncio.create_task(reload_insiders_loop())

    for _ in range(CONCURRENT_ANALYSIS):
        asyncio.create_task(analyzer_worker())
    
    # Ожидание загрузки маркетов
    while not WATCHLIST:
        sys.stdout.write(f"\r{Colors.YELLOW}⏳ Loading Markets...{Colors.RESET}")
        sys.stdout.flush()
        await asyncio.sleep(1)
    
    print(f"\n{Colors.GREEN}[+] Loaded. Waiting for signals...{Colors.RESET}")
    
    try: 
        await scanner_loop()
    except KeyboardInterrupt: 
        pass
    finally:
        # Корректное закрытие сессии при выходе
        if API_SESSION:
            await API_SESSION.close()
            print(f"\n{Colors.CYAN}[*] API Session Closed.{Colors.RESET}")

if __name__ == "__main__":
    # !!! ИСПРАВЛЕНИЕ ДЛЯ WINDOWS !!!
    # Эта строчка ДОЛЖНА быть Д   asyncio.run(), иначе скрипт прост   висит или закрывается
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}[!] Stopped by user{Colors.RESET}")
    except Exception as e:
        # Если скрипт падает, мы увидим ошибку
        print(f"\n{Colors.RED}[!!!] FATAL ERROR: {e}{Colors.RESET}")
        import traceback
        traceback.print_exc()
        input("\nPress Enter to exit...")
