#!/usr/bin/env python3
"""
config.py — InsiderScanner Configuration
Central configuration for all scanner worker modules.
"""

import os
import asyncio
import aiosqlite
import aiofiles
import time
import traceback
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, AsyncIterator, Any as typing_Any

import redis.asyncio as redis

redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# ==========================================
# TOKEN BUCKET RATE LIMITER
# ==========================================
class AsyncTokenBucket:
    def __init__(self, rate: float, capacity: int):
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_update = time.monotonic()
        self.lock = asyncio.Lock()

    async def consume(self, tokens: int = 1):
        async with self.lock:
            while True:
                now = time.monotonic()
                elapsed = now - self.last_update
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                self.last_update = now
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return
                wait_time = (tokens - self.tokens) / self.rate
                await asyncio.sleep(wait_time)

polygonscan_limiter = AsyncTokenBucket(rate=4.5, capacity=5)
polymarket_limiter = AsyncTokenBucket(rate=15.0, capacity=20)

# ==========================================
# LOAD .ENV
# ==========================================
try:
    from dotenv import load_dotenv
    _env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    load_dotenv(_env_path)
except ImportError:
    pass

# ==========================================
# PATHS — anchored to project root (one level up from config/)
# ==========================================
_CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(_CONFIG_DIR)          # InsiderScanner/
DB_DIR = os.path.join(BASE_DIR, "database")
DB_FILENAME = os.path.join(DB_DIR, "scanner.db")
CRASH_LOG_PATH = os.path.join(BASE_DIR, "logs", "crash.log")
CUSTOM_LABELS_FILE = os.path.join(BASE_DIR, "support", "custom_labels.json")

os.makedirs(DB_DIR, exist_ok=True)
os.makedirs(os.path.join(BASE_DIR, "logs"), exist_ok=True)

# ==========================================
# CRASH LOGGER
# ==========================================
def crash_log(source: str, error: Exception, tb: str = ""):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"[{ts}] [{source}] {type(error).__name__}: {error}\n"
    if tb:
        msg += tb + "\n"
    msg += "=" * 60 + "\n"
    try:
        with open(CRASH_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(msg)
    except Exception:
        pass
    print(msg)


async def crash_log_async(source: str, error: Exception, tb: str = ""):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"[{ts}] [{source}] {type(error).__name__}: {error}\n"
    if tb:
        msg += tb + "\n"
    msg += "=" * 60 + "\n"
    try:
        async with aiofiles.open(CRASH_LOG_PATH, "a", encoding="utf-8") as f:
            await f.write(msg)
    except Exception:
        pass
    print(msg)

# ==========================================
# FILE-BASED LOCKING
# ==========================================
LOCK_DIR = "/tmp/polymarket_scanner_locks"

def _ensure_lock_dir():
    os.makedirs(LOCK_DIR, exist_ok=True)

def get_lock_path(task_name: str) -> str:
    _ensure_lock_dir()
    return os.path.join(LOCK_DIR, f"{task_name}.lock")

def is_locked(task_name: str) -> bool:
    lock_path = get_lock_path(task_name)
    if not os.path.exists(lock_path):
        return False
    try:
        with open(lock_path, 'r') as f:
            pid = int(f.read().strip())
        os.kill(pid, 0)
        return True
    except (FileNotFoundError, ValueError, ProcessLookupError, PermissionError):
        try:
            os.remove(lock_path)
        except Exception:
            pass
        return False

def acquire_lock(task_name: str) -> bool:
    lock_path = get_lock_path(task_name)
    _ensure_lock_dir()
    if is_locked(task_name):
        return False
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        with os.fdopen(fd, 'w') as f:
            f.write(str(os.getpid()))
        return True
    except FileExistsError:
        return False
    except Exception as e:
        print(f"[LOCK] Failed to acquire lock for {task_name}: {e}")
        return False

def release_lock(task_name: str) -> bool:
    lock_path = get_lock_path(task_name)
    try:
        if os.path.exists(lock_path):
            with open(lock_path, 'r') as f:
                pid = int(f.read().strip())
            if pid == os.getpid():
                os.remove(lock_path)
                return True
        return False
    except Exception as e:
        print(f"[LOCK] Failed to release lock for {task_name}: {e}")
        try:
            os.remove(lock_path)
            return True
        except Exception:
            return False

# ==========================================
# TELEGRAM
# ==========================================
TG_TOKEN = os.getenv('TG_TOKEN')
TG_CHAT_ID = os.getenv('TG_CHAT_ID')
TG_INSIDER_CHAT_ID = os.getenv('TG_INSIDER_CHAT_ID')

# ==========================================
# API CONFIG
# ==========================================
POLYMARKET_GAMMA_API = "https://gamma-api.polymarket.com/markets"
POLYMARKET_CLOB_HISTORY = "https://clob.polymarket.com/prices-history"
POLYGONSCAN_API_KEY = os.getenv("POLYGONSCAN_API_KEY")
V2_API_URL = "https://api.etherscan.io/v2/api"
CHAIN_ID = "137"
USDC_CONTRACT = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"

# ==========================================
# SCANNER ANALYSIS SETTINGS
# ==========================================
PRED_LIMIT_NEW = 30
USD_LIMIT_NEW = 500
USD_LIMIT_OLD = 3000

CLUSTER_MIN_PEOPLE = 3
CLUSTER_WINDOW = 3600
CLUSTER_ENTRY_USD = 100

BOT_TRADE_THRESHOLD = 500
ORACLE_MIN_MEANINGFUL_TRADES = 5
ORACLE_MAX_TRADES = 500
SLOW_CLUSTER_MIN_USD = 25000
SLOW_CLUSTER_MIN_WALLETS = 3
INSTANT_ATTACK_MIN_USD = 2000

FRESH_CLUSTER_MIN_TRADES = 10
FRESH_CLUSTER_MIN_USD = 1000
DORMANT_THRESHOLD_BETS = 5

_CLEANUP_DAYS = int(os.getenv("CLEANUP_DAYS_AGO", "30"))

def get_data_cleanup_cutoff() -> int:
    return int((datetime.utcnow() - timedelta(days=_CLEANUP_DAYS)).timestamp())

MAX_PRICE_FILTER = 0.9
SCAN_MIN_USD = 10

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
USE_AI_FILTER = True

WALLET_CACHE_TTL = 3600
TOKEN_CONTRACT = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"
NULL_ADDR = "0x0000000000000000000000000000000000000000"

POLYMARKET_CONTRACTS = {
    "0x4d97dcd97ec945f40cf65f87097ace5ea0476045".lower(),
    "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e".lower(),
    "0xdb01f3e7a8370de8d2df1b9af0285a81413a1a54".lower(),
    "0xeb28cd5fb88f8d6fb24785ca88b0a9f5d37df210".lower(),
    "0x2b8102a0a382e753444265443213039d677d612e".lower(),
    "0xc5d563a36ae78145c45a50134d48a1215220f80a".lower(),
    "0x0000000000000000000000000000000000000000",
}

def check_required_env():
    required = {
        'TG_TOKEN': TG_TOKEN,
        'TG_CHAT_ID': TG_CHAT_ID,
        'POLYGONSCAN_API_KEY': POLYGONSCAN_API_KEY,
        'CEREBRAS_API_KEY': os.getenv('CEREBRAS_API_KEY'),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise EnvironmentError(f"[STARTUP] Missing required env variables: {', '.join(missing)}")

CEREBRAS_API_KEY = os.getenv("CEREBRAS_API_KEY")

# ==========================================
# CASINO KEYWORDS (for market_discovery.py)
# ==========================================
CASINO_KEYWORDS = [
    "STRIKE", "BOMB", "MISSILE", "ATTACK", "OFFENSIVE", "TROOPS", "CAPTURE",
    "INVADE", "INVASION", "CEASEFIRE", "DRONE", "CASUALTY", "HOSTAGE",
    "AIRSTRIKE", "NAVAL", "BATTALION", "FRONTLINE", "SIEGE",
    "FIRED AS", "RESIGN", "ASSASSINATION", "ASSASSINATE", "EXECUTE",
    "ARRESTED", "INDICTED", "CONVICTED", "IMPEACH",
    "HAMAS", "HEZBOLLAH", "HOUTHI", "ISIS", "WAGNER", "IDF", "KREMLIN", "PENTAGON SAYS",
    "WILL TRUMP SAY", "WILL TRUMP TWEET", "WILL TRUMP POST", "WILL BIDEN SAY",
    "POSTED ON TRUTH", "TELEGRAM CHANNEL", "FULL LID", "PRESS BRIEFING TODAY",
    "IN THE NEXT 24", "BEFORE MIDNIGHT", "BY END OF DAY",
    "REACH $", "HIT $", "DROP TO $", "DIPS BELOW", "PUMPS TO", "ALL-TIME HIGH", "CRASH TO",
]

# ==========================================
# ASYNC DATABASE HELPERS
# ==========================================
_db_connections = {}

async def get_async_db_connection(timeout: float = 30.0) -> aiosqlite.Connection:
    conn = await aiosqlite.connect(DB_FILENAME, timeout=timeout)
    await conn.execute("PRAGMA journal_mode=WAL;")
    await conn.execute("PRAGMA synchronous=NORMAL;")
    await conn.execute("PRAGMA busy_timeout=60000;")
    conn.row_factory = aiosqlite.Row
    return conn

async def get_persistent_async_db_connection(name: str = "default") -> aiosqlite.Connection:
    global _db_connections
    conn = _db_connections.get(name)
    if conn is not None:
        try:
            await conn.execute("SELECT 1")
        except Exception:
            try:
                await conn.close()
            except Exception:
                pass
            conn = None
            _db_connections.pop(name, None)
    if conn is None:
        conn = await get_async_db_connection()
        _db_connections[name] = conn
    return conn

async def close_all_async_db_connections():
    global _db_connections
    for name, conn in _db_connections.items():
        try:
            await conn.close()
        except Exception:
            pass
    _db_connections.clear()

def get_db_connection(timeout: float = 30.0):
    import sqlite3
    conn = sqlite3.connect(DB_FILENAME, timeout=timeout)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=60000;")
    conn.row_factory = sqlite3.Row
    return conn

# ==========================================
# SYNC STATUS HELPERS
# ==========================================
async def update_sync_status_async(last_block: int):
    conn = await get_async_db_connection()
    try:
        await conn.execute(
            "INSERT OR REPLACE INTO sync_status (id, last_block, updated_at) VALUES (1, ?, ?)",
            (last_block, int(time.time()))
        )
        await conn.commit()
    finally:
        await conn.close()

def update_sync_status(last_block: int):
    conn = get_db_connection()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO sync_status (id, last_block, updated_at) VALUES (1, ?, ?)",
            (last_block, int(time.time()))
        )
        conn.commit()
    finally:
        conn.close()

async def get_latest_sync_block_async() -> int:
    conn = await get_async_db_connection()
    try:
        cursor = await conn.execute("SELECT last_block, updated_at FROM sync_status WHERE id = 1")
        row = await cursor.fetchone()
        if row:
            return row['last_block']
        return 0
    finally:
        await conn.close()

def get_latest_sync_block() -> int:
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT last_block, updated_at FROM sync_status WHERE id = 1").fetchone()
        if row:
            return row['last_block']
        return 0
    finally:
        conn.close()

# ==========================================
# CRON STATE HELPERS
# ==========================================
async def get_cron_state_async(conn: aiosqlite.Connection, task_name: str) -> int:
    cursor = await conn.execute(
        "SELECT last_run FROM system_state WHERE task_name = ?",
        (task_name,)
    )
    row = await cursor.fetchone()
    return row['last_run'] if row else 0

async def set_cron_state_async(conn: aiosqlite.Connection, task_name: str, timestamp: int):
    await conn.execute("""
        INSERT INTO system_state (task_name, last_run)
        VALUES (?, ?)
        ON CONFLICT(task_name) DO UPDATE SET last_run = excluded.last_run
    """, (task_name, timestamp))
    await conn.commit()

def get_cron_state(conn, task_name: str) -> int:
    row = conn.execute(
        "SELECT last_run FROM system_state WHERE task_name = ?",
        (task_name,)
    ).fetchone()
    return row['last_run'] if row else 0

def set_cron_state(conn, task_name: str, timestamp: int):
    try:
        conn.execute("BEGIN IMMEDIATE TRANSACTION")
        conn.execute("""
            INSERT INTO system_state (task_name, last_run)
            VALUES (?, ?)
            ON CONFLICT(task_name) DO UPDATE SET last_run = excluded.last_run
        """, (task_name, timestamp))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e

# ==========================================
# QUERY EXECUTOR
# ==========================================
async def execute_query_with_retry_async(
    conn: aiosqlite.Connection,
    query: str,
    params: tuple = (),
    max_retries: int = 5
) -> aiosqlite.Cursor:
    for attempt in range(max_retries):
        try:
            return await conn.execute(query, params)
        except aiosqlite.OperationalError as e:
            error_msg = str(e)
            if "locked" in error_msg.lower() and attempt < max_retries - 1:
                wait_time = 0.1 * (2 ** attempt)
                await asyncio.sleep(wait_time)
                continue
            raise e
    raise aiosqlite.OperationalError("Max retries exceeded")

def execute_query_with_retry(conn, query, params=(), max_retries=5):
    import sqlite3
    for attempt in range(max_retries):
        try:
            return conn.execute(query, params)
        except sqlite3.OperationalError as e:
            error_msg = str(e)
            if "locked" in error_msg.lower() and attempt < max_retries - 1:
                wait_time = 0.1 * (2 ** attempt)
                time.sleep(wait_time)
                continue
            raise e

# ==========================================
# PROVEN INSIDERS HELPERS
# ==========================================
async def load_proven_insiders_from_db_async() -> set:
    try:
        conn = await get_async_db_connection()
        cursor = await conn.execute(
            "SELECT wallet_addr FROM proven_insiders WHERE is_active = 1"
        )
        rows = await cursor.fetchall()
        await conn.close()
        return {row['wallet_addr'].lower() for row in rows}
    except Exception as e:
        await crash_log_async("load_proven_insiders_from_db_async", e, "")
        return set()

def load_proven_insiders_from_db() -> set:
    try:
        conn = get_db_connection()
        rows = conn.execute(
            "SELECT wallet_addr FROM proven_insiders WHERE is_active = 1"
        ).fetchall()
        conn.close()
        return {row['wallet_addr'].lower() for row in rows}
    except Exception as e:
        crash_log("load_proven_insiders_from_db", e, "")
        return set()

def migrate_insiders_file_to_db(filepath: str) -> int:
    if not os.path.exists(filepath):
        return 0
    conn = get_db_connection()
    count = 0
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                wallet = line.strip().lower()
                if wallet.startswith("0x") and len(wallet) == 42:
                    conn.execute(
                        "INSERT OR IGNORE INTO proven_insiders (wallet_addr, added_at, added_by, notes) VALUES (?, ?, ?, ?)",
                        (wallet, int(time.time()), "file_migration", "imported from proven_insiders.txt"),
                    )
                    count += 1
        conn.commit()
    except Exception as e:
        crash_log("migrate_insiders_file_to_db", e, "")
    finally:
        conn.close()
    return count

# ==========================================
# OUTCOME NORMALIZATION
# ==========================================
def normalize_outcome(outcome: str) -> str:
    if not outcome:
        return ""
    cleaned = outcome.replace('🟢 ', '').replace('🔴 ', '').strip().lower()
    return cleaned

def normalize_outcome_for_price(outcome: str) -> str:
    if not outcome:
        return ""
    cleaned = outcome.replace('🟢 ', '').replace('🔴 ', '').strip()
    return cleaned.upper()

# ==========================================
# CONFIDENCE SCORE HELPERS
# ==========================================
def calculate_confidence_score(
    is_sybil: bool = False,
    is_instant_funding: bool = False,
    is_fresh_capital: bool = False,
    is_elite: bool = False,
    is_mixer: bool = False,
    is_early_bird: bool = False,
    cluster_size: int = 0,
    win_rate: float = 0.0,
    pnl_pct: float = 0.0
) -> Dict[str, Any]:
    score = 0
    reasons = []
    if is_mixer:
        score += 40
        reasons.append("Mixer")
    if is_sybil:
        score += 35
        reasons.append("Sybil")
    if is_instant_funding:
        score += 30
        reasons.append("Instant")
    if is_elite:
        score += 25
        reasons.append("Elite")
    if is_early_bird:
        score += 20
        reasons.append("Early")
    if is_fresh_capital:
        score += 15
        reasons.append("Fresh")
    if cluster_size >= 5:
        score += 20
        reasons.append(f"Cluster x{cluster_size}")
    elif cluster_size >= 3:
        score += 15
    if pnl_pct >= 100:
        score += 15
    combo_count = sum([is_sybil, is_instant_funding, is_elite, is_mixer, is_early_bird])
    if combo_count >= 3:
        score = int(score * 1.5)
    elif combo_count >= 2:
        score = int(score * 1.2)
    score = min(score, 100)
    if score >= 90:
        return {'score': score, 'level': 'NUCLEAR', 'emoji': '☢️'}
    elif score >= 70:
        return {'score': score, 'level': 'CRITICAL', 'emoji': '🚨'}
    elif score >= 50:
        return {'score': score, 'level': 'HIGH', 'emoji': '⚠️'}
    elif score >= 30:
        return {'score': score, 'level': 'MEDIUM', 'emoji': '👁️'}
    else:
        return {'score': score, 'level': 'LOW', 'emoji': '👀'}

def get_confidence_badge(confidence: Dict[str, Any]) -> str:
    return f"{confidence['emoji']} <b>{confidence['level']}</b> ({confidence['score']}/100)"

def format_wallet_short(wallet: str) -> str:
    return f"{wallet[:6]}...{wallet[-4:]}" if len(wallet) >= 10 else wallet

def format_pnl(pnl_usd: float, pnl_pct: float) -> str:
    sign = "+" if pnl_usd >= 0 else ""
    return f"{sign}${pnl_usd:,.0f} ({sign}{pnl_pct:.1f}%)"

# ==========================================
# UI COLORS
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

def log(msg, color=Colors.RESET):
    t = datetime.now().strftime("%H:%M:%S")
    print(f"{color}[{t}] {msg}{Colors.RESET}")

# ==========================================
# POLYMARKET DATA API HELPERS
# ==========================================
async def get_polymarket_predictions(session, wallet_address: str) -> int:
    params = {"user": wallet_address}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Connection": "keep-alive"
    }
    try:
        await polymarket_limiter.consume()
        async with session.get(
            "https://data-api.polymarket.com/traded",
            params=params,
            headers=headers,
            timeout=10
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get("traded", 0)
            return 0
    except Exception:
        return 0
