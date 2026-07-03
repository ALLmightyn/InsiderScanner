#!/usr/bin/env python3
"""
enrich_v2.py — Sidecar enricher for signals ("crystal data" protocol, 2026-06-10).

Does NOT touch the detector (maintest.py). Polls the signals table for new
quality signals and, at the moment of detection, appends to signals_enrich:
  - CLOB order book snapshot (raw JSON) + executable prices for $100/$300/$1000
  - real slug/negRisk/category/endDate/liquidity from Gamma (by token_id)
  - block_ts from Polygon RPC -> detect_lag_s per row
  - heartbeat once every 60s (uptime is provable)

Design invariants:
  - cursor starts at MAX(id): the order book only makes sense in real time,
    backfilling old signals is not allowed.
  - idempotency: signal_id PRIMARY KEY, INSERT OR IGNORE.
  - single instance: pid lockfile (lesson learned from the ADA double-launch
    incident in HLCarryBot).
  - no hypothesis-based filtering: we write all FRESH/WHALE/ACCUM, including <$700
    (negative control). The hypothesis is applied only at analysis time.
"""
import asyncio
import aiohttp
import aiosqlite
import json
import os
import sys
import time
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "database", "scanner.db")
LOCK_FILE = "/tmp/enrich_v2.pid"

CLOB_BOOK = "https://clob.polymarket.com/book"
GAMMA = "https://gamma-api.polymarket.com/markets"
POLYGON_RPCS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
    "https://polygon.drpc.org",
]

QUALITY_SQL = "(type LIKE '%FRESH%' OR type LIKE '%WHALE%' OR type LIKE '%ACCUMULATION%')"
POLL_S = 2.0
HEARTBEAT_S = 60
EXEC_SIZES = (100.0, 300.0, 1000.0)

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [ENRICH] {msg}", flush=True)

# ---------- single instance lock ----------
def acquire_lock():
    if os.path.exists(LOCK_FILE):
        try:
            old = int(open(LOCK_FILE).read().strip())
            os.kill(old, 0)
            log(f"FATAL: already running as pid={old}, exiting")
            sys.exit(1)
        except (ProcessLookupError, ValueError):
            pass  # stale lock
    open(LOCK_FILE, "w").write(str(os.getpid()))

# ---------- db ----------
async def init_tables(db):
    await db.execute("""CREATE TABLE IF NOT EXISTS signals_enrich (
        signal_id INTEGER PRIMARY KEY,
        tx_hash TEXT, token_id TEXT,
        block_number INTEGER, block_ts INTEGER, enrich_ts INTEGER, detect_lag_s REAL,
        best_bid REAL, best_ask REAL, mid REAL,
        exec_px_100 REAL, exec_px_300 REAL, exec_px_1000 REAL,
        ask_depth_12x_usd REAL,
        book_json TEXT,
        gamma_slug TEXT, neg_risk INTEGER, end_date TEXT,
        liquidity REAL, volume24h REAL, is_sports INTEGER,
        enrich_status TEXT)""")
    await db.execute("""CREATE TABLE IF NOT EXISTS collector_heartbeat (
        ts INTEGER PRIMARY KEY, cursor_id INTEGER,
        enriched_total INTEGER, errors_total INTEGER)""")
    await db.commit()

async def get_cursor(db):
    cur = await db.execute(
        "SELECT last_run FROM system_state WHERE task_name='enrich_v2_cursor'")
    row = await cur.fetchone()
    if row is not None:
        return row[0]
    cur = await db.execute("SELECT COALESCE(MAX(id),0) FROM signals")
    max_id = (await cur.fetchone())[0]
    await db.execute(
        "INSERT OR REPLACE INTO system_state (task_name, last_run) VALUES ('enrich_v2_cursor', ?)",
        (max_id,))
    await db.commit()
    log(f"first run: cursor = MAX(id) = {max_id} (no backfill — the order book is only valid live)")
    return max_id

async def save_cursor(db, cid):
    await db.execute(
        "INSERT OR REPLACE INTO system_state (task_name, last_run) VALUES ('enrich_v2_cursor', ?)",
        (cid,))
    await db.commit()

# ---------- enrichment pieces ----------
SPORT_KEYS = ("cbb-", "nba-", "nhl-", "mlb-", "epl-", "ucl-", "cfb-", "nfl-", "atp-",
              "wta-", "mls-", "cs2-", "lol-", "uel-", "sea-", "spread", "moneyline", "-vs-")

class Enricher:
    def __init__(self, session):
        self.s = session
        self.gamma_cache = {}   # token_id -> dict
        self.block_cache = {}   # block_number -> ts
        self.rpc_idx = 0

    async def book(self, token_id):
        try:
            async with self.s.get(CLOB_BOOK, params={"token_id": token_id},
                                  timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status != 200:
                    return None
                return await r.json()
        except Exception:
            return None

    async def gamma(self, token_id):
        if token_id in self.gamma_cache:
            return self.gamma_cache[token_id]
        try:
            for extra in ((("closed", "false"),), ()):
                params = [("clob_token_ids", token_id), *extra]
                async with self.s.get(GAMMA, params=params,
                                      timeout=aiohttp.ClientTimeout(total=8)) as r:
                    if r.status != 200:
                        continue
                    d = await r.json()
                    if d:
                        m = d[0]
                        out = {
                            "slug": m.get("slug"),
                            "neg_risk": 1 if m.get("negRisk") else 0,
                            "end_date": m.get("endDate"),
                            "liquidity": float(m.get("liquidityNum") or m.get("liquidity") or 0),
                            "volume24h": float(m.get("volume24hr") or 0),
                        }
                        self.gamma_cache[token_id] = out
                        return out
        except Exception:
            pass
        return None

    async def block_ts(self, block_number):
        if not block_number:
            return None
        if block_number in self.block_cache:
            return self.block_cache[block_number]
        payload = {"jsonrpc": "2.0", "method": "eth_getBlockByNumber",
                   "params": [hex(block_number), False], "id": 1}
        for i in range(len(POLYGON_RPCS)):
            url = POLYGON_RPCS[(self.rpc_idx + i) % len(POLYGON_RPCS)]
            try:
                async with self.s.post(url, json=payload,
                                       timeout=aiohttp.ClientTimeout(total=6)) as r:
                    d = await r.json()
                    ts = int(d["result"]["timestamp"], 16)
                    self.rpc_idx = (self.rpc_idx + i) % len(POLYGON_RPCS)
                    if len(self.block_cache) > 5000:
                        self.block_cache.clear()
                    self.block_cache[block_number] = ts
                    return ts
            except Exception:
                continue
        return None

    @staticmethod
    def walk_asks(book, usd_amounts, trade_px):
        """Executable average buy price for $X on the ask side + depth within 1.2x trade_px."""
        try:
            asks = sorted(
                [(float(a["price"]), float(a["size"])) for a in book.get("asks", [])],
                key=lambda x: x[0])
        except Exception:
            return {}, None
        out = {}
        for usd in usd_amounts:
            remaining, cost, shares = usd, 0.0, 0.0
            for px, size in asks:
                level_usd = px * size
                take = min(remaining, level_usd)
                shares += take / px
                cost += take
                remaining -= take
                if remaining <= 0:
                    break
            out[usd] = (cost / shares) if shares > 0 and remaining <= 0 else None
        depth = None
        if trade_px:
            depth = sum(px * size for px, size in asks if px <= trade_px * 1.2)
        return out, depth

    async def enrich(self, sig):
        enrich_ts = time.time()
        token_id = sig["token_id"]
        status = []

        book = await self.book(token_id) if token_id else None
        best_bid = best_ask = mid = None
        exec_px = {u: None for u in EXEC_SIZES}
        depth = None
        if book:
            try:
                bids = [float(b["price"]) for b in book.get("bids", [])]
                asks = [float(a["price"]) for a in book.get("asks", [])]
                best_bid = max(bids) if bids else None
                best_ask = min(asks) if asks else None
                if best_bid is not None and best_ask is not None:
                    mid = (best_bid + best_ask) / 2
                exec_px, depth = self.walk_asks(book, EXEC_SIZES, sig["price"])
            except Exception:
                status.append("book_parse_fail")
        else:
            status.append("book_fail")

        g = await self.gamma(token_id) if token_id else None
        if g is None:
            status.append("gamma_fail")
            g = {}

        bts = await self.block_ts(sig["block_number"])
        if bts is None:
            status.append("blockts_fail")

        slug = g.get("slug") or sig["slug"] or ""
        is_sports = 1 if any(k in slug for k in SPORT_KEYS) else 0

        return (
            sig["id"], sig["tx_hash"], token_id,
            sig["block_number"], bts, int(enrich_ts),
            (enrich_ts - bts) if bts else None,
            best_bid, best_ask, mid,
            exec_px.get(100.0), exec_px.get(300.0), exec_px.get(1000.0),
            depth,
            json.dumps(book) if book else None,
            g.get("slug"), g.get("neg_risk"), g.get("end_date"),
            g.get("liquidity"), g.get("volume24h"), is_sports,
            ",".join(status) if status else "ok",
        )

async def main():
    acquire_lock()
    log(f"start pid={os.getpid()} db={DB_PATH}")
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL;")
    await db.execute("PRAGMA synchronous=NORMAL;")
    await init_tables(db)
    cursor = await get_cursor(db)
    enriched_total = errors_total = 0
    last_hb = 0.0

    async with aiohttp.ClientSession() as session:
        en = Enricher(session)
        while True:
            try:
                cur = await db.execute(
                    f"""SELECT id, tx_hash, token_id, block_number, price, slug
                        FROM signals WHERE id > ? AND {QUALITY_SQL}
                        ORDER BY id LIMIT 50""", (cursor,))
                rows = await cur.fetchall()

                for sig in rows:
                    try:
                        rec = await en.enrich(sig)
                        await db.execute(
                            """INSERT OR IGNORE INTO signals_enrich VALUES
                               (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", rec)
                        enriched_total += 1
                        if rec[-1] != "ok":
                            errors_total += 1
                    except Exception as e:
                        errors_total += 1
                        log(f"enrich fail id={sig['id']}: {e}")

                if len(rows) == 50:
                    # full batch: there may be more quality signals — only advance to the last one processed
                    new_cursor = rows[-1]["id"]
                else:
                    # all quality signals up to MAX(id) processed — skip past bot spam
                    cur2 = await db.execute("SELECT COALESCE(MAX(id), ?) FROM signals", (cursor,))
                    new_cursor = (await cur2.fetchone())[0]
                if new_cursor != cursor:
                    cursor = new_cursor
                    await save_cursor(db, cursor)
                else:
                    await db.commit()

                now = time.time()
                if now - last_hb >= HEARTBEAT_S:
                    await db.execute(
                        "INSERT OR REPLACE INTO collector_heartbeat VALUES (?,?,?,?)",
                        (int(now), cursor, enriched_total, errors_total))
                    await db.commit()
                    last_hb = now

                await asyncio.sleep(POLL_S)
            except Exception as e:
                log(f"loop error: {e}")
                errors_total += 1
                await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
