#!/usr/bin/env python3
"""
market_discovery.py — AI Market Filter Worker
Runs independently, checks all active Polymarket markets via AI,
writes results to markets_whitelist table.
maintest.py and funding_tracer_test.py read from this table instantly (no API calls in hot path).
"""
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
import time
import os
import traceback
from datetime import datetime

from config import (
    get_async_db_connection,
    crash_log,
    USE_AI_FILTER,
    CASINO_KEYWORDS,
)

CEREBRAS_API_KEY = os.getenv("CEREBRAS_API_KEY")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
POLYMARKET_GAMMA_API = "https://gamma-api.polymarket.com/markets"
DISCOVERY_INTERVAL = 10   # 10 seconds — maximum speed for pending signals
AI_REQUEST_DELAY  = 4.2   # ~17 req/min — optimized for Cerebras Free Tier (no 60s blocks)
AI_DECISIONS_LOG = os.path.join(BASE_DIR, "ai_decisions.log")

# Proper asyncio lock to prevent overlapping discovery runs
_discovery_lock = asyncio.Lock()


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [DISCOVERY] {msg}")


async def log_ai_decision(slug: str, question: str, status: str, reason: str):
    """
    Log AI decision to ai_decisions.log.
    Async version using aiofiles for non-blocking I/O.
    Format: [YYYY-MM-DD HH:MM:SS] [APPROVED/REJECTED] [Reason: AI/Keywords/Dead] Market: {question} (slug: {slug})
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status_str = "APPROVED" if status == "approved" else "REJECTED"
    # Truncate question for log readability
    q_short = question[:80] + "..." if len(question) > 80 else question
    log_line = f"[{timestamp}] [{status_str}] [Reason: {reason}] Market: {q_short} (slug: {slug})\n"

    try:
        async with aiofiles.open(AI_DECISIONS_LOG, "a", encoding="utf-8") as f:
            await f.write(log_line)
    except Exception as e:
        print(f"[DISCOVERY] Failed to write to ai_decisions.log: {e}")


def fast_casino_check(question: str) -> bool:
    """Returns True if market passes keyword filter (not a casino market)."""
    q_upper = question.upper()
    return not any(kw in q_upper for kw in CASINO_KEYWORDS)


async def ai_check(session: aiohttp.ClientSession, question: str) -> bool:
    """
    Returns True if market is insider-tradeable (geopolitics, corporate events, etc).
    Uses Cerebras API (llama-3.3-70b).
    FAIL-SAFE: Returns False on API errors to prevent junk markets from being approved.
    """
    if not USE_AI_FILTER or not CEREBRAS_API_KEY:
        return True

    url = "https://api.cerebras.ai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {CEREBRAS_API_KEY}",
        "Content-Type": "application/json",
    }
    system_prompt = (
        "You are a Conservative Risk Manager for an automated Market Making bot "
        "operating with a micro-account ($50). Your goal is survival. "
        "You must SELECT SLOW-MOVING, MEAN-REVERTING markets and STRICTLY REJECT "
        "news-driven, toxic, or explosive markets.\n\n"
        "APPROVE ('TRUE') ONLY IF:\n"
        "- Entertainment, Pop Culture, Box Office, Oscars.\n"
        "- Distant Elections (voting day is > 2 months away).\n"
        "- Long-term sports seasons (e.g., 'Winner of NBA Finals' months in advance).\n"
        "- Expected slow, gradual price discovery.\n\n"
        "REJECT ('FALSE') IMMEDIATELY IF:\n"
        "- Geopolitics, war, Middle East, Israel, Iran, Russia, Ukraine (100% TOXIC).\n"
        "- Court verdicts, arrests, legal decisions, indicted figures.\n"
        "- Breaking news, assassination attempts, emergencies.\n"
        "- Token launches, airdrops, crypto price targets.\n"
        "- Resolves in LESS than 14 days.\n\n"
        "Reply ONLY 'TRUE' or 'FALSE'."
    )
    payload = {
        "model": "llama3.1-8b",
        "messages":[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Market: '{question}'"}
        ],
        "temperature": 0,
        "max_tokens": 10
    }

    try:
        async with session.post(url, json=payload, headers=headers, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                answer = data["choices"][0]["message"]["content"].strip().upper()
                return "TRUE" in answer
            elif resp.status == 429:
                log(f"[AI] Rate limited — waiting 60s")
                await asyncio.sleep(60)
                return None  # Rate limit: return None to retry later
            else:
                error_text = await resp.text()
                log(f"[AI] Error {resp.status}: {error_text[:100]}")
                return None  # API error: return None to retry later
    except Exception as e:
        log(f"[AI] Exception: {type(e).__name__}: {e}")
        return None  # Connection error: return None to retry later


# ── DISCOVERY LOOP ────────────────────────────────────────────────────────────

async def discovery_loop():
    log("Started. Scanning all active markets every 30 seconds.")

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                if _discovery_lock.locked():
                    log("[SKIP] Previous discovery run still in progress — skipping this cycle")
                    await asyncio.sleep(DISCOVERY_INTERVAL)
                    continue

                async with _discovery_lock:
                    await run_discovery(session)
            except Exception as e:
                crash_log("market_discovery discovery_loop", e, traceback.format_exc())
            await asyncio.sleep(DISCOVERY_INTERVAL)


async def run_discovery(session: aiohttp.ClientSession):
    """
    Priority-based market discovery:
    1. URGENT: Process markets from signals queue (where money already flowed)
    2. GENERAL: Scan new Polymarket markets (only if no pending signals)
    """
    conn = await get_async_db_connection()

    try:
        # ── 1. URGENT: PROCESS SIGNALS QUEUE FIRST ────────────────────────────
        # Check ONLY markets where signals already exist (real money flowed)
        cursor = await conn.execute("""
            SELECT DISTINCT slug, market_q
            FROM signals
            WHERE is_analyzed = 0 AND slug != ''
        """)
        pending = await cursor.fetchall()

        if pending:
            log(f"[URGENT] Processing {len(pending)} markets from signals queue")
            for i, (slug, question) in enumerate(pending):
                if i > 0:                          # first request has no delay
                    await asyncio.sleep(0.3)       # 0.3s between requests = ~200 req/min
                await process_market(session, conn, slug, question, is_urgent=True)
            return  # Exit early - don't waste API limits on general scan while queue exists

        # ── 2. GENERAL MARKET SCAN (only if no pending signals) ───────────────
        await general_market_scan(session, conn)

    finally:
        await conn.close()


async def process_market(session: aiohttp.ClientSession, conn, slug: str, question: str, is_urgent: bool = False):
    """
    Process a single market: check whitelist, run AI, update signals.
    """
    prefix = "[URGENT]" if is_urgent else "[GENERAL]"

    # Check if already in whitelist
    cursor = await conn.execute("SELECT is_approved FROM markets_whitelist WHERE slug = ?", (slug,))
    existing = await cursor.fetchone()

    if existing:
        # Already classified - just update signals table
        if existing['is_approved'] == 1:
            # Check casino keywords (override if needed)
            q_upper = question.upper()
            is_casino = any(kw in q_upper for kw in CASINO_KEYWORDS)
            new_status = 2 if is_casino else 1
        else:
            new_status = 2

        await conn.execute("UPDATE signals SET is_analyzed = ? WHERE slug = ? AND is_analyzed = 0", (new_status, slug))
        await conn.commit()
        log(f"{prefix} {slug[:40]} — already classified, signals updated")
        return

    # Fetch market from Gamma API
    try:
        async with session.get(f"{POLYMARKET_GAMMA_API}?slug={slug}", timeout=10) as resp:
            if resp.status == 200:
                markets_data = await resp.json()

                if not markets_data or len(markets_data) == 0:
                    # Dead slug
                    await conn.execute(
                        "INSERT OR REPLACE INTO markets_whitelist (slug, question, is_approved, checked_at) VALUES (?, ?, 0, ?)",
                        (slug, f"Dead slug: {slug}", int(time.time())),
                    )
                    await conn.execute("UPDATE signals SET is_analyzed = 2 WHERE slug = ? AND is_analyzed = 0", (slug,))
                    await conn.commit()
                    await log_ai_decision(slug, f"Dead slug: {slug}", "rejected", "Not Found")
                    log(f"{prefix} ❌ DEAD SLUG: {slug}")
                    return

                market = markets_data[0]
                question = market.get("question", question)
            else:
                log(f"{prefix} Gamma API error {resp.status} for {slug}")
                return
    except Exception as e:
        log(f"{prefix} Error fetching {slug}: {type(e).__name__}: {e}")
        return

    # Step 1: Fast keyword check
    if not fast_casino_check(question):
        await conn.execute(
            "INSERT OR REPLACE INTO markets_whitelist (slug, question, is_approved, checked_at) VALUES (?, ?, 0, ?)",
            (slug, question, int(time.time())),
        )
        await conn.execute("UPDATE signals SET is_analyzed = 2 WHERE slug = ? AND is_analyzed = 0", (slug,))
        await conn.commit()
        await log_ai_decision(slug, question, "rejected", "Keywords")
        log(f"{prefix} ❌ REJECTED (keywords): {question[:50]}")
        return

    # Step 2: AI check
    await asyncio.sleep(AI_REQUEST_DELAY)
    approved = await ai_check(session, question)

    # If AI returned None (error/rate limit), skip DB write and exit for retry later
    if approved is None:
        log(f"{prefix} ⚠️ AI API Error/RateLimit: Skipping {slug[:40]} for later retry")
        return

    # Only write to DB if we got a clear True/False from AI
    await conn.execute(
        "INSERT OR REPLACE INTO markets_whitelist (slug, question, is_approved, checked_at) VALUES (?, ?, ?, ?)",
        (slug, question, 1 if approved else 0, int(time.time())),
    )

    if approved:
        await conn.execute("UPDATE signals SET is_analyzed = 1 WHERE slug = ? AND is_analyzed = 0", (slug,))
    else:
        await conn.execute("UPDATE signals SET is_analyzed = 2 WHERE slug = ? AND is_analyzed = 0", (slug,))
    await conn.commit()

    await log_ai_decision(slug, question, "approved" if approved else "rejected", "AI")
    log(f"{prefix} {'✅' if approved else '❌'} {question[:50]}")


async def general_market_scan(session: aiohttp.ClientSession, conn):
    """
    Scan all active Polymarket markets (background task when no pending signals).
    """
    offset = 0
    limit = 500
    new_checked = 0
    new_approved = 0
    new_rejected = 0

    # Load existing slugs to avoid redundant checks
    cursor = await conn.execute("SELECT slug FROM markets_whitelist")
    existing = set(row[0] for row in await cursor.fetchall())
    
    while True:
        try:
            async with session.get(
                POLYMARKET_GAMMA_API,
                params={"active": "true", "closed": "false", "limit": limit, "offset": offset},
                timeout=30,
            ) as resp:
                if resp.status != 200:
                    log(f"Gamma API error {resp.status}")
                    break
                markets = await resp.json()
                if not markets:
                    break
        except Exception as e:
            log(f"Gamma API fetch error: {type(e).__name__}: {e}")
            break
        
        for market in markets:
            slug = market.get("slug", "")
            question = market.get("question", "")
            
            if not slug or not question:
                continue
            
            if slug in existing:
                continue
            
            # Step 1: Fast keyword check
            if not fast_casino_check(question):
                await conn.execute(
                    "INSERT OR REPLACE INTO markets_whitelist (slug, question, is_approved, checked_at) VALUES (?, ?, 0, ?)",
                    (slug, question, int(time.time())),
                )
                await conn.commit()
                existing.add(slug)
                new_checked += 1
                new_rejected += 1
                await log_ai_decision(slug, question, "rejected", "Keywords")
                continue

            # Step 2: AI check
            await asyncio.sleep(AI_REQUEST_DELAY)
            approved = await ai_check(session, question)

            # If AI returned None (error/rate limit), skip this market for later retry
            if approved is None:
                log(f"[GENERAL] ⚠️ AI API Error/RateLimit: Skipping {slug[:40]} for later retry")
                continue

            # Only write to DB if we got a clear True/False from AI
            await conn.execute(
                "INSERT OR REPLACE INTO markets_whitelist (slug, question, is_approved, checked_at) VALUES (?, ?, ?, ?)",
                (slug, question, 1 if approved else 0, int(time.time())),
            )
            await conn.commit()
            existing.add(slug)
            new_checked += 1
            if approved:
                new_approved += 1
            else:
                new_rejected += 1

            await log_ai_decision(slug, question, "approved" if approved else "rejected", "AI")
            log(f"[{'✅' if approved else '❌'}] {question[:60]}")
        
        offset += limit
        if len(markets) < limit:
            break
    
    if new_checked > 0:
        log(f"Scan complete: {new_checked} new markets checked. Approved: {new_approved}, Rejected: {new_rejected}")
    else:
        log(f"Scan complete: no new markets found.")


# ── MAIN ─────────────────────────────────────────────────────────────────────

async def main():
    # First things first — check the environment
    from config import check_required_env
    check_required_env()

    # STAGE 2: Schema migrations handled by standalone migrate.py
    # Run 'python migrate.py' ONCE before starting workers

    log("Market Discovery Worker started")
    await discovery_loop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("Stopped by user")
