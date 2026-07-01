#!/usr/bin/env python3
"""
uma_oracle_watcher.py — UMA Oracle Pre-Resolution Shield
=========================================================
Monitors ProposePrice events on Polygon from UmaCtfAdapter contract.
When detected: cancels MM quotes on that market BEFORE Polymarket.com updates.

Flow:
  eth_getLogs(UmaCtfAdapter, ProposePrice topic) every 30s
  → match questionId to active MM market via open_quotes.condition_id
  → write to uma_watcher_log
  → publish to Redis 'uma_cancel' channel
  → mm_worker.py receives and cancels quotes

PAPER_MODE: all actions are logged but no actual API calls to CLOB.
"""
# --- PATH BOOTSTRAP ---
import sys as _sys, os as _os
_SRC_DIR = _os.path.dirname(_os.path.abspath(__file__))
_PROJECT_DIR = _os.path.dirname(_SRC_DIR)
for _p in [_SRC_DIR, _os.path.join(_PROJECT_DIR, 'config')]:
    if _p not in _sys.path:
        _sys.path.insert(0, _p)
del _sys, _os, _SRC_DIR, _PROJECT_DIR, _p

import os
import asyncio
import aiohttp
import json
import time
import traceback
from datetime import datetime
from typing import Optional
from config import (
    get_async_db_connection, crash_log, crash_log_async,
    redis_client, Colors
)
from alert_manager import send_telegram

PAPER_MODE = os.getenv("PAPER_MODE", "true").lower() == "true"

# UMA CTF Adapter — used as requester address filter (NOT the contract we listen to)
UMA_CTF_ADAPTER = "0x6a9D222616C90FcA5754cd1333cFD9b7fb6a4F74"

# Optimistic Oracle V2 on Polygon — THIS is the contract that emits ProposePrice
# Source: UMA Protocol official deployment registry
OPTIMISTIC_ORACLE_V2 = "0xeE3Afe347D5C74317041E2618C49534dAf887c24"

# ProposePrice event topic — keccak256 of the CORRECT OO V2 event signature
# Signature: ProposePrice(address indexed requester, address indexed proposer,
#            bytes32 identifier, uint256 timestamp, bytes ancillaryData,
#            int256 proposedPrice, uint256 expirationTimestamp, address currency)
# keccak256("ProposePrice(address,address,bytes32,uint256,bytes,int256,uint256,address)")
PROPOSE_PRICE_TOPIC = "0x0e91f9f55f5af3b6b754f7ac20da80e0c6b8c7012bef26eadcd4c9de671e0e0b"

# topics[1] = requester (indexed) — filter to only UmaCtfAdapter requests
# This ensures we only catch Polymarket proposals, not other OO users
UMA_ADAPTER_TOPIC = "0x000000000000000000000000" + UMA_CTF_ADAPTER[2:].lower()

POLL_INTERVAL = 30  # seconds
RPC_LIST = [u.strip() for u in os.getenv("RPC_LIST", "").split(",") if u.strip()]
if not RPC_LIST:
    RPC_LIST = ["https://polygon-rpc.com", "https://rpc.ankr.com/polygon"]

_rpc_idx = 0

def log(msg: str):
    print(f"{Colors.MAGENTA}[{datetime.now().strftime('%H:%M:%S')}] [UMA] {msg}{Colors.RESET}")

def get_rpc() -> str:
    global _rpc_idx
    rpc = RPC_LIST[_rpc_idx % len(RPC_LIST)]
    _rpc_idx += 1
    return rpc


async def eth_get_logs(session: aiohttp.ClientSession, from_block: int, to_block: int) -> list:
    """
    Call eth_getLogs to get ProposePrice events from Optimistic Oracle V2.
    Filters by: requester = UmaCtfAdapter (topics[1]) to catch only Polymarket proposals.
    """
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getLogs",
        "params": [{
            "address": OPTIMISTIC_ORACLE_V2,
            "topics": [
                PROPOSE_PRICE_TOPIC,
                UMA_ADAPTER_TOPIC   # requester = UmaCtfAdapter (indexed, position 1)
            ],
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block)
        }]
    }
    try:
        async with session.post(get_rpc(), json=payload, timeout=15) as resp:
            if resp.status == 200:
                data = await resp.json()
                result = data.get("result", [])
                if isinstance(result, list):
                    return result
                if isinstance(data.get("error"), dict):
                    log(f"RPC error: {data['error']}")
    except Exception as e:
        crash_log("uma_watcher.eth_get_logs", e)
    return []


async def get_latest_block(session: aiohttp.ClientSession) -> int:
    """Get current Polygon block number."""
    payload = {"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []}
    try:
        async with session.post(get_rpc(), json=payload, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                return int(data.get("result", "0x0"), 16)
    except Exception as e:
        crash_log("uma_watcher.get_latest_block", e)
    return 0


async def match_condition_id(question_id: str) -> Optional[str]:
    """
    Try to find market_slug by matching question_id / condition_id in open_quotes table.
    question_id comes from ProposePrice log data (bytes32 hex string).
    Returns market_slug or None.
    """
    conn = await get_async_db_connection()
    try:
        # Try exact match on condition_id
        cursor = await conn.execute(
            "SELECT market_slug FROM open_quotes WHERE condition_id = ? LIMIT 1",
            (question_id,)
        )
        row = await cursor.fetchone()
        if row:
            return row["market_slug"]
        # Try partial match (condition_id may be prefixed differently)
        cursor = await conn.execute(
            "SELECT market_slug FROM open_quotes WHERE condition_id LIKE ? LIMIT 1",
            (f"%{question_id[-20:]}%",)
        )
        row = await cursor.fetchone()
        if row:
            return row["market_slug"]
    finally:
        await conn.close()
    return None


async def handle_propose_price(log_entry: dict, session: aiohttp.ClientSession):
    """
    Process a single ProposePrice log entry.
    Extracts questionId, finds market_slug, logs and cancels.

    ProposePrice event signature (OO V2):
      ProposePrice(address indexed requester, address indexed proposer,
                   bytes32 identifier, uint256 timestamp, bytes ancillaryData,
                   int256 proposedPrice, uint256 expirationTimestamp, address currency)

    Indexed params go to topics[]:
      topics[0] = event signature hash
      topics[1] = requester address (UmaCtfAdapter, padded to 32 bytes)
      topics[2] = proposer address (padded to 32 bytes)
    Non-indexed params go to data: identifier, timestamp, ancillaryData, proposedPrice, etc.
    """
    try:
        topics = log_entry.get("topics", [])
        if len(topics) < 3:
            log(f"ProposePrice log has only {len(topics)} topics — skipping")
            return

        # FIX-2: identifier is NON-INDEXED → in data field, first 32 bytes
        raw_data = log_entry.get("data", "0x")
        if len(raw_data) < 66:
            log(f"ProposePrice data too short ({len(raw_data)} chars) — skipping")
            return
        question_id = "0x" + raw_data[2:66]  # bytes 0-31 = identifier (bytes32)

        # proposer is topics[2] (indexed, padded 32 bytes → take last 40 hex = 20 bytes)
        proposer = "0x" + topics[2][-40:] if len(topics) >= 3 and len(topics[2]) >= 40 else ""

        block_number = int(log_entry.get("blockNumber", "0x0"), 16)
        tx_hash = log_entry.get("transactionHash", "")

        log(f"ProposePrice detected! questionId={question_id[:20]}... block={block_number}")

        # Try to find matching market
        market_slug = await match_condition_id(question_id)

        # Log to uma_watcher_log regardless of match
        conn = await get_async_db_connection()
        try:
            await conn.execute(
                """INSERT INTO uma_watcher_log
                   (condition_id, question_id, proposer_addr, proposed_price,
                    detected_at, action_taken, market_slug)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (question_id, question_id, proposer, None,
                 int(time.time()),
                 "cancel_requested" if market_slug else "no_match",
                 market_slug)
            )
            await conn.commit()
        finally:
            await conn.close()

        if market_slug:
            # Publish to Redis — mm_worker.py listens and cancels quotes
            await redis_client.publish("uma_cancel", json.dumps({
                "slug": market_slug,
                "question_id": question_id,
                "ts": int(time.time())
            }))
            log(f"Published uma_cancel for market: {market_slug}")

            # Telegram alert
            mode = "PAPER" if PAPER_MODE else "LIVE"
            await send_telegram(
                f"⚠️ <b>UMA ProposePrice [{mode}]</b>\n"
                f"Market: <code>{market_slug}</code>\n"
                f"Proposer: <code>{proposer[:20]}...</code>\n"
                f"Block: {block_number}\n"
                f"Quotes cancelled. 2h challenge window begins."
            )
        else:
            log(f"ProposePrice: no matching market found for questionId={question_id[:20]}...")

    except Exception as e:
        crash_log("uma_watcher.handle_propose_price", e, traceback.format_exc())


async def watcher_loop():
    """Main polling loop."""
    log(f"UMA Oracle Watcher started. Contract: {UMA_CTF_ADAPTER}")
    log(f"Using RPCs: {RPC_LIST}")
    log(f"PAPER_MODE: {PAPER_MODE}")

    async with aiohttp.ClientSession() as session:
        # Get starting block (current - 100 for safety margin)
        latest = await get_latest_block(session)
        if latest == 0:
            log("ERROR: Cannot get latest block. Check RPC connection.")
            return

        last_checked_block = latest - 100
        log(f"Starting from block: {last_checked_block}")

        while True:
            try:
                latest = await get_latest_block(session)
                if latest <= last_checked_block:
                    await asyncio.sleep(POLL_INTERVAL)
                    continue

                # Limit to 1000 blocks per poll to avoid RPC timeouts
                to_block = min(latest, last_checked_block + 1000)

                logs = await eth_get_logs(session, last_checked_block + 1, to_block)

                if logs:
                    log(f"Found {len(logs)} ProposePrice event(s) in blocks {last_checked_block+1}–{to_block}")
                    for log_entry in logs:
                        await handle_propose_price(log_entry, session)
                else:
                    # Periodic heartbeat every 10 minutes
                    if int(time.time()) % 600 < POLL_INTERVAL:
                        log(f"Heartbeat: checked blocks {last_checked_block}–{to_block}, no events")

                last_checked_block = to_block

            except Exception as e:
                crash_log("uma_watcher.loop", e, traceback.format_exc())

            await asyncio.sleep(POLL_INTERVAL)


async def main():
    from config import check_required_env
    check_required_env()
    await watcher_loop()


if __name__ == "__main__":
    import sys
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("Stopped by user")
