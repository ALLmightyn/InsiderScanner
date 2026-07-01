#!/usr/bin/env python3
"""
hybrid_detector.py — Micro-Shield Bridge
=========================================
Coordinates between on-chain scanner (maintest.py) and market maker (mm_worker.py).
Listens to Redis channels published by maintest.py and uma_oracle_watcher.py.

Redis channels:
  'insider_signal'  — published by maintest.py when insider trade detected
  'uma_cancel'      — published by uma_oracle_watcher.py on ProposePrice event

This module is imported by mm_worker.py and run as a background asyncio task.
It is NOT a standalone worker — it runs inside the mm_worker process.

Integration point in maintest.py:
  After detecting an insider trade, add:
    await redis_client.publish('insider_signal', json.dumps({
        'wallet': trader_addr,
        'slug': slug,
        'confidence': 'HIGH'|'MEDIUM'|'LOW',
        'ts': int(time.time())
    }))
"""
# --- PATH BOOTSTRAP ---
import sys as _sys, os as _os
_SRC_DIR = _os.path.dirname(_os.path.abspath(__file__))
_PROJECT_DIR = _os.path.dirname(_SRC_DIR)
for _p in [_SRC_DIR, _os.path.join(_PROJECT_DIR, 'config')]:
    if _p not in _sys.path:
        _sys.path.insert(0, _p)
del _sys, _os, _SRC_DIR, _PROJECT_DIR, _p

# ⚠️  DEPRECATED: This module is NOT used in the current architecture.
# The MicroShield class inside mm_worker.py handles all Redis pub/sub directly.
# HybridDetector was an early design — kept for reference only.
# Do NOT import this module in production code.

import json
import time
import asyncio
import traceback
from datetime import datetime
from typing import Callable, List
from config import redis_client, crash_log, Colors
from alert_manager import send_telegram

def log(msg: str):
    print(f"{Colors.CYAN}[{datetime.now().strftime('%H:%M:%S')}] [HYBRID] {msg}{Colors.RESET}")


class HybridDetector:
    """
    Listens to Redis pub/sub channels and routes shield signals to mm_worker callbacks.
    """

    def __init__(self):
        self._shield_callbacks: List[Callable] = []     # list of async callables: f(slug, confidence, wallet)
        self._uma_callbacks: List[Callable] = []        # list of async callables: f(slug, question_id)
        self._running = False

    def on_shield(self, callback):
        """Register callback for insider_signal events. Callback: async f(slug, confidence, wallet)."""
        self._shield_callbacks.append(callback)

    def on_uma_cancel(self, callback):
        """Register callback for uma_cancel events. Callback: async f(slug, question_id)."""
        self._uma_callbacks.append(callback)

    async def run(self):
        """Start listening. Run as asyncio.create_task(detector.run())."""
        self._running = True
        pubsub = redis_client.pubsub()
        await pubsub.subscribe("insider_signal", "uma_cancel")
        log("Subscribed to Redis: insider_signal, uma_cancel")

        try:
            async for message in pubsub.listen():
                if not self._running:
                    break
                if message["type"] != "message":
                    continue

                channel = message.get("channel", "")
                try:
                    data = json.loads(message["data"])
                except (json.JSONDecodeError, TypeError):
                    continue

                if channel == "insider_signal":
                    await self._handle_insider_signal(data)
                elif channel == "uma_cancel":
                    await self._handle_uma_cancel(data)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            crash_log("hybrid_detector.run", e, traceback.format_exc())
        finally:
            await pubsub.unsubscribe()

    async def _handle_insider_signal(self, data: dict):
        """
        Route insider_signal to all registered shield callbacks.
        data: {'wallet': str, 'slug': str, 'confidence': str, 'ts': int}
        """
        slug = data.get("slug", "")
        confidence = data.get("confidence", "LOW")
        wallet = data.get("wallet", "")

        log(f"Insider signal: slug={slug}, confidence={confidence}, wallet={wallet[:10]}...")

        for cb in self._shield_callbacks:
            try:
                await cb(slug, confidence, wallet)
            except Exception as e:
                crash_log("hybrid_detector.shield_callback", e)

    async def _handle_uma_cancel(self, data: dict):
        """
        Route uma_cancel to all registered UMA callbacks.
        data: {'slug': str, 'question_id': str, 'ts': int}
        """
        slug = data.get("slug", "")
        question_id = data.get("question_id", "")

        log(f"UMA cancel signal: slug={slug}")

        for cb in self._uma_callbacks:
            try:
                await cb(slug, question_id)
            except Exception as e:
                crash_log("hybrid_detector.uma_callback", e)

    def stop(self):
        self._running = False
