
# --- PATH BOOTSTRAP ---
import sys as _sys, os as _os
_SRC_DIR = _os.path.dirname(_os.path.abspath(__file__))
_PROJECT_DIR = _os.path.dirname(_SRC_DIR)
for _p in [_SRC_DIR, _os.path.join(_PROJECT_DIR, 'config')]:
    if _p not in _sys.path:
        _sys.path.insert(0, _p)
del _sys, _os, _SRC_DIR, _PROJECT_DIR, _p
import aiohttp
import asyncio
import aiofiles
import os
import html
from typing import List, Dict, Optional, Any
from datetime import datetime, timezone

# Import from central config
from config import TG_TOKEN, TG_CHAT_ID, TG_INSIDER_CHAT_ID, format_wallet_short, calculate_confidence_score, get_confidence_badge, format_pnl, crash_log, crash_log_async

# ==========================================
# 🔄 Persistent Telegram HTTP session
# ==========================================
# Creating a new aiohttp.ClientSession per request is expensive (TCP + TLS handshake).
# During cluster events we send 5-10 alerts per second — use one persistent session.
_tg_session: Optional[aiohttp.ClientSession] = None


async def _get_tg_session() -> aiohttp.ClientSession:
    """Return the persistent Telegram session, creating it if needed."""
    global _tg_session
    if _tg_session is None or _tg_session.closed:
        connector = aiohttp.TCPConnector(
            limit=10,
            keepalive_timeout=30,
            enable_cleanup_closed=True,
        )
        _tg_session = aiohttp.ClientSession(connector=connector)
    return _tg_session


async def close_tg_session():
    """Call this on graceful shutdown."""
    global _tg_session
    if _tg_session and not _tg_session.closed:
        await _tg_session.close()
        _tg_session = None


# ==========================================
# 🔗 MARKET LINK HELPER
# ==========================================
def format_market_link(market_slug: str, event_slug: Optional[str] = None) -> str:
    """
    Generate Polymarket market link with proper format.
    
    Preferred format: https://polymarket.com/event/{event_slug}/{market_slug}
    Fallback format:  https://polymarket.com/event/{market_slug}
    
    Args:
        market_slug: The market slug (e.g., 'will-anthropic-have-the-1-ai-model...')
        event_slug: Optional parent event slug (e.g., 'which-company-has-the-1-ai-model...')
    
    Returns:
        Formatted Polymarket URL
    """
    if not market_slug:
        return "https://polymarket.com"
    
    # If event_slug provided, use extended format
    if event_slug:
        return f"https://polymarket.com/event/{event_slug}/{market_slug}"
    
    # Fallback to simple format
    return f"https://polymarket.com/event/{market_slug}"

# ==========================================
# 🛠️ EMERGENCY LOGGING - Telegram Errors
# ==========================================
TELEGRAM_ERROR_LOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "telegram_errors.log")

async def send_telegram(message: str, parse_mode: str = "HTML", disable_web_page_preview: bool = True, is_insider: bool = False) -> bool:
    """Send Telegram message with retry logic and error logging."""
    # Route to VIP group if is_insider=True and TG_INSIDER_CHAT_ID is set
    if is_insider and TG_INSIDER_CHAT_ID:
        chat_id = TG_INSIDER_CHAT_ID
        print(f"[ROUTING] Sending high-priority alert to VIP group")
    else:
        chat_id = TG_CHAT_ID

    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": parse_mode,
        "disable_web_page_preview": disable_web_page_preview
    }

    for attempt in range(3):
        try:
            session = await _get_tg_session()
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    return True
                elif resp.status == 429:
                    wait_time = 5 * (attempt + 1)
                    await asyncio.sleep(wait_time)
                else:
                    if attempt < 2:
                        await asyncio.sleep(2)
        except Exception as e:
            # ==========================================
            # 🚨 EMERGENCY LOGGING - Log exact error (async with aiofiles)
            # ==========================================
            error_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            error_msg = f"[{error_ts}] SEND FAILED | Error: {type(e).__name__}: {str(e)}\nMessage preview: {message[:200]}...\n{'='*50}\n"

            try:
                async with aiofiles.open(TELEGRAM_ERROR_LOG, "a", encoding="utf-8") as f:
                    await f.write(error_msg)
            except Exception as log_err:
                print(f"[CRITICAL] Failed to write to telegram_errors.log: {log_err}")
                # Fallback to crash_log_async if aiofiles fails
                await crash_log_async("telegram_error_log_write", log_err, "")

            if attempt < 2:
                await asyncio.sleep(2)

    return False


# ==========================================
# 🚨 CEX-COORDINATED ATTACK ALERT
# ==========================================

async def alert_cex_coordinated_attack(
    wallets: List[str],
    market_title: str,
    outcome: str,
    sizes: List[float],
    market_slug: str,
    timing_window_mins: float,
    avg_size: float,
    funding_source: str,
    is_instant_sync: bool = False,
    instant_sync_count: int = 0,
    wallet_details: List[Dict] = None,
    slippage_pct: float = None,
    # V13: Quality flags for alert display
    flag_low_activity: bool = False,
    flag_price_sync: bool = False,
    flag_time_sync: bool = False,
    # V14: New parameters for enhanced cluster analysis
    flag_size_uniformity: bool = False,
    weighted_avg_price: float = None,
    size_ratio: float = None,
    first_trade_ts: int = None,
    # TASK 2: New wallet tracking
    is_new_wallet: bool = True,
    known_wallets_count: int = 0,
    # TASK 3: Event slug for proper URL format
    event_slug: Optional[str] = None
):
    """
    Alert for Coordinated Sybil attack.
    Detected when 3+ wallets from same funder enter same market.

    REFACTORED:
    - Deduplicates wallets (shows unique count)
    - Smart labeling: INSTANT ATTACK vs BEHAVIORAL CLUSTER
    - Shows individual wallet stats (trades + size)
    - Slippage visibility for urgent entries
    
    V14 ENHANCEMENTS:
    - Avg Entry (Weighted): Weighted average entry price
    - Size Consistency: High/Low based on size_ratio
    - Current Lag: Time since first trade in cluster
    """
    market_link = format_market_link(market_slug, event_slug)
    
    # ==========================================
    # DEDUPLICATE WALLETS & AGGREGATE STATS
    # ==========================================
    unique_wallets = {}

    # TASK 2: Add trigger wallet (wallets[0]) to unique_wallets first
    # This ensures the trigger wallet is included in the count and list
    if wallets and len(wallets) > 0:
        trigger_addr = wallets[0]
        unique_wallets[trigger_addr] = {
            'trade_count': 0,
            'total_trades': 0,
            'total_size': 0.0,
            'entry_ts': 0
        }

    if wallet_details:
        # Use detailed wallet info if provided
        for w in wallet_details:
            # FIX: funding_tracer_test uses 'wallet_addr', not 'wallet'
            addr = w.get('wallet_addr') or w.get('wallet', '')
            if addr and addr not in unique_wallets:
                unique_wallets[addr] = {
                    'trade_count': 0,
                    'total_trades': w.get('total_trades', 0),  # Lifetime trades
                    'total_size': 0.0,
                    'entry_ts': w.get('entry_ts', 0)
                }
            if addr:
                # CRITICAL FIX: Use 'total_volume' key from funding_tracer_test siblings
                unique_wallets[addr]['trade_count'] += w.get('trade_count', 1)
                unique_wallets[addr]['total_size'] += w.get('total_volume', 0)
    else:
        # Fallback: aggregate from wallets/sizes lists
        for i, w in enumerate(wallets):
            if w not in unique_wallets:
                unique_wallets[w] = {'trade_count': 0, 'total_trades': 0, 'total_size': 0.0}
            unique_wallets[w]['trade_count'] += 1
            if i < len(sizes):
                # CRITICAL FIX: sizes should already be usd_size from DB
                # If usd_size is missing, fallback calculation should happen in caller
                unique_wallets[w]['total_size'] += sizes[i]

    unique_count = len(unique_wallets)
    total_trades = sum(w['trade_count'] for w in unique_wallets.values())
    total_size = sum(w['total_size'] for w in unique_wallets.values())

    # ==========================================
    # 🛑 KILL SWITCH: Zero Volume Cluster
    # ==========================================
    # A cluster with $0 volume is a FALSE POSITIVE - do not send alert
    if total_size == 0:
        print(f"[CLUSTER BLOCKED] Zero volume cluster! wallets={unique_count}, market={market_slug}")
        return  # DO NOT SEND ALERT

    # ==========================================
    # 🛑 STRICT INSTANT ATTACK CHECK
    # ==========================================
    # CRITICAL FIX: An alert should ONLY be called 🚨 INSTANT COORDINATED ATTACK if:
    # 1. The wallets share the SAME specific funding source (NOT a generic CEX hot wallet)
    # 2. AND they entered in the same block
    #
    # If the wallets are not proven to be related via funding_sources (same funder address),
    # NEVER use the ⚡ INSTANT tag. Label it as a generic market movement instead.
    
    # List of generic CEX/Service sources that should NOT trigger INSTANT alerts
    # Using SUBSTRING matching to catch "Relay (0x...)", "Binance Hot Wallet", etc.
    GENERIC_SOURCES = ["cex", "service", "bridge", "mixer", "whale", "unknown", "relay", "polymarket"]
    
    # Check if funding_source contains any generic source substring
    is_specific_funder = funding_source and not any(gen in funding_source.lower() for gen in GENERIC_SOURCES)
    
    if not is_specific_funder:
        # This is NOT a coordinated attack - wallets just happen to enter same block
        # Relabel as generic market movement
        alert_emoji = "📊"
        alert_title = "COORDINATED MARKET MOVEMENT"
        sync_badge = " [SAME BLOCK - UNRELATED]"
        sync_detail = ""
        footer = f"Cluster formed within {timing_window_mins:.1f} minutes (Relayer batching)"
        timing_display = f"{timing_window_mins:.1f} minutes"
        automation_badge = ""
        print(f"[INSTANT FIX] Skipped INSTANT tag for {market_title}: funding_source='{funding_source}' is generic (substring match)")
    # ==========================================
    # SMART LABELING BASED ON TIMING
    # ==========================================
    # CRITICAL FIX: Handle timing window < 0.1 as INSTANT
    elif timing_window_mins < 0.1:
        alert_emoji = "🚨"
        alert_title = "INSTANT COORDINATED ATTACK"
        sync_badge = " [⚡ INSTANT]"
        # FIX: Only show sync_detail if 2+ wallets at same second
        if is_instant_sync and instant_sync_count >= 2:
            sync_detail = f" ({instant_sync_count} wallets at exact same second)"
        else:
            sync_detail = ""
        footer = "Script automation suspected"
        timing_display = "⚡ INSTANT (Same-second sync)"

        # Check for scalping bot detection
        max_lifetime_trades = max((w.get('total_trades', 0) for w in unique_wallets.values()), default=0)
        if max_lifetime_trades > 500:
            automation_badge = " ⚠️ <b>SCALPING BOT DETECTED</b>"
        else:
            automation_badge = ""
    elif timing_window_mins < 15:
        if is_instant_sync:
            alert_emoji = "🚨"
            alert_title = "INSTANT COORDINATED ATTACK"
            sync_badge = " [⚡ SAME-SECOND SYNC]"
            # FIX: Only show sync_detail if 2+ wallets at same second
            if instant_sync_count >= 2:
                sync_detail = f" ({instant_sync_count} wallets at exact same second)"
            else:
                sync_detail = ""
            footer = "Script automation suspected"
            timing_display = f"{timing_window_mins:.1f} minutes"
            automation_badge = ""
        else:
            alert_emoji = "🚨"
            alert_title = "COORDINATED ATTACK"
            sync_badge = ""
            sync_detail = ""
            footer = f"Cluster formed within {timing_window_mins:.1f} minutes"
            timing_display = f"{timing_window_mins:.1f} minutes"
            automation_badge = ""
    else:
        alert_emoji = "🕸️"
        alert_title = "BEHAVIORAL CLUSTER"
        sync_badge = " [SLOW ACCUMULATION]"
        sync_detail = ""
        footer = f"Cluster formed over {timing_window_mins:.0f} minutes"
        timing_display = f"{timing_window_mins:.0f} minutes"
        automation_badge = ""
    
    # ==========================================
    # BUILD WALLET LIST (TOP 10 BY SIZE)
    # ==========================================
    sorted_wallets = sorted(
        unique_wallets.items(),
        key=lambda x: x[1]['total_size'],
        reverse=True
    )[:10]

    wallet_lines = []
    for i, (addr, stats) in enumerate(sorted_wallets, 1):
        addr_short = f"{addr[:6]}...{addr[-6:]}"
        profile_link = f"https://polymarket.com/profile/{addr}"
        arkham_link = f"https://intel.arkm.com/explorer/address/{addr}"

        # Display lifetime trades FIRST (truth mode)
        lifetime_trades = stats.get('total_trades', stats['trade_count'])

        wallet_lines.append(
            f"{i}. <a href='{profile_link}'>{addr_short}</a> | <a href='{arkham_link}'>Arkham</a>\n"
            f"   📊 <b>Trades: {lifetime_trades}</b> | Vol: ${stats['total_size']:,.0f}"
        )

    wallet_list = "\n".join(wallet_lines)

    # ==========================================
    # BUILD MESSAGE
    # ==========================================
    # Add block time for instant attacks
    block_time_line = ""
    if timing_window_mins < 0.1:
        try:
            block_time = datetime.now(timezone.utc).strftime("%H:%M:%S")
            block_time_line = f"⏰ <b>Block Time:</b> {block_time} UTC\n"
        except Exception:
            pass  # Block time display is optional

    # ==========================================
    # V14: ENHANCED CLUSTER METRICS
    # ==========================================
    # Avg Entry (Weighted)
    if weighted_avg_price and weighted_avg_price > 0:
        avg_entry_line = f"📊 <b>Avg Entry:</b> {weighted_avg_price:.3f} (Weighted)\n"
    else:
        avg_entry_line = ""

    # Size Consistency: High if ratio <= 2.5, Low if laddered
    if size_ratio is not None:
        if size_ratio <= 2.5:
            size_consistency = "🟢 High"
        elif size_ratio <= 3.0:
            size_consistency = "🟡 Medium"
        else:
            size_consistency = "🔴 Low (Ladder)"
        size_consistency_line = f"📏 <b>Size Consistency:</b> {size_consistency} (ratio: {size_ratio:.2f}x)\n"
    else:
        size_consistency_line = ""

    # TASK 3: Current Lag - Time since first trade in cluster (should be near 0-60 seconds for live mode)
    current_lag_line = ""
    if first_trade_ts and first_trade_ts > 0:
        try:
            first_trade_time = datetime.fromtimestamp(int(first_trade_ts), tz=timezone.utc)
            now = datetime.now(timezone.utc)
            lag_seconds = (now - first_trade_time).total_seconds()
            if lag_seconds < 60:
                lag_display = f"{lag_seconds:.0f} seconds"
            elif lag_seconds < 3600:
                lag_display = f"{lag_seconds / 60:.1f} minutes"
            else:
                lag_display = f"{lag_seconds / 3600:.1f} hours"
            current_lag_line = f"⏳ <b>Current Lag:</b> {lag_display}\n"
        except Exception:
            pass  # Current lag display is optional

    # ==========================================
    # TASK 3: SIMPLIFIED MESSAGE FORMAT
    # ==========================================
    # Focus on: THE TRIGGER, THE CONTEXT, THE TOTAL
    # Get the trigger wallet (the one that just entered)
    trigger_wallet = wallets[0] if wallets else ""
    trigger_short = f"{trigger_wallet[:6]}...{trigger_wallet[-6:]}" if trigger_wallet else "Unknown"
    trigger_profile = f"https://polymarket.com/profile/{trigger_wallet}" if trigger_wallet else "#"
    trigger_arkham = f"https://intel.arkm.com/explorer/address/{trigger_wallet}" if trigger_wallet else "#"

    # Build context line based on new wallet vs accumulation
    if is_new_wallet:
        context_line = f"🔔 <b>Trigger:</b> <a href='{trigger_profile}'>NEW wallet {trigger_short}</a> just entered!"
        context_context = f"\n📊 <b>Context:</b> This is the {unique_count}th wallet from this funder in the last 60 minutes"
    else:
        context_line = f"🔔 <b>Trigger:</b> <a href='{trigger_profile}'>{trigger_short}</a> added to position"
        context_context = f"\n📊 <b>Context:</b> Wallet #{known_wallets_count + 1} accumulating (cluster has {unique_count} wallets total)"

    # Build simplified message
    msg = (
        f"{alert_emoji} <b>{alert_title}{sync_badge}{automation_badge}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{context_line}{context_context}\n"
        f"💰 <b>Total Syndicate Volume:</b> ${total_size:,.0f}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"❓ <b>Market:</b> <a href='{market_link}'>{html.escape(market_title)}</a>\n"
        f"🎯 <b>Position:</b> {outcome}\n"
        f"{avg_entry_line}{size_consistency_line}{current_lag_line}"
        f"⏱ <b>Timing Window:</b> {timing_display}\n"
        f"🔗 <b>Funding Source:</b> {funding_source}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>📋 All Cluster Wallets ({unique_count}):</b>\n{wallet_list}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>{footer}</i>"
    )

    await send_telegram(msg)


# ==========================================
# 📊 PHASE 2: SINGLE TRADER ALERTS
# ==========================================

async def alert_instant_funding(
    wallet,
    mins_ago,
    source,
    market_title,
    outcome,
    size,
    price,
    market_slug: str = None,
    entry_time: int = 0,
    current_price: float = None,
    pnl: float = 0.0,
    trades: int = 0,
    dominance: float = 0.0,
    slippage_pct: float = None,
    is_insider: bool = False,  # TASK: Route to VIP group
    event_slug: Optional[str] = None  # TASK 3: Event slug for proper URL format
):
    """
    Phase 2: Alert for funding <10 mins before trade.
    Enhanced with full wallet details.
    """
    wallet_short = format_wallet_short(wallet)
    profile_link = f"https://polymarket.com/profile/{wallet}"
    arkham_link = f"https://intel.arkm.com/explorer/address/{wallet}"
    market_link = format_market_link(market_slug, event_slug) if market_slug else "#"

    # Format entry time
    if entry_time > 0:
        try:
            entry_time_str = datetime.fromtimestamp(entry_time).strftime("%H:%M:%S")
        except Exception:
            entry_time_str = "N/A"
    else:
        entry_time_str = "N/A"

    # Format entry price
    if price and price > 0:
        entry_price_str = f"{price:.3f}"
    else:
        entry_price_str = "N/A"

    # Format total predictions
    preds_str = str(trades) if trades > 0 else "0"

    # Build slippage line if significant
    # TASK 4: Only show slippage if > 1%, and 🔥 emoji only if > 3%
    slippage_line = ""
    if slippage_pct is not None and slippage_pct > 1.0:
        if slippage_pct > 3.0:
            slippage_line = f"\n📉 <b>Slippage:</b> {slippage_pct:.1f}% 🔥 (Urgent Entry)"
        else:
            slippage_line = f"\n📉 <b>Slippage:</b> {slippage_pct:.1f}%"

    msg = (
        f"⚡ <b>INSTANT FUNDING DETECTED</b>\n"
        f"⏱ <b>Time Gap:</b> {mins_ago} minutes\n"
        f"👤 <b>Wallet:</b> <code>{wallet}</code>\n"
        f"<a href='{profile_link}'>Profile</a> | <a href='{arkham_link}'>Arkham</a>\n"
        f"⏰ {entry_time_str} | Entry: {entry_price_str} | Preds: {preds_str}\n"
        f"💸 <b>Source:</b> {source}\n"
        f"📊 <b>Dominance:</b> {dominance:.1f}%{slippage_line}\n"
        f"❓ <b>Market:</b> <a href='{market_link}'>{html.escape(market_title)}</a>\n"
        f"🎯 <b>Position:</b> {outcome} @ {price:.3f}\n"
        f"💰 <b>Size:</b> ${size:,.0f}\n"
        f"<i>Copy wallet address to track</i>"
    )
    await send_telegram(msg, is_insider=is_insider)


async def alert_single_trader(
    wallet: str,
    wallet_status: str,
    market_title: str,
    outcome: str,
    entry_price: float,
    usd_size: float,
    dominance: float,
    funding_source: str,
    unrealized_pnl_usd: float,
    unrealized_pnl_pct: float,
    is_instant_funding: bool,
    market_slug: str,
    win_rate: float = 0.0,
    is_elite: bool = False,
    slippage_pct: float = None,
    predictions: int = 0,  # Polymarket predictions count
    is_insider: bool = False,  # TASK: Route to VIP group
    event_slug: Optional[str] = None  # TASK 3: Event slug for proper URL format
):
    """
    Phase 2: Comprehensive single trader alert with confidence score.
    """
    wallet_short = format_wallet_short(wallet)
    profile_link = f"https://polymarket.com/profile/{wallet}"
    market_link = format_market_link(market_slug, event_slug)
    arkham_link = f"https://intel.arkm.com/explorer/address/{wallet}"

    # PnL formatting using helper
    pnl_str = format_pnl(unrealized_pnl_usd, unrealized_pnl_pct)

    # Calculate confidence score
    confidence = calculate_confidence_score(
        is_instant_funding=is_instant_funding,
        is_elite=is_elite,
        win_rate=win_rate,
        pnl_pct=unrealized_pnl_pct
    )
    confidence_badge = get_confidence_badge(confidence)

    # Instant funding badge
    instant_badge = " ⚡ INSTANT" if is_instant_funding else ""

    # Build slippage line if significant
    # TASK 4: Only show slippage if > 1%, and 🔥 emoji only if > 3%
    slippage_line = ""
    if slippage_pct is not None and slippage_pct > 1.0:
        if slippage_pct > 3.0:
            slippage_line = f"\n📉 <b>Slippage:</b> {slippage_pct:.1f}% 🔥"
        else:
            slippage_line = f"\n📉 <b>Slippage:</b> {slippage_pct:.1f}%"

    # Build predictions line
    predictions_line = f"\n📋 <b>Polymarket Predictions:</b> {predictions}" if predictions > 0 else ""

    msg = (
        f"{wallet_status}{instant_badge} <b>ALERT</b>\n"
        f"👤 <b>Trader:</b> <a href='{profile_link}'>{wallet_short}</a>\n"
        f"💰 <b>Size:</b> ${usd_size:,.0f}\n"
        f"📊 <b>Dominance:</b> {dominance:.1f}%{slippage_line}{predictions_line}\n"
        f"📈 <b>Unrealized PnL:</b> {pnl_str}\n"
        f"❓ <b>Market:</b> {html.escape(market_title)}\n"
        f"🎯 <b>Position:</b> {outcome} @ {entry_price:.3f}\n"
        f"💸 <b>Funding:</b> {funding_source}\n"
        f"🔗 <a href='{arkham_link}'>Arkham</a> | <a href='{market_link}'>Polymarket</a>\n"
        f"<code>{wallet}</code>"
    )

    await send_telegram(msg, is_insider=is_insider)


# ==========================================
# 📋 PHASE 1-3: EXISTING ALERT TEMPLATES
# ==========================================
# NOTE: Cluster/Sybil alerts now use alert_cex_coordinated_attack (unified format)
# alert_alpha_sybil has been deprecated and removed

async def alert_fresh_capital(
    wallet,
    mins_ago,
    market_title,
    outcome,
    size,
    source=None,
    price=0.0,
    market_slug: str = None,
    entry_time: int = 0,
    current_price: float = None,
    pnl: float = 0.0,
    trades: int = 0,
    dominance: float = 0.0,
    slippage_pct: float = None,
    alert_header: str = "⚡ FRESH CAPITAL SNIPER",  # TASK 2: Dynamic alert header
    is_insider: bool = False,  # TASK: Route to VIP group
    event_slug: Optional[str] = None  # TASK 3: Event slug for proper URL format
):
    """Alert for fresh capital (wallet funded <1 hour ago). Enhanced with full details."""
    wallet_short = format_wallet_short(wallet)
    profile_link = f"https://polymarket.com/profile/{wallet}"
    arkham_link = f"https://intel.arkm.com/explorer/address/{wallet}"
    market_link = format_market_link(market_slug, event_slug) if market_slug else "#"
    source_text = f" via {source}" if source else ""

    # Format entry time
    if entry_time > 0:
        try:
            entry_time_str = datetime.fromtimestamp(entry_time).strftime("%H:%M:%S")
        except Exception:
            entry_time_str = "N/A"
    else:
        entry_time_str = "N/A"

    # Format entry price
    if price and price > 0:
        entry_price_str = f"{price:.3f}"
    else:
        entry_price_str = "N/A"

    # Format total predictions
    preds_str = str(trades) if trades > 0 else "0"

    # Build slippage line if significant
    # TASK 4: Only show slippage if > 1%, and 🔥 emoji only if > 3%
    slippage_line = ""
    if slippage_pct is not None and slippage_pct > 1.0:
        if slippage_pct > 3.0:
            slippage_line = f"\n📉 <b>Slippage:</b> {slippage_pct:.1f}% 🔥"
        else:
            slippage_line = f"\n📉 <b>Slippage:</b> {slippage_pct:.1f}%"

    msg = (
        f"{alert_header}\n"
        f"🏦 <b>Funded:</b> {mins_ago} mins ago{source_text}\n"
        f"👤 <b>Wallet:</b> <code>{wallet}</code>\n"
        f"<a href='{profile_link}'>Profile</a> | <a href='{arkham_link}'>Arkham</a>\n"
        f"⏰ {entry_time_str} | Entry: {entry_price_str} | Preds: {preds_str}\n"
        f"📊 <b>Dominance:</b> {dominance:.1f}%{slippage_line}\n"
        f"❓ <b>Market:</b> <a href='{market_link}'>{html.escape(market_title)}</a>\n"
        f"🎯 <b>ENTRY:</b> {outcome} @ {price:.3f}\n"
        f"💰 <b>Size:</b> ${size:,.0f}\n"
        f"<i>Copy wallet address to track</i>"
    )
    await send_telegram(msg, is_insider=is_insider)


async def alert_elite_move(
    wallet,
    win_rate,
    market_title,
    outcome,
    size,
    price=0.0,
    market_slug: str = None,
    entry_time: int = 0,
    current_price: float = None,
    pnl: float = 0.0,
    trades: int = 0,
    dominance: float = 0.0,
    slippage_pct: float = None,
    is_insider: bool = False,  # TASK: Route to VIP group
    event_slug: Optional[str] = None  # TASK 3: Event slug for proper URL format
):
    """Alert for high-performer trader move. Enhanced with full details."""
    wallet_short = format_wallet_short(wallet)
    profile_link = f"https://polymarket.com/profile/{wallet}"
    arkham_link = f"https://intel.arkm.com/explorer/address/{wallet}"
    market_link = format_market_link(market_slug, event_slug) if market_slug else "#"

    # Format entry time
    if entry_time > 0:
        try:
            entry_time_str = datetime.fromtimestamp(entry_time).strftime("%H:%M:%S")
        except Exception:
            entry_time_str = "N/A"
    else:
        entry_time_str = "N/A"

    # Format entry price
    if price and price > 0:
        entry_price_str = f"{price:.3f}"
    else:
        entry_price_str = "N/A"

    # Format total predictions
    preds_str = str(trades) if trades > 0 else "0"

    # Build slippage line if significant
    # TASK 4: Only show slippage if > 1%, and 🔥 emoji only if > 3%
    slippage_line = ""
    if slippage_pct is not None and slippage_pct > 1.0:
        if slippage_pct > 3.0:
            slippage_line = f"\n📉 <b>Slippage:</b> {slippage_pct:.1f}% 🔥"
        else:
            slippage_line = f"\n📉 <b>Slippage:</b> {slippage_pct:.1f}%"

    msg = (
        f"💎 <b>SMART MONEY MOVE</b>\n"
        f"👤 <b>Legend:</b> <code>{wallet}</code>\n"
        f"<a href='{profile_link}'>Profile</a> | <a href='{arkham_link}'>Arkham</a>\n"
        f"⏰ {entry_time_str} | Entry: {entry_price_str} | Preds: {preds_str}\n"
        f"🏆 <b>Winrate:</b> {win_rate:.0f}%\n"
        f"📊 <b>Dominance:</b> {dominance:.1f}%{slippage_line}\n"
        f"❓ <b>Market:</b> <a href='{market_link}'>{html.escape(market_title)}</a>\n"
        f"🎯 <b>BET:</b> {outcome} @ {price:.3f}\n"
        f"💰 <b>Size:</b> ${size:,.0f}\n"
        f"<i>Copy wallet address to track</i>"
    )
    await send_telegram(msg, is_insider=is_insider)


async def alert_mixer_detected(
    wallet,
    source,
    market,
    outcome,
    size,
    market_slug: str = None,
    entry_time: int = 0,
    price: float = 0.0,
    current_price: float = None,
    pnl: float = 0.0,
    trades: int = 0,
    dominance: float = 0.0,
    slippage_pct: float = None,
    is_insider: bool = False,  # TASK: Route to VIP group
    event_slug: Optional[str] = None  # TASK 3: Event slug for proper URL format
):
    """Alert for Tornado Cash / mixer usage. Enhanced with full details."""
    wallet_short = format_wallet_short(wallet)
    profile_link = f"https://polymarket.com/profile/{wallet}"
    arkham_link = f"https://intel.arkm.com/explorer/address/{wallet}"
    market_link = format_market_link(market_slug, event_slug) if market_slug else "#"

    # Format entry time
    if entry_time > 0:
        try:
            entry_time_str = datetime.fromtimestamp(entry_time).strftime("%H:%M:%S")
        except Exception:
            entry_time_str = "N/A"
    else:
        entry_time_str = "N/A"

    # Format entry price
    if price and price > 0:
        entry_price_str = f"{price:.3f}"
    else:
        entry_price_str = "N/A"

    # Format total predictions
    preds_str = str(trades) if trades > 0 else "0"

    # Build slippage line if significant
    # TASK 4: Only show slippage if > 1%, and 🔥 emoji only if > 3%
    slippage_line = ""
    if slippage_pct is not None and slippage_pct > 1.0:
        if slippage_pct > 3.0:
            slippage_line = f"\n📉 <b>Slippage:</b> {slippage_pct:.1f}% 🔥"
        else:
            slippage_line = f"\n📉 <b>Slippage:</b> {slippage_pct:.1f}%"

    msg = (
        f"🌪️ <b>DIRTY MONEY (MIXER)</b>\n"
        f"👤 <b>Wallet:</b> <code>{wallet}</code>\n"
        f"<a href='{profile_link}'>Profile</a> | <a href='{arkham_link}'>Arkham</a>\n"
        f"⏰ {entry_time_str} | Entry: {entry_price_str} | Preds: {preds_str}\n"
        f"💸 <b>Source:</b> {source}\n"
        f"📊 <b>Dominance:</b> {dominance:.1f}%{slippage_line}\n"
        f"💰 <b>Bet:</b> {outcome} (${size:,.0f})\n"
        f"❓ <b>Market:</b> <a href='{market_link}'>{html.escape(market)}</a>\n"
        f"<i>Copy wallet address to track</i>"
    )
    await send_telegram(msg, is_insider=is_insider)


# ==========================================
# ☢️ HIGH-CONFIDENCE ALERTS
# ==========================================

async def alert_nuclear_signal(
    wallet: str,
    market_title: str,
    outcome: str,
    price: float,
    size: float,
    confidence: Dict[str, Any],
    reasons: List[str],
    event_slug: Optional[str] = None  # TASK 3: Event slug for proper URL format
):
    """
    Special alert for NUCLEAR/CRITICAL confidence signals.
    Used when multiple signals combine (Sybil + Instant + Elite, etc.)
    """
    wallet_short = format_wallet_short(wallet)
    profile_link = f"https://polymarket.com/profile/{wallet}"
    arkham_link = f"https://app.arkhamintelligence.com/explorer/address/{wallet}"
    market_link = format_market_link(market_title.lower().replace(' ', '-').replace('?', ''), event_slug) if market_title else "#"

    reasons_str = "\n".join([f"• {r}" for r in reasons])

    msg = (
        f"☢️ <b>NUCLEAR SIGNAL DETECTED</b> ☢️\n"
        f"👤 <b>Wallet:</b> <a href='{profile_link}'>{wallet_short}</a>\n"
        f"💰 <b>Size:</b> ${size:,.0f}\n"
        f"❓ <b>Market:</b> {html.escape(market_title)}\n"
        f"🎯 <b>Position:</b> {outcome} @ {price:.3f}\n"
        f"🔍 <b>Signals:</b>\n{reasons_str}\n"
        f"🔗 <a href='{arkham_link}'>Arkham</a> | <a href='{market_link}'>Polymarket</a>\n"
        f"<code>{wallet}</code>\n"
        f"<i>⚠️ This is a high-confidence insider signal!</i>"
    )
    await send_telegram(msg)


async def alert_high_confidence_trade(
    wallet: str,
    market_title: str,
    outcome: str,
    entry_price: float,
    size: float,
    pnl_usd: float,
    pnl_pct: float,
    confidence: Dict[str, Any],
    reasons: List[str]
):
    """
    Alert for high confidence trades (score >= 50).
    """
    wallet_short = format_wallet_short(wallet)
    profile_link = f"https://polymarket.com/profile/{wallet}"
    arkham_link = f"https://app.arkhamintelligence.com/explorer/address/{wallet}"
    
    reasons_str = ", ".join(reasons)
    pnl_str = format_pnl(pnl_usd, pnl_pct)

    msg = (
        f"{confidence['emoji']} <b>HIGH CONFIDENCE TRADE</b>\n"
        f"📊 <b>Signals:</b> {reasons_str}\n"
        f"👤 <b>Trader:</b> <a href='{profile_link}'>{wallet_short}</a>\n"
        f"💰 <b>Size:</b> ${size:,.0f}\n"
        f"📈 <b>PnL:</b> {pnl_str}\n"
        f"❓ <b>Market:</b> {html.escape(market_title)}\n"
        f"🎯 <b>Entry:</b> {outcome} @ {entry_price:.3f}\n"
        f"🔗 <a href='{arkham_link}'>Arkham</a>\n"
        f"<code>{wallet}</code>"
    )
    await send_telegram(msg)


# ==========================================
# 📊 PHASE 4: DAILY SUMMARY ALERTS
# ==========================================

async def alert_daily_summary(
    date: str,
    total_alerts: int,
    top_insiders: List[Dict],
    sybil_clusters: int,
    fresh_capital_count: int,
    elite_moves: int
):
    """
    Phase 4: Daily summary of all insider activity.
    
    Args:
        date: Date string (e.g., "2026-02-18")
        total_alerts: Total number of alerts today
        top_insiders: List of top insider wallets with stats
        sybil_clusters: Number of Sybil clusters detected
        fresh_capital_count: Number of fresh capital alerts
        elite_moves: Number of elite trader moves
    """
    # Build top insiders list
    insiders_list = "\n".join([
        f"  {i+1}. <a href='https://polymarket.com/profile/{ins['wallet']}'>{format_wallet_short(ins['wallet'])}</a> | "
        f"PnL: {format_pnl(ins['pnl_usd'], ins['pnl_pct'])} | WR: {ins['win_rate']:.0f}%"
        for i, ins in enumerate(top_insiders[:10])
    ])
    
    msg = (
        f"📊 <b>DAILY INSIDER SUMMARY</b>\n"
        f"📅 <b>Date:</b> {date}\n"
        f"🔔 <b>Total Alerts:</b> {total_alerts}\n"
        f"🕸 <b>Sybil Clusters:</b> {sybil_clusters}\n"
        f"⚡ <b>Fresh Capital:</b> {fresh_capital_count}\n"
        f"💎 <b>Elite Moves:</b> {elite_moves}\n"
        f"🏆 <b>Top Insiders:</b>\n{insiders_list}\n"
        f"<i>Generated by Polymarket Insider Bot</i>"
    )
    await send_telegram(msg)


async def alert_weekly_report(
    week_start: str,
    week_end: str,
    total_alerts: int,
    total_sybil_clusters: int,
    total_pnl_tracked: float,
    top_performers: List[Dict]
):
    """
    Phase 4: Weekly report with aggregated statistics.
    """
    # Build top performers list
    performers_list = "\n".join([
        f"  {i+1}. <a href='https://polymarket.com/profile/{perf['wallet']}'>{format_wallet_short(perf['wallet'])}</a> | "
        f"PnL: {format_pnl(perf['pnl_usd'], perf['pnl_pct'])} | Trades: {perf['trades']}"
        for i, perf in enumerate(top_performers[:10])
    ])
    
    msg = (
        f"📈 <b>WEEKLY INSIDER REPORT</b>\n"
        f"📅 <b>Period:</b> {week_start} - {week_end}\n"
        f"🔔 <b>Total Alerts:</b> {total_alerts:,}\n"
        f"🕸 <b>Sybil Clusters:</b> {total_sybil_clusters}\n"
        f"💰 <b>Total PnL Tracked:</b> ${total_pnl_tracked:,.0f}\n"
        f"🏆 <b>Top Performers:</b>\n{performers_list}\n"
        f"<i>Generated by Polymarket Insider Bot</i>"
    )
    await send_telegram(msg)


async def send_paper_report(stats: dict, active_markets: int = 0, open_quotes: int = 0):
    """
    Send hourly paper trading session report to Telegram.
    Called by mm_worker.py every 3600 seconds when PAPER_MODE=True.

    Args:
        stats: dict from PaperTradingEngine.get_stats()
        active_markets: number of markets currently being MM'd
        open_quotes: number of currently open paper quotes
    """
    pnl = stats.get("total_pnl", 0.0)
    pnl_sign = "+" if pnl >= 0 else ""
    capital = stats.get("current_capital", 500.0)
    fills = stats.get("total_fills", 0)
    adverse = stats.get("adverse_fills", 0)
    adverse_rate = stats.get("adverse_rate_pct", 0.0)
    vs_target = stats.get("pnl_vs_target", -70.0)
    session_id = stats.get("session_id", "?")

    adverse_emoji = "✅" if adverse_rate < 8.0 else "⚠️"
    pnl_emoji = "📈" if pnl >= 0 else "📉"

    msg = (
        f"📊 <b>PAPER SESSION #{session_id}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{pnl_emoji} P&amp;L: <b>{pnl_sign}${pnl:.2f}</b> | Capital: <b>${capital:.2f}</b>\n"
        f"🎯 Target (+$70): <b>{'+' if vs_target >= 0 else ''}{vs_target:.2f}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📋 Fills: <b>{fills}</b> total | Adverse: <b>{adverse}</b> ({adverse_rate:.1f}%) {adverse_emoji}\n"
        f"🏪 Active markets: <b>{active_markets}</b> | Open quotes: <b>{open_quotes}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>Acceptance: P&amp;L &gt; +$70 AND adverse &lt; 8%</i>"
    )
    await send_telegram(msg)