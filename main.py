# nifty_options_heikin_trader_final_v6_master.py
# Run in Pydroid3. Requires: pip install pandas growwapi pytz

import time
import json
import os
import csv
import signal
import logging
from datetime import datetime, timedelta
import pandas as pd
from growwapi import GrowwAPI
import pytz

# ========= USER CONFIG =========
API_AUTH_TOKEN = "eyJraWQiOiJaTUtjVXciLCJhbGciOiJFUzI1NiJ9.eyJleHAiOjE3NjA2NjEwMDAsImlhdCI6MTc2MDU4ODk2NywibmJmIjoxNzYwNTg4OTY3LCJzdWIiOiJ7XCJ0b2tlblJlZklkXCI6XCJlMWFjNjM4Zi01N2Y0LTQ5NjYtYWJkZC1iYzM4MmRiOWI5YjZcIixcInZlbmRvckludGVncmF0aW9uS2V5XCI6XCJlMzFmZjIzYjA4NmI0MDZjODg3NGIyZjZkODQ5NTMxM1wiLFwidXNlckFjY291bnRJZFwiOlwiZjk0MDVhODctNjdmMy00NzJjLThiZWEtMTNkOTJjZjc3NDZhXCIsXCJkZXZpY2VJZFwiOlwiYjcyODAxOWQtMTUwOS00ZjFlLWFjNDItYWE1ODE5YWMxMWRkXCIsXCJzZXNzaW9uSWRcIjpcIjczMGE3MDEzLTZkN2YtNGM3Mi04MWVkLTRiNjc4ZDQxNzgxNlwiLFwiYWRkaXRpb25hbERhdGFcIjpcIno1NC9NZzltdjE2WXdmb0gvS0EwYkFBcnZtS25Wb3hvZjkyZ1pwNVNZU1ZSTkczdTlLa2pWZDNoWjU1ZStNZERhWXBOVi9UOUxIRmtQejFFQisybTdRPT1cIixcInJvbGVcIjpcIm9yZGVyLWJhc2ljLGxpdmVfZGF0YS1iYXNpYyxub25fdHJhZGluZy1iYXNpYyxvcmRlcl9yZWFkX29ubHktYmFzaWMsYmFja190ZXN0XCIsXCJzb3VyY2VJcEFkZHJlc3NcIjpcIjI0MDI6M2E4MDo2ODU6YTQ4MjozMDEwOmJmZjpmZTkwOjNkODAsMTcyLjY5Ljg2LjE4NywzNS4yNDEuMjMuMTIzXCIsXCJ0d29GYUV4cGlyeVRzXCI6MTc2MDY2MTAwMDAwMH0iLCJpc3MiOiJhcGV4LWF1dGgtcHJvZC1hcHAifQ.eW6hEMOG5xPK41h2DnpgY2bvLO-P1zFRQr15vybf8KYEU6zw8Ycupq1IF5eh8cX-vFYtqMjSKRo2wRDTmQpDgg"
DRY_RUN = False
POLL_INTERVAL = 120
POSITION_FILE = "positions_state.json"
PRICE_THRESHOLD = 60
STRIKE_INTERVAL = 50
STRIKE_COUNT = 10
TOP_N = 2
EXPIRY_DATE = "25O20"
CANDLE_INTERVAL_MIN = 10   # 🔹 user-defined candle interval (e.g., 15 or 30)


TRADES_LOG_CSV = "trades_log.csv"
LOG_FILE = "trader.log"

# market hours & square-off time (IST)
MARKET_OPEN = (9, 15)
MARKET_CLOSE = (15, 30)
SQUARE_OFF_TIME = (15, 18)
IST = pytz.timezone("Asia/Kolkata")

# ========= LOGGING =========
logger = logging.getLogger("trader")
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(LOG_FILE)
fh.setLevel(logging.DEBUG)
fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
fh.setFormatter(fmt)
logger.addHandler(fh)
ch = logging.StreamHandler()
ch.setFormatter(fmt)
logger.addHandler(ch)

# ========= INIT API =========
logger.info("🔑 Initializing Groww API...")
try:
    groww = GrowwAPI(API_AUTH_TOKEN)
    logger.info(f"✅ Groww API init done. DRY_RUN = {DRY_RUN}")
except Exception as e:
    groww = None
    logger.warning("⚠️ Groww init failed (continuing in DRY_RUN or offline): %s", e)

# ========= STATE MANAGEMENT =========
def load_state():
    if os.path.exists(POSITION_FILE):
        try:
            with open(POSITION_FILE, "r") as f:
                s = json.load(f)
                if "instruments" not in s:
                    s = {"instruments": {}}
                return s
        except Exception as e:
            logger.warning("Failed to load state file, starting fresh: %s", e)
            return {"instruments": {}}
    return {"instruments": {}}

def save_state(state):
    try:
        with open(POSITION_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logger.error("⚠️ Failed to save state: %s", e)

# initialize trades csv if missing
def init_trades_csv():
    if not os.path.exists(TRADES_LOG_CSV):
        with open(TRADES_LOG_CSV, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp","symbol","side","entry_price","exit_price","qty","pnl_value","pnl_pct","entry_time","exit_time","exit_reason"])

# append trade
def log_trade(symbol, side, entry_price, exit_price, qty, entry_time, exit_time, exit_reason):
    pnl_value = None
    pnl_pct = None
    try:
        pnl_value = (exit_price - entry_price) * qty
        pnl_pct = ((exit_price - entry_price) / entry_price) * 100 if entry_price != 0 else None
    except Exception:
        pass
    with open(TRADES_LOG_CSV, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
            symbol,
            side,
            entry_price,
            exit_price,
            qty,
            round(pnl_value, 4) if pnl_value is not None else "",
            round(pnl_pct, 4) if pnl_pct is not None else "",
            entry_time or "",
            exit_time or "",
            exit_reason or ""
        ])

# ========= UTILS =========
def now_ist():
    return datetime.now(IST)

def is_market_open(now):
    open_time = now.replace(hour=MARKET_OPEN[0], minute=MARKET_OPEN[1], second=0, microsecond=0)
    close_time = now.replace(hour=MARKET_CLOSE[0], minute=MARKET_CLOSE[1], second=0, microsecond=0)
    return open_time <= now <= close_time

def is_square_off_time(now):
    so = now.replace(hour=SQUARE_OFF_TIME[0], minute=SQUARE_OFF_TIME[1], second=0, microsecond=0)
    return now >= so and now < now.replace(hour=MARKET_CLOSE[0], minute=MARKET_CLOSE[1], second=0, microsecond=0)

# handle graceful shutdown
running = True
def shutdown(signum, frame):
    global running
    logger.info("Received shutdown signal. Saving state and exiting...")
    running = False

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# ========= HEIKIN ASHI =========
def heikin_ashi(df):
    ha = pd.DataFrame(index=df.index, columns=["o","h","l","c"])
    ha["c"] = (df["o"] + df["h"] + df["l"] + df["c"]) / 4
    ha_open = [(df["o"].iloc[0] + df["c"].iloc[0]) / 2]
    for i in range(1, len(df)):
        ha_open.append((ha_open[i-1] + ha["c"].iloc[i-1]) / 2)
    ha["o"] = ha_open
    ha["h"] = df[["h","o","c"]].max(axis=1)
    ha["l"] = df[["l","o","c"]].min(axis=1)
    return ha

# ========= HISTORICAL OHLC =========
def _extract_possible_list_from_dict(d):
    for k in ("candles","data","ohlc","candlesList"):
        if k in d and isinstance(d[k], (list, tuple)):
            return d[k]
    for v in d.values():
        if isinstance(v,(list,tuple)):
            return v
    return None

def get_historical_ohlc(symbol, minutes=10, days=1, include_previous_day=False):
    end_time_dt = now_ist()
    end_time = int(end_time_dt.timestamp() * 1000)
    
    start_time_dt = end_time_dt - timedelta(days=days) # Default behavior
    
    if include_previous_day:
        # Calculate start time to include previous market day's close for initial candles
        # We need at least 3 candles.
        # If interval is 10 mins, 3 candles = 30 mins.
        # Market close is 15:30. To get 2 candles from previous day (e.g., 15:10, 15:20)
        # and 1 from current day (e.g., 9:15), we need to go back at least to previous day's 15:10.
        # To be safe, let's target fetching from the beginning of the previous market day or slightly before.
        
        # Go back to start of current day
        start_time_dt = end_time_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # If current time is before 9:30 AM (approx, for 9:25 AM trade after 10 min candle)
        # and we need previous day's candles
        if end_time_dt.time() < datetime(2000,1,1,9,30).time(): # Check against a fixed time
            # Go back to the previous calendar day
            prev_day_dt = start_time_dt - timedelta(days=1)
            # Fetch from previous day's market open to ensure we have enough data
            start_time_dt = prev_day_dt.replace(hour=MARKET_OPEN[0], minute=MARKET_OPEN[1], second=0, microsecond=0)
        else:
            # If it's already past 9:30, we can just fetch 'days' worth back from now
            start_time_dt = end_time_dt - timedelta(days=days)

    start_time = int(start_time_dt.timestamp() * 1000)

    try:
        if groww is None:
            raise Exception("Groww not initialized")
        resp = groww.get_historical_candle_data(
            trading_symbol=symbol,
            exchange=groww.EXCHANGE_NSE,
            segment=groww.SEGMENT_FNO,
            start_time=start_time,
            end_time=end_time,
            interval_in_minutes=minutes
        )
    except Exception as e:
        logger.warning("❌ OHLC fetch failed for %s: %s", symbol, e)
        return None

    if isinstance(resp, str):
        try:
            resp = json.loads(resp)
        except Exception:
            pass

    if isinstance(resp, dict):
        tmp = _extract_possible_list_from_dict(resp)
        if tmp is not None:
            resp = tmp

    candles = []
    for c in resp:
        try:
            if isinstance(c, dict):
                t = c.get("time") or c.get("t") or c.get("timestamp")
                o = c.get("open") or c.get("o")
                h = c.get("high") or c.get("h")
                l = c.get("low") or c.get("l")
                close = c.get("close") or c.get("c")
            elif isinstance(c, (list, tuple)):
                t, o, h, l, close = c[:5]
            else:
                continue
            t_val = int(float(t))
            if t_val < 1e11:
                t_val *= 1000
            candles.append({"o": float(o), "h": float(h), "l": float(l), "c": float(close), "t": t_val})
        except Exception:
            continue

    if not candles:
        return None
    df = pd.DataFrame(candles).sort_values("t").reset_index(drop=True)
    return df

# ========= POC (Median) =========
def calculate_poc(df):
    if df is None or len(df)==0:
        return None
    return float(df['c'].median())

# ========= ORDER HANDLER =========
def place_order(symbol, quantity, side):
    """Place market order via Groww (or dry-run). Return True on 'placed', else False."""
    if DRY_RUN:
        logger.info("[DRY RUN] Would place %s %s on %s", side, quantity, symbol)
        return {"status":"dry_run","symbol":symbol,"qty":quantity,"side":side}
    try:
        resp = groww.place_order(
            trading_symbol=symbol,
            quantity=quantity,
            validity=groww.VALIDITY_DAY,
            exchange=groww.EXCHANGE_NSE,
            segment=groww.SEGMENT_FNO,
            product=groww.PRODUCT_NRML,
            order_type=groww.ORDER_TYPE_MARKET,
            transaction_type=groww.TRANSACTION_TYPE_BUY if side=="BUY" else groww.TRANSACTION_TYPE_SELL
        )
        logger.info("[%s] Order response: %s", symbol, resp)
        return resp
    except Exception as e:
        logger.error("[%s] Order failed: %s", symbol, e)
        return None

def get_quantity_for_price(price):
    try:
        price = float(price)
    except Exception:
        return 0
    if 40 <= price <= 60:
        return 150
    elif 20 <= price < 40:
        return 300
    elif price < 20:
        return 450
    else:
        return 0

# ========= GROWW POSITION CONFIRMATION =========
def fetch_groww_positions():
    """Return dict: {symbol: {'qty': int, 'side': 'LONG' or 'SHORT'}}"""
    try:
        if groww is None:
            return {}
        resp = groww.get_positions_for_user(segment=groww.SEGMENT_FNO, timeout=5)
        if isinstance(resp, str):
            try:
                resp = json.loads(resp)
            except Exception:
                pass
        positions = {}
        candidates = []
        if isinstance(resp, dict):
            candidates = _extract_possible_list_from_dict(resp) or []
        elif isinstance(resp, list):
            candidates = resp
        for p in candidates:
            try:
                sym = p.get('tradingSymbol') or p.get('symbol') or p.get('trading_symbol')
                net_qty = p.get('netQuantity') or p.get('netQty') or p.get('net_qty') or p.get('quantity') or p.get('qty')
                if net_qty is None:
                    net_qty = p.get('quantity') or 0
                net_qty = int(float(net_qty))
                if sym and net_qty != 0:
                    positions[sym] = {'qty': abs(net_qty), 'side': 'LONG' if net_qty>0 else 'SHORT'}
            except Exception:
                continue
        return positions
    except Exception as e:
        logger.warning('⚠️ Failed to fetch Groww positions: %s', e)
        return {}

# ========= INSTRUMENT SELECTION =========
def fetch_top_options():
    try:
        if groww is None:
            raise Exception("Groww not initialized")
        q = groww.get_quote("NIFTY", exchange=groww.EXCHANGE_NSE, segment=groww.SEGMENT_CASH)
        nifty_value = q.get("last_price") or q.get("ltp")
        if nifty_value is None:
            logger.warning("❌ Could not read NIFTY value from quote")
            return []
    except Exception as e:
        logger.warning("❌ Failed to fetch NIFTY quote: %s", e)
        return []

    atm_strike = round(nifty_value / STRIKE_INTERVAL) * STRIKE_INTERVAL
    ce_symbols = [f"NIFTY{EXPIRY_DATE}{atm_strike + i*STRIKE_INTERVAL}CE" for i in range(-STRIKE_COUNT, STRIKE_COUNT+1)]
    pe_symbols = [f"NIFTY{EXPIRY_DATE}{atm_strike + i*STRIKE_INTERVAL}PE" for i in range(-STRIKE_COUNT, STRIKE_COUNT+1)]
    all_syms = [{"symbol":s,"type":"CE"} for s in ce_symbols] + [{"symbol":s,"type":"PE"} for s in pe_symbols]

    results = []
    logger.info("ℹ️ Fetching quotes for CE and PE symbols...")
    for item in all_syms:
        symbol = item["symbol"]
        try:
            quote = groww.get_quote(symbol, exchange=groww.EXCHANGE_NSE, segment=groww.SEGMENT_FNO)
            ltp = quote.get("last_price") or quote.get("ltp") or 0
            if ltp >= PRICE_THRESHOLD:
                continue
            results.append({"symbol": symbol, "type": item["type"], "ltp": float(ltp)})
        except Exception:
            continue

    ce_opts = sorted([r for r in results if r['type']=="CE"], key=lambda x: x['ltp'], reverse=True)[:TOP_N]
    pe_opts = sorted([r for r in results if r['type']=="PE"], key=lambda x: x['ltp'], reverse=True)[:TOP_N]

    final_syms = ce_opts + pe_opts
    logger.info("📈 Instruments fetched from Groww API this cycle: %s", [i['symbol'] for i in final_syms])
    return final_syms

# ========= SQUARE-OFF =========
def square_off_all(state):
    logger.info("⏹️ Square-off: exiting all open positions now...")
    groww_pos = fetch_groww_positions()
    for symbol, info in list(state['instruments'].items()):
        if info.get('position') == "LONG":
            remaining = info.get('remaining_qty') or info.get('last_quantity') or 0
            if remaining > 0:
                resp = place_order(symbol, remaining, "SELL")
                time.sleep(2)
                groww_pos = fetch_groww_positions()
                if symbol in groww_pos:
                    logger.warning("[%s] ❌ Square-off not confirmed, retrying SELL", symbol)
                    place_order(symbol, remaining, "SELL")
                    time.sleep(2)
                    groww_pos = fetch_groww_positions()
                    if symbol in groww_pos:
                        logger.error("[%s] ❌ After retries still present on Groww — manual check required", symbol)
                # log trade (if we have entry info)
                entry_price = info.get('entry_price')
                entry_time = info.get('entry_time')
                exit_price = None
                # try to get exit price from latest market quote (best-effort)
                try:
                    q = groww.get_quote(symbol, exchange=groww.EXCHANGE_NSE, segment=groww.SEGMENT_FNO)
                    exit_price = float(q.get('last_price') or q.get('ltp') or 0)
                except Exception:
                    exit_price = None
                if entry_price and exit_price is not None:
                    log_trade(symbol, "LONG", entry_price, exit_price, remaining, entry_time, datetime.now(IST).strftime("%Y-%m-%d %Y-%m-%d %H:%M:%S"), "SQUARE_OFF")
            info['position'] = None
            info['entry_price'] = None
            info['entry_time'] = None
            info['remaining_qty'] = None
            info['partial_booked'] = False
            info['last_exit_time'] = time.time()
    save_state(state)

# ========= TRADING LOGIC =========
def analyze_and_trade(symbol, state):
    inst = state['instruments'].setdefault(symbol, {
        'position': None,
        'entry_price': None,
        'entry_time': None,
        'last_quantity': None,
        'remaining_qty': None,
        'partial_booked': False,
        'last_exit_time': None
    })

    # MODIFIED: Fetch historical OHLC including previous day's data
    df10 = get_historical_ohlc(symbol, minutes=CANDLE_INTERVAL_MIN, days=2, include_previous_day=True) 
    
    if df10 is None or len(df10) < 3:
        logger.debug("[%s] ⚠️ Not enough %d-min data (need 3), skipping", symbol, CANDLE_INTERVAL_MIN)
        return

    ha10 = heikin_ashi(df10)
    if len(ha10) < 2:
        logger.debug("[%s] ⚠️ Not enough HA candles, skipping", symbol)
        return

    prev_ha = ha10.iloc[-2]
    is_prev_ha_red = prev_ha['c'] < prev_ha['o']
    is_prev_ha_green = prev_ha['c'] > prev_ha['o']

    prev_norm = df10.iloc[-2]
    is_prev_norm_red = prev_norm['c'] < prev_norm['o']  # existing exit criteria (red normal candle)
    # 🌟 NEW ENTRY CRITERIA: Previous normal candle must be green
    is_prev_norm_green = prev_norm['c'] > prev_norm['o']


    prev_high = prev_ha['h']
    seventy_pct_level = 0.7 * prev_high # This variable is kept, but its usage in exit logic is removed as per request.
    current_price = float(df10['c'].iloc[-1])

    now = now_ist()
    
    # Adjusting entry_allowed_time to reflect earliest possible trade execution time.
    first_candle_close_time = (datetime(2000,1,1,MARKET_OPEN[0],MARKET_OPEN[1]) + timedelta(minutes=CANDLE_INTERVAL_MIN)).time()
    entry_allowed_time = now.replace(hour=first_candle_close_time.hour, minute=first_candle_close_time.minute, second=0, microsecond=0)

    # 1) NEW: No new buy orders after 12:05 PM IST
    no_new_buy_after_time = now.replace(hour=12, minute=5, second=0, microsecond=0)


    df_last_day = get_historical_ohlc(symbol, minutes=CANDLE_INTERVAL_MIN, days=2, include_previous_day=True) # Fetch for POC as well
    poc_price = calculate_poc(df_last_day)

    cooldown_passed = True
    last_exit = inst.get('last_exit_time')
    if last_exit and (time.time() - last_exit) / 60.0 < 15.0:
        cooldown_passed = False

    logger.debug("[%s] DEBUG -> Current Price: %s, POC: %s, HA Red: %s, Norm Red: %s, 70%% PrevHigh: %.2f, Cooldown Passed: %s, Prev HA Green: %s, Prev Norm Green: %s",
                 symbol, current_price, poc_price, is_prev_ha_red, is_prev_norm_red, seventy_pct_level, cooldown_passed, is_prev_ha_green, is_prev_norm_green)

    # ===== EXIT LOGIC =====
    if inst.get('position') == "LONG":
        qty = inst.get('remaining_qty') or inst.get('last_quantity') or 0
        exit_condition = False
        exit_reason = None

        if is_prev_ha_red:
            exit_condition = True
            exit_reason = "HA_RED"
        # 2) REMOVED: elif is_prev_norm_red: - removed as per request
        # 3) REMOVED: elif inst.get('entry_price') and current_price >= 1.8 * inst['entry_price']: - removed as per request
        # 3) REMOVED: elif current_price <= seventy_pct_level: - removed as per request
        # 4) NEW: Add exit criteria if price reaches below 30% of entry price
        elif inst.get('entry_price') and current_price < 0.3 * inst['entry_price']:
            exit_condition = True
            exit_reason = "PRICE_BELOW_30PCT_ENTRY"

        if exit_condition and qty > 0:
            logger.info("[%s] 🔴 Exit triggered (%s). Attempting SELL qty=%s", symbol, exit_reason, qty)
            resp = place_order(symbol, qty, "SELL")
            time.sleep(2)
            groww_pos = fetch_groww_positions()
            if symbol in groww_pos:
                logger.warning("[%s] ❌ Exit not confirmed in Groww, retrying SELL", symbol)
                place_order(symbol, qty, "SELL")
                time.sleep(2)
                groww_pos = fetch_groww_positions()
                if symbol in groww_pos:
                    logger.error("[%s] ❌ After retries still present on Groww — manual check required", symbol)
                    # Don't clear local state if Groww still shows open (user must intervene)
                    return

            # confirm exit price best-effort via quote
            exit_price = None
            try:
                q = groww.get_quote(symbol, exchange=groww.EXCHANGE_NSE, segment=groww.SEGMENT_FNO)
                exit_price = float(q.get('last_price') or q.get('ltp') or 0)
            except Exception:
                exit_price = current_price

            entry_price = inst.get('entry_price')
            entry_time = inst.get('entry_time')
            inst['position'] = None
            inst['entry_price'] = None
            inst['entry_time'] = None
            inst['remaining_qty'] = None
            inst['partial_booked'] = False
            inst['last_exit_time'] = time.time()
            save_state(state)
            logger.info("[%s] ✅ EXIT LONG @ %s qty=%s (reason=%s)", symbol, exit_price, qty, exit_reason)
            if entry_price is not None and exit_price is not None:
                log_trade(symbol, "LONG", entry_price, exit_price, qty, entry_time, datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"), exit_reason)
        else:
            logger.debug("[%s] ⏳ Holding LONG, no exit trigger", symbol)
        return

    # ===== ENTRY LOGIC =====
    allow_entry = (
        now >= entry_allowed_time and
        now < no_new_buy_after_time and # 1) NEW: No new buy orders after 12:05 PM IST
        is_prev_ha_green and
        is_prev_norm_green and  # Retained as per request
        cooldown_passed and
        poc_price and
        current_price > poc_price
    )

    # Confirm no existing position in LOCAL state (master)
    if inst.get('position') == "LONG":
        logger.debug("[%s] ⚠️ Local state already shows LONG. Skipping entry", symbol)
        allow_entry = False

    # Confirm Groww has no open position (avoid duplication)
    groww_pos = fetch_groww_positions()
    if symbol in groww_pos:
        logger.warning("[%s] ⚠️ Groww shows open position (qty=%s). Skipping entry", symbol, groww_pos[symbol]['qty'])
        allow_entry = False

    logger.debug("[%s] DEBUG -> Allow Entry: %s", symbol, allow_entry)

    if allow_entry:
        qty = get_quantity_for_price(current_price)
        if qty > 0:
            logger.info("[%s] Attempting ENTRY BUY qty=%s @ %s", symbol, qty, current_price)
            resp = place_order(symbol, qty, "BUY")
            if resp:
                time.sleep(2)
                groww_pos = fetch_groww_positions()
                if symbol in groww_pos:
                    logger.info("[%s] ✅ Entry confirmed on Groww (qty %s)", symbol, groww_pos[symbol]['qty'])
                    inst['position'] = 'LONG'
                    inst['entry_price'] = current_price
                    inst['entry_time'] = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
                    inst['last_quantity'] = qty
                    inst['remaining_qty'] = qty
                    inst['partial_booked'] = False
                    inst['last_exit_time'] = None # Clear exit time on new entry
                    save_state(state)
                else:
                    logger.warning("[%s] ⚠️ Entry not seen on Groww after buy — retrying once", symbol)
                    place_order(symbol, qty, "BUY")
                    time.sleep(2)
            else:
                logger.error("[%s] ❌ Entry order failed or not placed", symbol)
        else:
            logger.info("[%s] ℹ️ LTP %s not in quantity brackets, skip", symbol, current_price)
    else:
        logger.debug("[%s] ⏳ Entry criteria not met", symbol)

# ========= MAIN LOOP =========
def main_loop():
    init_trades_csv()
    state = load_state()

    # Startup clear option (user-driven)
    if os.path.exists(POSITION_FILE):
        try:
            choice = input("Clear local position file? (yes/no): ").strip().lower()
        except Exception:
            choice = "no"
        if choice == "yes":
            state = {"instruments": {}}
            save_state(state)
            logger.info("Local position file cleared by user at startup.")

    logger.info("🚀 Trader started. Poll interval: %s sec. DRY_RUN: %s", POLL_INTERVAL, DRY_RUN)

    global running
    while running:
        cycle_start = time.time()
        now = now_ist()

        if not is_market_open(now):
            next_open = now.replace(hour=MARKET_OPEN[0], minute=MARKET_OPEN[1], second=0, microsecond=0)
            if now >= next_open:
                next_open += timedelta(days=1)
            wait = (next_open - now).total_seconds()
            logger.info("🌙 Market closed. Sleeping until %s IST", next_open.strftime("%Y-%m-%d %H:%M:%S"))
            time.sleep(min(wait, 600))
            continue

        if is_square_off_time(now):
            square_off_all(state)
            logger.info("⏹️ Square-off complete. Sleeping till market close...")
            time.sleep(60)
            continue

        # Show open positions from local master
        open_positions = {k:v for k,v in state['instruments'].items() if v.get('position')}
        logger.info("📊 Current Local Positions: %s", open_positions or "None")

        # Fetch selection
        instruments = fetch_top_options()
        if not instruments:
            logger.warning("⚠️ No instruments selected this cycle")
        else:
            for itm in instruments:
                try:
                    analyze_and_trade(itm['symbol'], state)
                except Exception as e:
                    logger.exception("Unexpected error in analyze_and_trade for %s: %s", itm['symbol'], e)

        cycle_end = time.time()
        elapsed = cycle_end - cycle_start
        sleep_time = max(1, POLL_INTERVAL - elapsed)
        logger.debug("⏳ Cycle done. Sleeping %.1f s", sleep_time)
            # Final save on exit
    save_state(state)
    logger.info("Trader stopped. State saved.")

if __name__ == "__main__":
    main_loop()
