# nifty_options_heikin_trader_render.py
# Run in Pydroid3 or on Render. Requires: pip install pandas growwapi pytz

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

# ========= USER CONFIG / ENVIRONMENT VARIABLES =========
# These will be read from environment variables on Render
# For local testing, you can uncomment and fill them, or set them in your shell.
 os.environ["API_AUTH_TOKEN"] = "YOUR_GROWW_API_TOKEN"
 os.environ["DRY_RUN"] = "False" # "True" or "False"
 os.environ["CLEAR_POSITION_ON_START"] = "False" # "True" or "False"
 os.environ["EXPIRY_DATE"] = "25O28" # Example: "25O20" for 25th Oct 2020. Adjust for current expiry.
 os.environ["PERSISTENT_DISK_PATH"] = "/var/data" # Example path for Render persistent disk


API_AUTH_TOKEN = os.getenv("API_AUTH_TOKEN")
DRY_RUN = os.getenv("DRY_RUN", "True").lower() == "true"
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "120"))
# Use a path on Render's persistent disk, if configured. Otherwise, files will be ephemeral.
# Default to current directory if PERSISTENT_DISK_PATH is not set (useful for local dev)
PERSISTENT_DISK_PATH = os.getenv("PERSISTENT_DISK_PATH", ".")

POSITION_FILE = os.path.join(PERSISTENT_DISK_PATH, "positions_state.json")
TRADES_LOG_CSV = os.path.join(PERSISTENT_DISK_PATH, "trades_log.csv")

PRICE_THRESHOLD = int(os.getenv("PRICE_THRESHOLD", "60"))
STRIKE_INTERVAL = int(os.getenv("STRIKE_INTERVAL", "50"))
STRIKE_COUNT = int(os.getenv("STRIKE_COUNT", "5"))
TOP_N = int(os.getenv("TOP_N", "2")) # Max number of *globally* opened positions
EXPIRY_DATE = os.getenv("EXPIRY_DATE", "25O20") # Dynamically set expiry date
CANDLE_INTERVAL_MIN = int(os.getenv("CANDLE_INTERVAL_MIN", "10")) # 🔹 user-defined candle interval (e.g., 15 or 30)

LOG_FILE = os.path.join(PERSISTENT_DISK_PATH, "trader.log") # Log file also on persistent disk

# market hours & square-off time (IST)
MARKET_OPEN = (9, 15)
MARKET_CLOSE = (15, 30)
SQUARE_OFF_TIME = (15, 18)
IST = pytz.timezone("Asia/Kolkata")

# ========= LOGGING =========
# Ensure log directory exists
os.makedirs(PERSISTENT_DISK_PATH, exist_ok=True)

logger = logging.getLogger("trader")
logger.setLevel(logging.DEBUG)
# Use RotatingFileHandler to manage log file size
from logging.handlers import RotatingFileHandler
fh = RotatingFileHandler(LOG_FILE, maxBytes=10*1024*1024, backupCount=5) # 10 MB, 5 backup files
fh.setLevel(logging.DEBUG)
fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
fh.setFormatter(fmt)
logger.addHandler(fh)
ch = logging.StreamHandler()
ch.setFormatter(fmt)
logger.addHandler(ch)

# ========= INIT API =========
logger.info("🔑 Initializing Groww API...")
groww = None # Initialize to None
try:
    if API_AUTH_TOKEN: # Only try to initialize if token is provided
        groww = GrowwAPI(API_AUTH_TOKEN)
        logger.info(f"✅ Groww API init done. DRY_RUN = {DRY_RUN}")
    else:
        logger.warning("⚠️ Groww API_AUTH_TOKEN not provided. Running in forced DRY_RUN mode.")
        DRY_RUN = True
except Exception as e:
    groww = None
    logger.warning("⚠️ Groww init failed (continuing in DRY_RUN or offline): %s", e)
    if not DRY_RUN:
        logger.critical("⛔ API initialization failed in live mode. Exiting.")
        import sys
        sys.exit(1)


# ========= STATE MANAGEMENT =========
def load_state():
    if os.path.exists(POSITION_FILE):
        try:
            with open(POSITION_FILE, "r") as f:
                s = json.load(f)
                if "instruments" not in s:
                    s = {"instruments": {}}
                # Clean up old/invalid states if necessary
                for symbol in list(s['instruments'].keys()):
                    if s['instruments'][symbol].get('position') not in [None, 'LONG']:
                        s['instruments'][symbol]['position'] = None # Reset to None if invalid state
                        logger.warning(f"Resetting invalid position state for {symbol}")
                return s
        except json.JSONDecodeError:
            logger.warning("Failed to decode JSON from state file, starting fresh.")
            return {"instruments": {}}
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
        if side == "LONG":
            pnl_value = (exit_price - entry_price) * qty
            pnl_pct = ((exit_price - entry_price) / entry_price) * 100 if entry_price != 0 else None
        # Removed SHORT side calculation as it's not explicitly used in the current strategy
        # elif side == "SHORT":
        #     pnl_value = (entry_price - exit_price) * qty
        #     pnl_pct = ((entry_price - exit_price) / entry_price) * 100 if entry_price != 0 else None
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
    # Ensure it's before market close but after square-off trigger
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

def get_historical_ohlc(symbol, minutes, days, include_previous_day=False):
    end_time_dt = now_ist()
    end_time = int(end_time_dt.timestamp() * 1000)
    
    start_time_dt = end_time_dt - timedelta(days=days) # Default behavior
    
    if include_previous_day:
        # Adjusted logic for previous day to ensure full previous day's data if needed
        # and not just a partial day based on current time
        market_open_today = end_time_dt.replace(hour=MARKET_OPEN[0], minute=MARKET_OPEN[1], second=0, microsecond=0)
        
        if end_time_dt < market_open_today: # If before today's market open
            # Get data from previous trading day's open
            prev_trading_day = end_time_dt - timedelta(days=1)
            # Find the actual previous trading day (skip weekends)
            while prev_trading_day.weekday() >= 5: # 5 is Saturday, 6 is Sunday
                prev_trading_day -= timedelta(days=1)
            start_time_dt = prev_trading_day.replace(hour=MARKET_OPEN[0], minute=MARKET_OPEN[1], second=0, microsecond=0)
        else:
            # If after today's market open, get data from current day's open or previous day depending on 'days' parameter
            start_time_dt = end_time_dt - timedelta(days=days)
            start_time_dt = start_time_dt.replace(hour=MARKET_OPEN[0], minute=MARKET_OPEN[1], second=0, microsecond=0)


    start_time = int(start_time_dt.timestamp() * 1000)

    try:
        if groww is None:
            logger.warning("Groww API not initialized. Cannot fetch historical data.")
            return None
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
                v = c.get("volume") or c.get("v") # Extract volume if available
            elif isinstance(c, (list, tuple)):
                # Assuming format: [timestamp, open, high, low, close, volume]
                t, o, h, l, close = c[:5]
                v = c[5] if len(c) > 5 else None # Extract volume
            else:
                continue
            
            t_val = int(float(t))
            if t_val < 1e11: # If timestamp is in seconds, convert to milliseconds
                t_val *= 1000
            
            candle_data = {"o": float(o), "h": float(h), "l": float(l), "c": float(close), "t": t_val}
            if v is not None:
                candle_data['v'] = float(v) # Add volume to the dict
            candles.append(candle_data)
        except Exception as e:
            logger.debug(f"Skipping malformed candle data for {symbol}: {c} - {e}")
            continue

    if not candles:
        return None
    df = pd.DataFrame(candles).sort_values("t").reset_index(drop=True)
    return df

# ========= POC (Median & Volume) =========
def calculate_median_poc(df):
    """Calculates Point of Control as the median of closing prices."""
    if df is None or len(df)==0:
        return None
    return float(df['c'].median())

def calculate_volume_poc(df):
    """
    Calculates Point of Control as the price level with the highest volume.
    Requires 'v' (volume) and 'c' (close) columns in the DataFrame.
    It considers the last known price to be representative of the POC range if volumes are spread.
    """
    if df is None or len(df) == 0 or 'v' not in df.columns or df['v'].sum() == 0:
        logger.debug("No valid volume data for Volume POC calculation.")
        return None
    
    # Create price bins for more robust POC detection, as exact match might be rare
    # Use close prices for binning, and then find the bin with max volume
    price_range = df['c'].max() - df['c'].min()
    num_bins = max(10, int(price_range / 0.5)) # At least 10 bins, or 0.5 price interval
    
    # If num_bins is very large due to small price movements but large range, cap it.
    # Alternatively, use fixed bin width. Let's use 0.5 as bin width.
    bin_width = 0.5
    min_price = df['c'].min() - bin_width # Start bin slightly below min price
    bins = [min_price + i * bin_width for i in range(int(price_range / bin_width) + 3)] # +3 for safety

    df['price_bin'] = pd.cut(df['c'], bins=bins, include_lowest=True, labels=False)
    
    volume_by_bin = df.groupby('price_bin')['v'].sum()
    if volume_by_bin.empty:
        return None

    max_volume_bin_index = volume_by_bin.idxmax()
    
    # Get the average price for candles falling into the max volume bin
    poc_candles = df[df['price_bin'] == max_volume_bin_index]
    if not poc_candles.empty:
        return float(poc_candles['c'].mean())
    
    return None


# ========= ORDER HANDLER =========
def place_order(symbol, quantity, side):
    """Place market order via Groww (or dry-run). Return order response on 'placed', else None."""
    if DRY_RUN:
        logger.info("[DRY RUN] Would place %s %s on %s", side, quantity, symbol)
        return {"status":"dry_run","symbol":symbol,"qty":quantity,"side":side}
    
    if groww is None:
        logger.error("[%s] Groww API is not initialized. Cannot place live orders.", symbol)
        return None
        
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
            logger.debug("Groww API not initialized. Cannot fetch Groww positions.")
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
def fetch_eligible_options_sorted_by_ltp():
    """
    Fetches NIFTY options, filters by price threshold, and returns them
    sorted by LTP descending (highest LTP < threshold first).
    """
    try:
        if groww is None:
            logger.warning("Groww API not initialized. Cannot fetch NIFTY quote.")
            return []
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
    all_syms = ce_symbols + pe_symbols

    eligible_options = []
    logger.info("ℹ️ Fetching quotes for CE and PE symbols around ATM...")
    for symbol in all_syms:
        try:
            if groww is None: # Double check if API is still initialized
                logger.warning(f"Groww API not initialized. Skipping quote for {symbol}.")
                break # Stop iterating if API is down
            quote = groww.get_quote(symbol, exchange=groww.EXCHANGE_NSE, segment=groww.SEGMENT_FNO)
            ltp = quote.get("last_price") or quote.get("ltp") or 0
            # Filter by PRICE_THRESHOLD
            if 0 < ltp < PRICE_THRESHOLD: # Ensure LTP is positive
                eligible_options.append({"symbol": symbol, "ltp": float(ltp)})
        except Exception as e:
            logger.debug(f"Could not get quote for {symbol}: {e}") # Debugging for specific symbol quote failures
            continue

    # Sort eligible options by LTP in descending order
    eligible_options.sort(key=lambda x: x['ltp'], reverse=True)
    
    logger.info("📈 Eligible instruments (LTP < %s, sorted by LTP desc): %s", PRICE_THRESHOLD, [i['symbol'] for i in eligible_options])
    return eligible_options

# ========= SQUARE-OFF =========
def square_off_all(state):
    logger.info("⏹️ Square-off: exiting all open positions now...")
    groww_pos = fetch_groww_positions()
    
    # Get a list of symbols to square off to avoid issues with dict modification during iteration
    symbols_to_square_off = []
    for symbol, info in state['instruments'].items():
        if info.get('position') == "LONG":
            symbols_to_square_off.append(symbol)

    for symbol in symbols_to_square_off:
        info = state['instruments'][symbol] # Re-fetch info as it might have changed
        if info.get('position') == "LONG": # Double check if still LONG
            remaining = info.get('remaining_qty') or info.get('last_quantity') or 0
            if remaining > 0:
                logger.info("[%s] Attempting square-off SELL qty=%s", symbol, remaining)
                resp = place_order(symbol, remaining, "SELL")
                
                # Check Groww positions repeatedly to confirm exit
                retries = 3
                for i in range(retries):
                    time.sleep(2) # Wait for order to process
                    groww_pos = fetch_groww_positions() # Refresh Groww positions
                    if symbol not in groww_pos:
                        logger.info("[%s] ✅ Square-off confirmed on Groww.", symbol)
                        break
                    else:
                        current_groww_qty = groww_pos[symbol]['qty']
                        logger.warning("[%s] ❌ Square-off not confirmed (Groww still shows qty %s), retry %s/%s", symbol, current_groww_qty, i+1, retries)
                        if i < retries -1: # Only retry order if it's not the last attempt
                            place_order(symbol, remaining, "SELL")
                else: # This block executes if the loop completes without a 'break'
                    logger.error("[%s] ❌ After retries still present on Groww — MANUAL CHECK REQUIRED", symbol)
                    # Do not clear local state if Groww still shows open (user must intervene)
                    continue # Skip state update for this symbol, try next one

                # If confirmed or exhausted retries without confirmation, update local state and log
                entry_price = info.get('entry_price')
                entry_time = info.get('entry_time')
                exit_price = None
                try:
                    # Get final LTP for logging
                    if groww:
                        q = groww.get_quote(symbol, exchange=groww.EXCHANGE_NSE, segment=groww.SEGMENT_FNO)
                        exit_price = float(q.get('last_price') or q.get('ltp') or 0)
                except Exception:
                    exit_price = None # If quote fails, exit price remains None for logging

                # Update local state
                info['position'] = None
                info['entry_price'] = None
                info['entry_time'] = None
                info['last_quantity'] = None # Clear original quantity
                info['remaining_qty'] = None
                info['partial_booked'] = False
                info['last_exit_time'] = time.time()
                
                logger.info("[%s] Local state cleared after square-off attempt.", symbol)
                if entry_price and exit_price is not None:
                    log_trade(symbol, "LONG", entry_price, exit_price, remaining, entry_time, datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"), "SQUARE_OFF")
            else:
                logger.debug("[%s] No quantity to square-off in local state.", symbol)
    save_state(state)
    logger.info("⏹️ All known positions processed for square-off.")


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

    df_ohlc = get_historical_ohlc(symbol, minutes=CANDLE_INTERVAL_MIN, days=5, include_previous_day=True) 
    
    if df_ohlc is None or len(df_ohlc) < 3: 
        logger.debug("[%s] ⚠️ Not enough %d-min OHLC data (need 3 normal candles), skipping", symbol, CANDLE_INTERVAL_MIN)
        return False # Indicate no trade

    ha10 = heikin_ashi(df_ohlc)
    if len(ha10) < 3: 
        logger.debug("[%s] ⚠️ Not enough HA candles (need 3), skipping", symbol)
        return False # Indicate no trade

    prev_ha = ha10.iloc[-2]
    is_prev_ha_red = prev_ha['c'] < prev_ha['o']
    is_prev_ha_green = prev_ha['c'] > prev_ha['o']

    prev_prev_ha = ha10.iloc[-3]
    is_prev_prev_ha_red = prev_prev_ha['c'] < prev_prev_ha['o']

    current_price = float(df_ohlc['c'].iloc[-1]) # Current price based on last normal candle close

    now = now_ist()
    
    # Calculate POC (Volume POC first, then Median POC)
    poc_price = calculate_volume_poc(df_ohlc)
    poc_type = "Volume"
    if poc_price is None:
        poc_price = calculate_median_poc(df_ohlc)
        poc_type = "Median"
        logger.debug("[%s] Volume POC not available or zero, falling back to Median POC.", symbol)

    cooldown_passed = True
    last_exit = inst.get('last_exit_time')
    if last_exit and (time.time() - last_exit) / 60.0 < 15.0:
        cooldown_passed = False

    logger.debug("[%s] DEBUG -> Current Price: %s, %s POC: %s, Prev HA Red: %s, PrevPrev HA Red: %s, Prev HA Green: %s, Cooldown Passed: %s",
                 symbol, current_price, poc_type, poc_price, is_prev_ha_red, is_prev_prev_ha_red, is_prev_ha_green, cooldown_passed)

    # ===== EXIT LOGIC =====
    if inst.get('position') == "LONG":
        qty = inst.get('remaining_qty') or inst.get('last_quantity') or 0
        exit_condition = False
        exit_reason = None

        if is_prev_ha_red:
            exit_condition = True
            exit_reason = "HA_RED"
        elif inst.get('entry_price') and current_price < 0.5 * inst['entry_price']:
            exit_condition = True
            exit_reason = "PRICE_BELOW_50PCT_ENTRY"

        if exit_condition and qty > 0:
            logger.info("[%s] 🔴 Exit triggered (%s). Attempting SELL qty=%s", symbol, exit_reason, qty)
            resp = place_order(symbol, qty, "SELL")
            
            # Check Groww positions repeatedly to confirm exit
            retries = 3
            for i in range(retries):
                time.sleep(2) # Wait for order to process
                groww_pos = fetch_groww_positions()
                if symbol not in groww_pos:
                    logger.info("[%s] ✅ Exit confirmed on Groww.", symbol)
                    break
                else:
                    logger.warning("[%s] ❌ Exit not confirmed (Groww still shows qty %s), retry %s/%s", symbol, groww_pos[symbol]['qty'], i+1, retries)
                    if i < retries -1: # Only retry order if it's not the last attempt
                        place_order(symbol, qty, "SELL")
            else: # This block executes if the loop completes without a 'break'
                logger.error("[%s] ❌ After retries still present on Groww — MANUAL CHECK REQUIRED", symbol)
                return False # Indicate no successful trade action for main loop

            # If confirmed or exhausted retries without confirmation, update local state and log
            exit_price = None
            try:
                if groww:
                    q = groww.get_quote(symbol, exchange=groww.EXCHANGE_NSE, segment=groww.SEGMENT_FNO)
                    exit_price = float(q.get('last_price') or q.get('ltp') or 0)
            except Exception:
                exit_price = current_price # Fallback to current OHLC close if quote fails

            entry_price = inst.get('entry_price')
            entry_time = inst.get('entry_time')
            inst['position'] = None
            inst['entry_price'] = None
            inst['entry_time'] = None
            inst['last_quantity'] = None
            inst['remaining_qty'] = None
            inst['partial_booked'] = False
            inst['last_exit_time'] = time.time()
            save_state(state)
            logger.info("[%s] ✅ EXIT LONG @ %s qty=%s (reason=%s)", symbol, exit_price, qty, exit_reason)
            if entry_price is not None and exit_price is not None:
                log_trade(symbol, "LONG", entry_price, exit_price, qty, entry_time, datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"), exit_reason)
            return True # Indicate successful trade action
        else:
            logger.debug("[%s] ⏳ Holding LONG, no exit trigger", symbol)
        return False # Indicate no trade action

    # ===== ENTRY LOGIC =====
    no_new_buy_after_time = now.replace(hour=12, minute=5, second=0, microsecond=0)

    allow_entry = (
    now_ist().time() >= first_candle_close_time and # Only allow entry after first candle of the day closes
        #now < no_new_buy_after_time and # No new buys after 12:05 PM (kept this from original)
        is_prev_prev_ha_red and     # 3rd last HA candle RED
        is_prev_ha_green and        # 2nd last HA candle GREEN
        cooldown_passed and
        poc_price is not None and   # Ensure POC was calculated
        current_price > poc_price
    )

    if inst.get('position') == "LONG":
        logger.debug("[%s] ⚠️ Local state already shows LONG. Skipping entry", symbol)
        allow_entry = False

    groww_pos = fetch_groww_positions()
    if symbol in groww_pos:
        logger.warning("[%s] ⚠️ Groww shows open position (qty=%s). Skipping entry", symbol, groww_pos[symbol]['qty'])
        allow_entry = False
    
    # Check if API is initialized before attempting to trade live
    if not DRY_RUN and groww is None:
        logger.warning("[%s] Groww API not initialized in live mode. Skipping entry.", symbol)
        allow_entry = False


    logger.debug("[%s] DEBUG -> Allow Entry: %s", symbol, allow_entry)

    if allow_entry:
        qty = get_quantity_for_price(current_price)
        if qty > 0:
            logger.info("[%s] Attempting ENTRY BUY qty=%s @ %s", symbol, qty, current_price)
            resp = place_order(symbol, qty, "BUY")
            if resp:
                # Check Groww positions repeatedly to confirm entry
                retries = 3
                for i in range(retries):
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
                        return True # Indicate successful trade action
                    else:
                        logger.warning("[%s] ⚠️ Entry not seen on Groww after buy, retry %s/%s", symbol, i+1, retries)
                        if i < retries -1: # Only retry order if it's not the last attempt
                            place_order(symbol, qty, "BUY")
                else:
                    logger.error("[%s] ❌ Entry order failed after retries or not confirmed on Groww.", symbol)
                    return False # Indicate no successful trade action
            else:
                logger.error("[%s] ❌ Initial entry order failed or not placed (API response error).", symbol)
                return False # Indicate no successful trade action
        else:
            logger.info("[%s] ℹ️ LTP %s not in quantity brackets, skip", symbol, current_price)
            return False # Indicate no trade action
    else:
        logger.debug("[%s] ⏳ Entry criteria not met", symbol)
        return False # Indicate no trade action

# ========= MAIN LOOP =========
def main_loop():
    # Ensure the persistent disk path exists
    os.makedirs(PERSISTENT_DISK_PATH, exist_ok=True)
    
    init_trades_csv()
    state = load_state()

    # New logic: Check environment variable for clearing state
    clear_on_start = os.getenv("CLEAR_POSITION_ON_START", "False").lower() == "true"
    if clear_on_start:
        state = {"instruments": {}}
        save_state(state)
        logger.info("Local position file cleared via CLEAR_POSITION_ON_START environment variable.")

    logger.info("🚀 Trader started. Poll interval: %s sec. DRY_RUN: %s, Global TOP_N entries: %s", POLL_INTERVAL, DRY_RUN, TOP_N)
    logger.info("Persistence Path: %s, Position File: %s, Trades Log: %s", PERSISTENT_DISK_PATH, POSITION_FILE, TRADES_LOG_CSV)


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
            time.sleep(min(wait, 600)) # Sleep max 10 minutes
            continue

        if is_square_off_time(now):
            square_off_all(state)
            logger.info("⏹️ Square-off complete. Sleeping till market close...")
            # After square-off, wait until market close + a buffer to avoid re-entry attempts
            market_close_dt = now.replace(hour=MARKET_CLOSE[0], minute=MARKET_CLOSE[1], second=0, microsecond=0) + timedelta(minutes=5) # 5 min buffer
            if now < market_close_dt:
                wait_after_so = (market_close_dt - now).total_seconds()
                time.sleep(max(60, wait_after_so)) # Sleep at least 1 min, or until after market close
            continue
        
        # Count currently open positions from local state
        # This is the master list for enforcing TOP_N
        current_open_positions_count = sum(1 for inst_info in state['instruments'].values() if inst_info.get('position') == "LONG")
        logger.info("📊 Current Local Positions: %s (Count: %s/%s TOP_N)", 
                    {k:v for k,v in state['instruments'].items() if v.get('position')}, current_open_positions_count, TOP_N)

        # Process existing open positions first for potential exits
        for symbol in list(state['instruments'].keys()): # Iterate over a copy to allow dict modification
            if state['instruments'][symbol].get('position') == "LONG":
                try:
                    # analyze_and_trade handles exits for existing positions
                    analyze_and_trade(symbol, state)
                    time.sleep(1) # Small delay to not spam API
                except Exception as e:
                    logger.exception("Unexpected error processing existing position %s: %s", symbol, e)
        
        # Re-count open positions after processing exits
        current_open_positions_count = sum(1 for inst_info in state['instruments'].values() if inst_info.get('position') == "LONG")
        
        # === NEW LOGIC: CONDITIONAL FETCHING OF ELIGIBLE INSTRUMENTS ===
        if current_open_positions_count < TOP_N:
            # Fetch eligible instruments for potential NEW entries ONLY IF we have capacity
            eligible_instruments = fetch_eligible_options_sorted_by_ltp()
            if not eligible_instruments:
                logger.warning("⚠️ No eligible instruments found this cycle for new entries.")
            else:
                new_entries_attempted_this_cycle = 0
                for itm in eligible_instruments:
                    if not running: break # Check shutdown flag again

                    # Only attempt new entries if we are below TOP_N and haven't hit TOP_N in this cycle's new attempts
                    if current_open_positions_count < TOP_N:
                        if state['instruments'].get(itm['symbol'], {}).get('position') is None: # Only consider instruments not already held
                            logger.debug("Attempting new entry for %s. Open count: %s/%s", itm['symbol'], current_open_positions_count, TOP_N)
                            try:
                                trade_successful = analyze_and_trade(itm['symbol'], state)
                                if trade_successful and state['instruments'][itm['symbol']]['position'] == 'LONG':
                                    current_open_positions_count += 1 # Increment if a new position was successfully opened
                                    new_entries_attempted_this_cycle += 1
                                    logger.info("✅ New entry confirmed for %s. Total open: %s/%s", itm['symbol'], current_open_positions_count, TOP_N)
                                time.sleep(1) # Small delay between processing instruments
                            except Exception as e:
                                logger.exception("Unexpected error in analyze_and_trade for new entry %s: %s", itm['symbol'], e)
                    else:
                        logger.info("🎯 Global TOP_N (%s) open positions reached. Skipping further new entry attempts.", TOP_N)
                        break # Stop trying to enter new positions
        else:
            logger.info("🎯 Global TOP_N (%s) open positions already reached. Skipping fetching new instruments.", TOP_N)
            
        # === END OF NEW LOGIC ===

        cycle_end = time.time()
        elapsed = cycle_end - cycle_start
        sleep_time = max(1, POLL_INTERVAL - elapsed)
        logger.debug("⏳ Cycle done. Sleeping %.1f s", sleep_time)
        time.sleep(sleep_time)

    # Final save on exit
    save_state(state)
    logger.info("Trader stopped. State saved.")

if __name__ == "__main__":
    main_loop()
