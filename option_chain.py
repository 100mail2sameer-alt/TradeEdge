from __future__ import annotations

import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from flask import Blueprint, jsonify, request
from kiteconnect import KiteConnect, KiteTicker

bp = Blueprint("option_chain", __name__)

SESSION = {
  "api_key": None,
  "access_token": None,
}

latest_ticks: Dict[int, dict] = {}
subscribed_tokens: set[int] = set()
_ticker: KiteTicker | None = None
_ticker_lock = threading.Lock()
last_close_stats: Dict[int, dict] = {}

INSTRUMENTS_CACHE = {
  "NFO": None,
  "NSE": None,
}

INDEX_UNDERLYINGS = {
  "NIFTY",
  "BANKNIFTY",
  "FINNIFTY",
  "MIDCPNIFTY",
}


def init_session(api_key: str, access_token: str) -> None:
  SESSION["api_key"] = api_key
  SESSION["access_token"] = access_token
  _start_ticker(api_key, access_token)


def clear_session() -> None:
  global _ticker
  with _ticker_lock:
    if _ticker is not None:
      try:
        _ticker.close()
      except Exception:
        pass
      _ticker = None

  SESSION["api_key"] = None
  SESSION["access_token"] = None
  latest_ticks.clear()
  subscribed_tokens.clear()
  last_close_stats.clear()
  INSTRUMENTS_CACHE["NFO"] = None
  INSTRUMENTS_CACHE["NSE"] = None


def _build_kite(api_key: str) -> KiteConnect:
  return KiteConnect(api_key=api_key)


def _start_ticker(api_key: str, access_token: str) -> None:
  global _ticker

  with _ticker_lock:
    if _ticker is not None:
      try:
        _ticker.close()
      except Exception:
        pass
      _ticker = None

    kws = KiteTicker(api_key, access_token)

    def on_ticks(ws, ticks):
      for tick in ticks:
        token = tick.get("instrument_token")
        if token is None:
          continue
        latest_ticks[token] = {
          "instrument_token": token,
          "last_price": tick.get("last_price"),
          "change": tick.get("change"),
          "ohlc": tick.get("ohlc"),
          "volume": tick.get("volume"),
          "oi": tick.get("oi"),
          "timestamp": tick.get("timestamp") or tick.get("exchange_timestamp"),
        }

    def on_connect(ws, response):
      tokens = list(subscribed_tokens)
      if tokens:
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_FULL, tokens)

    def on_close(ws, code, reason):
      ws.stop()

    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close

    thread = threading.Thread(target=kws.connect, daemon=True)
    thread.start()
    _ticker = kws


def _ensure_logged_in() -> bool:
  return bool(SESSION.get("api_key") and SESSION.get("access_token"))


def _get_kite() -> KiteConnect:
  api_key = SESSION.get("api_key")
  access_token = SESSION.get("access_token")
  kite = _build_kite(api_key)
  kite.set_access_token(access_token)
  return kite


def _load_last_close_stats(kite: KiteConnect, tokens: List[int]) -> None:
  if not tokens:
    return

  to_date = datetime.now()
  from_date = to_date - timedelta(days=7)
  for token in tokens:
    try:
      candles = kite.historical_data(
        instrument_token=token,
        from_date=from_date,
        to_date=to_date,
        interval="day",
        continuous=False,
        oi=True,
      )
    except Exception:
      continue

    if not candles:
      continue

    last_candle = candles[-1]
    last_close_stats[token] = {
      "volume": last_candle.get("volume"),
      "oi": last_candle.get("oi"),
    }


def _load_instruments(kite: KiteConnect) -> Tuple[List[dict], List[dict]]:
  if INSTRUMENTS_CACHE["NFO"] is None:
    INSTRUMENTS_CACHE["NFO"] = kite.instruments("NFO")
  if INSTRUMENTS_CACHE["NSE"] is None:
    INSTRUMENTS_CACHE["NSE"] = kite.instruments("NSE")
  return INSTRUMENTS_CACHE["NFO"], INSTRUMENTS_CACHE["NSE"]


def _underlying_symbol(underlying: str, nse_instruments: List[dict]) -> Optional[str]:
  mapping = {
    "NIFTY": "NIFTY 50",
    "BANKNIFTY": "NIFTY BANK",
    "FINNIFTY": "NIFTY FIN SERVICE",
    "MIDCPNIFTY": "NIFTY MID SELECT",
  }
  if underlying in mapping:
    return f"NSE:{mapping[underlying]}"

  for inst in nse_instruments:
    if inst.get("tradingsymbol") == underlying:
      return f"NSE:{underlying}"

  for inst in nse_instruments:
    if inst.get("name") == underlying:
      return f"NSE:{inst.get('tradingsymbol')}"

  return None


def _get_underlying_quote(kite: KiteConnect, underlying: str, nse_instruments: List[dict]) -> Tuple[Optional[float], Optional[float]]:
  symbol = _underlying_symbol(underlying, nse_instruments)
  if not symbol:
    return None, None

  try:
    data = kite.quote([symbol])
  except Exception:
    return None, None

  quote = data.get(symbol)
  if not quote:
    return None, None

  last_price = quote.get("last_price")
  ohlc = quote.get("ohlc") or {}
  close_price = ohlc.get("close")

  change_pct = None
  if last_price is not None and close_price:
    try:
      change_pct = ((last_price - close_price) / close_price) * 100
    except ZeroDivisionError:
      change_pct = None

  return last_price, change_pct


def _get_underlyings(nfo_instruments: List[dict], category: str) -> List[str]:
  names = {
    inst.get("name")
    for inst in nfo_instruments
    if inst.get("segment") == "NFO-OPT" and inst.get("name")
  }
  if category == "index":
    names = {name for name in names if name in INDEX_UNDERLYINGS}
  elif category == "stock":
    names = {name for name in names if name not in INDEX_UNDERLYINGS}
  return sorted(names)


def _get_expiries(nfo_instruments: List[dict], underlying: str) -> List[str]:
  expiries = {
    inst.get("expiry")
    for inst in nfo_instruments
    if inst.get("segment") == "NFO-OPT" and inst.get("name") == underlying
  }
  return sorted([exp.strftime("%Y-%m-%d") for exp in expiries if exp])


def _build_chain_rows(
  nfo_instruments: List[dict],
  underlying: str,
  expiry: str,
  ltp: Optional[float],
  strike_interval: Optional[float] = None,
):
  filtered = [
    inst
    for inst in nfo_instruments
    if inst.get("segment") == "NFO-OPT"
    and inst.get("name") == underlying
    and inst.get("expiry")
    and inst.get("expiry").strftime("%Y-%m-%d") == expiry
    and inst.get("instrument_type") in ("CE", "PE")
  ]

  strike_map: Dict[float, Dict[str, dict]] = {}
  for inst in filtered:
    strike = inst.get("strike")
    if strike is None:
      continue
    strike_map.setdefault(float(strike), {})[inst.get("instrument_type")] = inst

  strikes = sorted(strike_map.keys())
  if not strikes:
    return []

  if strike_interval:
    scale = 100 if strike_interval % 1 else 1
    interval_scaled = int(round(strike_interval * scale))
    strikes = [
      s for s in strikes if int(round(s * scale)) % interval_scaled == 0
    ] or strikes
  if not strikes:
    return []

  rows = []
  if ltp is not None:
    atm_strike = min(strikes, key=lambda s: abs(s - ltp))
    atm_index = strikes.index(atm_strike)
  else:
    atm_index = len(strikes) // 2
  start = max(0, atm_index - 20)
  end = min(len(strikes), atm_index + 21)
  strikes = strikes[start:end]

  for strike in strikes:
    ce = strike_map[strike].get("CE")
    pe = strike_map[strike].get("PE")
    rows.append(
      {
        "strike": strike,
        "ce_token": ce.get("instrument_token") if ce else None,
        "pe_token": pe.get("instrument_token") if pe else None,
        "ce_symbol": ce.get("tradingsymbol") if ce else "--",
        "pe_symbol": pe.get("tradingsymbol") if pe else "--",
        "lot_size": (ce.get("lot_size") if ce else None) or (pe.get("lot_size") if pe else None),
        "ce_exchange": ce.get("exchange") if ce else None,
        "pe_exchange": pe.get("exchange") if pe else None,
      }
    )

  return rows


def _get_strike_intervals(nfo_instruments: List[dict], underlying: str, expiry: str) -> List[float]:
  strikes = sorted(
    {
      float(inst.get("strike"))
      for inst in nfo_instruments
      if inst.get("segment") == "NFO-OPT"
      and inst.get("name") == underlying
      and inst.get("expiry")
      and inst.get("expiry").strftime("%Y-%m-%d") == expiry
      and inst.get("strike") is not None
    }
  )
  if len(strikes) < 2:
    return []

  diffs = sorted({round(strikes[i + 1] - strikes[i], 4) for i in range(len(strikes) - 1)})
  intervals = [d for d in diffs if d >= 100]
  for extra in (100.0, 200.0):
    if extra not in intervals:
      intervals.append(extra)
  return sorted(intervals)


@bp.get("/api/option-chain/underlyings")
def option_chain_underlyings():
  if not _ensure_logged_in():
    return jsonify({"error": "not_logged_in"}), 401

  category = (request.args.get("type") or "index").strip().lower()
  if category not in ("index", "stock"):
    category = "index"

  kite = _get_kite()
  nfo_instruments, _ = _load_instruments(kite)
  return jsonify({"underlyings": _get_underlyings(nfo_instruments, category)})


@bp.get("/api/option-chain/expiries")
def option_chain_expiries():
  if not _ensure_logged_in():
    return jsonify({"error": "not_logged_in"}), 401

  underlying = (request.args.get("underlying") or "").strip()
  category = (request.args.get("type") or "index").strip().lower()
  if category not in ("index", "stock"):
    category = "index"
  if not underlying:
    return jsonify({"error": "underlying_required"}), 400

  kite = _get_kite()
  nfo_instruments, _ = _load_instruments(kite)
  allowed = _get_underlyings(nfo_instruments, category)
  if underlying not in allowed:
    return jsonify({"error": "invalid_underlying"}), 400
  return jsonify({"expiries": _get_expiries(nfo_instruments, underlying)})


@bp.get("/api/option-chain/intervals")
def option_chain_intervals():
  if not _ensure_logged_in():
    return jsonify({"error": "not_logged_in"}), 401

  underlying = (request.args.get("underlying") or "").strip()
  expiry = (request.args.get("expiry") or "").strip()
  category = (request.args.get("type") or "index").strip().lower()
  if category not in ("index", "stock"):
    category = "index"
  if not underlying or not expiry:
    return jsonify({"error": "underlying_expiry_required"}), 400

  kite = _get_kite()
  nfo_instruments, _ = _load_instruments(kite)
  allowed = _get_underlyings(nfo_instruments, category)
  if underlying not in allowed:
    return jsonify({"error": "invalid_underlying"}), 400

  intervals = _get_strike_intervals(nfo_instruments, underlying, expiry)
  return jsonify({"intervals": intervals})


@bp.post("/api/option-chain/build")
def option_chain_build():
  if not _ensure_logged_in():
    return jsonify({"error": "not_logged_in"}), 401

  data = request.get_json(force=True)
  underlying = (data.get("underlying") or "").strip()
  expiry = (data.get("expiry") or "").strip()
  category = (data.get("instrument_type") or "index").strip().lower()
  if category not in ("index", "stock"):
    category = "index"
  interval = data.get("interval")
  strike_interval = float(interval) if interval else None

  if not underlying or not expiry:
    return jsonify({"error": "underlying_expiry_required"}), 400

  kite = _get_kite()
  nfo_instruments, nse_instruments = _load_instruments(kite)
  allowed = _get_underlyings(nfo_instruments, category)
  if underlying not in allowed:
    return jsonify({"error": "invalid_underlying"}), 400

  ltp, change_pct = _get_underlying_quote(kite, underlying, nse_instruments)
  rows = _build_chain_rows(nfo_instruments, underlying, expiry, ltp, strike_interval)

  tokens = [row["ce_token"] for row in rows if row["ce_token"]] + [row["pe_token"] for row in rows if row["pe_token"]]

  global subscribed_tokens
  with _ticker_lock:
    if _ticker is not None and subscribed_tokens:
      _ticker.unsubscribe(list(subscribed_tokens))
    subscribed_tokens = set(tokens)
    if _ticker is not None and tokens:
      _ticker.subscribe(tokens)
      _ticker.set_mode(_ticker.MODE_FULL, tokens)

  latest_ticks.clear()
  last_close_stats.clear()
  _load_last_close_stats(kite, tokens)

  return jsonify(
    {
      "underlying": underlying,
      "ltp": ltp,
      "change_pct": change_pct,
      "rows": rows,
    }
  )


@bp.get("/api/option-chain/ticks")
def get_ticks():
  if not _ensure_logged_in():
    return jsonify({"error": "not_logged_in"}), 401
  resolved = {}
  for token, tick in latest_ticks.items():
    fallback = last_close_stats.get(token, {})
    resolved_tick = dict(tick)
    if resolved_tick.get("volume") in (None, 0):
      resolved_tick["volume"] = fallback.get("volume")
    if resolved_tick.get("oi") in (None, 0):
      resolved_tick["oi"] = fallback.get("oi")
    resolved[token] = resolved_tick

  for token, fallback in last_close_stats.items():
    if token in resolved:
      continue
    resolved[token] = {
      "instrument_token": token,
      "last_price": None,
      "volume": fallback.get("volume"),
      "oi": fallback.get("oi"),
    }

  return jsonify({"ticks": resolved})


@bp.get("/api/portfolio/positions")
def portfolio_positions():
  if not _ensure_logged_in():
    return jsonify({"error": "not_logged_in"}), 401

  kite = _get_kite()
  data = kite.positions()
  return jsonify(data)


@bp.post("/api/trade/execute")
def trade_execute():
  if not _ensure_logged_in():
    return jsonify({"error": "not_logged_in"}), 401
  return jsonify({"error": "Execution is not configured yet."}), 400


@bp.post("/api/trade/exit-all")
def trade_exit_all():
  if not _ensure_logged_in():
    return jsonify({"error": "not_logged_in"}), 401
  return jsonify({"error": "Exit all is not configured yet."}), 400
