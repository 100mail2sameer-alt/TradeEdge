from __future__ import annotations

import os
from datetime import datetime

from flask import Flask, jsonify, request, send_from_directory
from kiteconnect import KiteConnect

app = Flask(__name__, static_folder=".", static_url_path="")

# Simple in-memory session store for this demo (clears when server stops)
SESSION = {
  "api_key": None,
  "access_token": None,
}


def _build_kite(api_key: str) -> KiteConnect:
  return KiteConnect(api_key=api_key)


@app.get("/")
def index():
  return send_from_directory(".", "index.html")


@app.post("/api/login_url")
def login_url():
  data = request.get_json(force=True)
  api_key = (data.get("api_key") or "").strip()
  if not api_key:
    return jsonify({"error": "api_key_required"}), 400

  kite = _build_kite(api_key)
  SESSION["api_key"] = api_key
  return jsonify({"login_url": kite.login_url()})


@app.post("/api/access_token")
def access_token():
  data = request.get_json(force=True)
  api_key = (data.get("api_key") or "").strip()
  api_secret = (data.get("api_secret") or "").strip()
  request_token = (data.get("request_token") or "").strip()

  if not api_key or not api_secret or not request_token:
    return jsonify({"error": "api_key_api_secret_request_token_required"}), 400

  kite = _build_kite(api_key)
  session = kite.generate_session(request_token, api_secret=api_secret)
  access_token_value = session.get("access_token")

  SESSION["api_key"] = api_key
  SESSION["access_token"] = access_token_value

  return jsonify({"access_token": access_token_value})


@app.post("/api/ltp")
def ltp():
  data = request.get_json(force=True)
  symbols = data.get("symbols") or []
  api_key = SESSION.get("api_key")
  access_token_value = SESSION.get("access_token")

  if not api_key or not access_token_value:
    return jsonify({"error": "not_logged_in"}), 401

  kite = _build_kite(api_key)
  kite.set_access_token(access_token_value)

  return jsonify(kite.ltp(symbols))


@app.post("/api/historical")
def historical():
  data = request.get_json(force=True)
  api_key = SESSION.get("api_key")
  access_token_value = SESSION.get("access_token")

  if not api_key or not access_token_value:
    return jsonify({"error": "not_logged_in"}), 401

  instrument_token = int(data.get("instrument_token"))
  interval = data.get("interval") or "day"
  from_date = data.get("from_date")
  to_date = data.get("to_date")

  kite = _build_kite(api_key)
  kite.set_access_token(access_token_value)

  candles = kite.historical_data(
    instrument_token=instrument_token,
    from_date=datetime.fromisoformat(from_date),
    to_date=datetime.fromisoformat(to_date),
    interval=interval,
    continuous=False,
    oi=False,
  )

  return jsonify(candles)


if __name__ == "__main__":
  port = int(os.getenv("PORT", "8000"))
  app.run(debug=True, port=port)