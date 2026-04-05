from __future__ import annotations

import os

from flask import Flask, jsonify, render_template, request
from kiteconnect import KiteConnect

from option_chain import bp as option_chain_bp, clear_session, init_session

app = Flask(__name__, template_folder="templates", static_folder="static")
app.register_blueprint(option_chain_bp)

SESSION = {
  "api_key": None,
  "access_token": None,
}


def _build_kite(api_key: str) -> KiteConnect:
  return KiteConnect(api_key=api_key)


@app.get("/")
def index():
  return render_template("index.html")


@app.get("/option-chain")
def option_chain_page():
  return render_template("option_chain.html")


@app.get("/option_chain.html")
def option_chain_legacy():
  return render_template("option_chain.html")


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

  init_session(api_key, access_token_value)

  return jsonify({"access_token": access_token_value})


@app.post("/api/logout")
def logout():
  clear_session()
  SESSION["api_key"] = None
  SESSION["access_token"] = None
  return jsonify({"ok": True})


if __name__ == "__main__":
  port = int(os.getenv("PORT", "8000"))
  app.run(debug=True, port=port)
