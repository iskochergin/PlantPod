from __future__ import annotations
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, List, Tuple

from flask import Blueprint, jsonify, request, session, current_app

profile_bp = Blueprint("profile_bp", __name__)

AUTH_DIR = Path("auth")
USERS_PATH = AUTH_DIR / "users.json"  # логины/пароли в открытом виде
PROFILES_PATH = AUTH_DIR / "profiles.json"  # счётчики, создаётся автоматически

AUTH_DIR.mkdir(parents=True, exist_ok=True)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _today_key(dt: datetime | None = None) -> str:
    dt = dt or _now_utc()
    return dt.date().isoformat()


def _read_json(path: Path, default):
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _write_json(path: Path, data):
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def _load_users() -> Dict[str, Dict[str, Any]]:
    return _read_json(USERS_PATH, {})


def _load_profiles() -> Dict[str, Dict[str, Any]]:
    return _read_json(PROFILES_PATH, {})


def _save_profiles(data: Dict[str, Dict[str, Any]]):
    _write_json(PROFILES_PATH, data)


# ---- auth ----
def _verify_password(user_rec: Dict[str, Any], password: str) -> bool:
    # ПРОСТОЕ сравнение открытого пароля
    return (user_rec.get("password") or "") == (password or "")


# ---- stats helpers ----
def _ensure_profile(profiles: Dict[str, Any], user: str) -> Dict[str, Any]:
    prof = profiles.get(user)
    if not prof:
        prof = {"history": {}, "total": 0, "streak_days": 0, "streak_last": ""}
        profiles[user] = prof
    return prof


def _sum_window(history: Dict[str, int], days: int, today: str) -> int:
    base = datetime.fromisoformat(today)
    s = 0
    for i in range(days):
        key = (base - timedelta(days=i)).date().isoformat()
        s += int(history.get(key, 0))
    return s


def record_added_for_user(username: str, added: int):
    if not username or added <= 0:
        return
    profiles = _load_profiles()
    prof = _ensure_profile(profiles, username)
    today = _today_key()
    prof["history"][today] = int(prof["history"].get(today, 0)) + int(added)
    prof["total"] = int(prof.get("total", 0)) + int(added)

    last = prof.get("streak_last") or ""
    if last == "":
        prof["streak_days"] = 1 if prof["history"][today] > 0 else 0
        prof["streak_last"] = today
    else:
        if last != today:
            last_dt = datetime.fromisoformat(last)
            if (datetime.fromisoformat(today) - last_dt).days == 1 and prof["history"][today] > 0:
                prof["streak_days"] = int(prof.get("streak_days", 0)) + 1
            elif prof["history"][today] > 0:
                prof["streak_days"] = 1
            prof["streak_last"] = today

    _save_profiles(profiles)


def _stats_payload(username: str) -> Dict[str, Any]:
    profiles = _load_profiles()
    prof = _ensure_profile(profiles, username)
    today = _today_key()
    day = int(prof["history"].get(today, 0))
    week = _sum_window(prof["history"], 7, today)
    month = _sum_window(prof["history"], 30, today)
    total = int(prof.get("total", 0))
    streak_days = int(prof.get("streak_days", 0))
    return {"day": day, "week": week, "month": month, "total": total,
            "streak_days": streak_days, "today_done": day >= 200}


# ---- endpoints ----
@profile_bp.post("/api/auth/login")
def api_login():
    if not USERS_PATH.exists():
        return jsonify({"ok": False, "error": "users.json not found"}), 400
    js = request.get_json(silent=True) or {}
    username = (js.get("username") or "").strip()
    password = (js.get("password") or "")
    users = _load_users()
    rec = users.get(username)
    if not rec or not _verify_password(rec, password):
        return jsonify({"ok": False, "error": "invalid credentials"}), 200
    session["user"] = username
    session.permanent = True  # ← сохраняем сессию надолго
    current_app.logger.info("login: %s", username)
    return jsonify({"ok": True, "user": username})


@profile_bp.post("/api/auth/logout")
def api_logout():
    u = session.pop("user", None)
    current_app.logger.info("logout: %s", u)
    return jsonify({"ok": True})


@profile_bp.get("/api/auth/me")
def api_me():
    if "user" not in session:
        return jsonify({"logged_in": False})
    u = session["user"]
    stats = _stats_payload(u)
    return jsonify({"logged_in": True, "user": u, "stats": stats})


@profile_bp.get("/api/auth/leaderboard")
def api_leaderboard():
    window = (request.args.get("window") or "day").lower()
    if window not in ("day", "week", "month", "total"):
        window = "day"
    users = _load_users()
    profiles = _load_profiles()
    today = _today_key()
    rows = []
    for u in users.keys():
        prof = profiles.get(u) or {}
        if window == "total":
            val = int(prof.get("total", 0))
        else:
            history = prof.get("history", {})
            days = 1 if window == "day" else 7 if window == "week" else 30
            val = _sum_window(history, days, today)
        rows.append((u, val))
    rows.sort(key=lambda x: x[1], reverse=True)
    top = [{"user": u, "value": v} for u, v in rows[:50]]
    return jsonify({"ok": True, "window": window, "top": top})


def record_added_for_request_user(added: int):
    u = session.get("user")
    if u and added > 0:
        record_added_for_user(u, added)
