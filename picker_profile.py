from __future__ import annotations
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, Tuple

from flask import Blueprint, jsonify, request, session, current_app

profile_bp = Blueprint("profile_bp", __name__)

AUTH_DIR = Path("auth")
USERS_PATH = AUTH_DIR / "users.json"  # {"user":{"password":"plain"}}
PROFILES_PATH = AUTH_DIR / "profiles.json"  # создаётся/обновляется автоматически
AUTH_DIR.mkdir(parents=True, exist_ok=True)

GOAL_PER_DAY = 1

MSK_TZ = timezone(timedelta(hours=3))


def _now_msk() -> datetime:
    return datetime.now(MSK_TZ)


def _today_key(dt: datetime | None = None) -> str:
    # ДЕНЬ начинается в 00:00 по Москве
    dt = dt or _now_msk()
    return dt.date().isoformat()


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


def _ensure_profile(profiles: Dict[str, Any], user: str) -> Dict[str, Any]:
    prof = profiles.get(user)
    if not prof:
        prof = {"history": {}, "total": 0, "streak_days": 0, "streak_last": ""}  # total мы будем пересчитывать
        profiles[user] = prof
    return prof


def _sum_window(history: Dict[str, int], days: int, today: str) -> int:
    base = datetime.fromisoformat(today)
    s = 0
    for i in range(days):
        key = (base - timedelta(days=i)).date().isoformat()
        s += int(history.get(key, 0))
    return s


def _recompute_total(history: Dict[str, int]) -> int:
    return sum(int(v) for v in history.values())


def _recompute_streak(history: Dict[str, int], today_key: str) -> Tuple[int, str]:
    """
    Считаем текущий стрик как количество подряд идущих дней,
    заканчивающихся СЕГОДНЯ, в каждом из которых count >= GOAL_PER_DAY.
    Если сегодня < GOAL_PER_DAY — текущий стрик = 0.
    Возвращаем (streak_days, streak_last).
    """
    today_count = int(history.get(today_key, 0))
    if today_count < GOAL_PER_DAY:
        # сегодня не выполнено — стрика нет
        # streak_last оставим как последний день, в который было выполнено (если надо — вычислим)
        # найдём последний >= GOAL_PER_DAY для метаданных
        d = datetime.fromisoformat(today_key)
        last_ok = ""
        for i in range(3650):  # ограничим поиск 10 годами
            key = (d - timedelta(days=i)).date().isoformat()
            if int(history.get(key, 0)) >= GOAL_PER_DAY:
                last_ok = key
                break
        return 0, last_ok
    # сегодня выполнено — считаем длину подряд
    streak = 0
    d = datetime.fromisoformat(today_key)
    while True:
        key = d.date().isoformat()
        if int(history.get(key, 0)) >= GOAL_PER_DAY:
            streak += 1
            d = d - timedelta(days=1)
        else:
            break
    return streak, today_key


def record_change_for_user(username: str, added: int = 0, removed: int = 0):
    """
    Обновляет статистику:
      - history[today] += added - removed (не ниже 0)
      - total = sum(history.values()) (может уменьшаться)
      - streak_days пересчитывается по правилу >= GOAL_PER_DAY
    """
    if not username:
        return
    delta = int(added) - int(removed)
    profiles = _load_profiles()
    prof = _ensure_profile(profiles, username)
    today = _today_key()

    day_val = int(prof["history"].get(today, 0)) + delta
    if day_val < 0:
        day_val = 0
    prof["history"][today] = day_val

    prof["total"] = _recompute_total(prof["history"])
    streak_days, streak_last = _recompute_streak(prof["history"], today)
    prof["streak_days"] = streak_days
    prof["streak_last"] = streak_last

    _save_profiles(profiles)


def _sum_current_week_msk(history: Dict[str, int], today_key: str) -> int:
    """
    Текущая неделя по МСК: с субботы 00:00 до сегодняшнего дня включительно.
    (Порог смены недели — в субботу)
    """
    today_dt = datetime.fromisoformat(today_key)
    # Mon=0 ... Sat=5, Sun=6 -> сколько дней прошло с субботы
    days_since_sat = (today_dt.weekday() - 5) % 7
    start_dt = today_dt - timedelta(days=days_since_sat)
    s = 0
    d = start_dt
    while d.date() <= today_dt.date():
        s += int(history.get(d.date().isoformat(), 0))
        d += timedelta(days=1)
    return s


def _stats_payload(username: str) -> Dict[str, Any]:
    profiles = _load_profiles()
    prof = _ensure_profile(profiles, username)
    today = _today_key()
    day = int(prof["history"].get(today, 0))
    week = _sum_current_week_msk(prof["history"], today)
    total = int(prof.get("total", _recompute_total(prof["history"])))
    streak_days, streak_last = _recompute_streak(prof["history"], today)
    return {
        "day": day, "week": week, "month": 0, "total": total,
        "streak_days": streak_days, "today_done": day >= GOAL_PER_DAY
    }


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
    if not rec or (rec.get("password") or "") != password:
        return jsonify({"ok": False, "error": "invalid credentials"}), 200
    session["user"] = username
    session.permanent = True
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
        history = prof.get("history", {})
        if window == "total":
            val = sum(int(v) for v in history.values())
        elif window == "week":
            val = _sum_current_week_msk(history, today)
        else:
            days = 1 if window == "day" else 30
            val = _sum_window(history, days, today)
        rows.append((u, val))
    rows.sort(key=lambda x: x[1], reverse=True)
    top = [{"user": u, "value": v} for u, v in rows[:50]]
    return jsonify({"ok": True, "window": window, "top": top})


def record_change_for_request_user(added: int = 0, removed: int = 0):
    u = session.get("user")
    if u:
        record_change_for_user(u, added=added, removed=removed)
