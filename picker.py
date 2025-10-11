# -*- coding: utf-8 -*-
from __future__ import annotations
import csv, re, hashlib, os, time, glob, gc, re as _re
from io import BytesIO
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from threading import Lock, BoundedSemaphore

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from PIL import Image, ImageOps, ImageFile

from flask import Blueprint, jsonify, request, render_template

try:
    from picker_profile import record_change_for_request_user
except Exception:
    def record_change_for_request_user(*_, **__):
        pass

# Pillow: не падать на обрезанных файлах
ImageFile.LOAD_TRUNCATED_IMAGES = True
try:
    # жёсткая защита от гигантских изображений (без ENV)
    Image.MAX_IMAGE_PIXELS = 30_000_000
except Exception:
    pass

# ===================== Константы/пути =====================
DATASET_OUT_DIR = Path("dataset_collect")
DATASET_OUT_DIR.mkdir(parents=True, exist_ok=True)
TMP_DIR = DATASET_OUT_DIR / "_tmp"  # для потоковых загрузок
TMP_DIR.mkdir(parents=True, exist_ok=True)

UA = "PlantPicker/1.3 (python-requests)"
GBIF_SPECIES_API = "https://api.gbif.org/v1/species/{key}"
INAT_TAXA_API = "https://api.inaturalist.org/v1/taxa"
INAT_OBS_API = "https://api.inaturalist.org/v1/observations"

# --------- ЖЁСТКО ЗАДАННЫЕ ЛИМИТЫ (без окружения) ----------
COLLECT_CONCURRENCY = 1  # одновременно /collect/sync
WEBP_CONCURRENCY = 1  # одновременно конверсий WebP
WEBP_METHOD = 3  # быстрее и экономнее CPU, чем 6
WEBP_QUALITY = 80  # компромисс качество/размер
MAX_WEBP_SIDE = 1400  # даунскейл по длинной стороне
MEM_MIN_FREE_MB = 300  # если свободно меньше — ждём
MEM_CHECK_EVERY = 5  # каждые N изображений — GC

_collect_sem = BoundedSemaphore(COLLECT_CONCURRENCY)
_webp_sem = BoundedSemaphore(WEBP_CONCURRENCY)

# ===================== Flask Blueprints ====================
picker_page_bp = Blueprint("picker_page_bp", __name__, template_folder="templates")
picker_api_bp = Blueprint("picker_api_bp", __name__)

DEBUG_PP = False


def _log(*a, **k):
    if DEBUG_PP:
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"[PP {ts}]", *a, *[f"{x}={y}" for x, y in k.items()], flush=True)


def init_picker(app):
    DATASET_OUT_DIR.mkdir(parents=True, exist_ok=True)


# ===================== Страница ============================
@picker_page_bp.route("/picker")
def picker_page():
    return render_template("picker.html")


# ===================== HTTP сессии (ротация/сброс) =========
SESSION_LIMIT = 50
_session_lock: Lock = Lock()
_SESSION: Optional[requests.Session] = None
_SESSION_USES: int = 0


def _build_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )
    # умеренные пулы: не бурстим сетью и не плодим объекты
    adapter = HTTPAdapter(max_retries=retries, pool_connections=8, pool_maxsize=16)
    s.mount("https://", adapter)
    s.headers.update({"User-Agent": UA})
    return s


def _get_session() -> requests.Session:
    global _SESSION, _SESSION_USES
    with _session_lock:
        if _SESSION is None or _SESSION_USES >= SESSION_LIMIT:
            _SESSION = _build_session()
            _SESSION_USES = 0
        _SESSION_USES += 1
        return _SESSION


def _reset_http_session():
    global _SESSION, _SESSION_USES
    with _session_lock:
        _SESSION = None
        _SESSION_USES = 0


# ===================== RAM guard ===========================
def _mem_available_mb() -> int:
    """Читаем свободную память из /proc/meminfo (Linux)."""
    try:
        with open("/proc/meminfo", "r") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    parts = line.split()
                    return int(parts[1]) // 1024  # kB -> MB
    except Exception:
        pass
    return 1024  # если непонятно — считаем, что достаточно


def _wait_mem():
    """Если свободно мало RAM — ждём и пробуем ещё."""
    tries = 0
    while _mem_available_mb() < MEM_MIN_FREE_MB and tries < 240:  # до ~60 сек при 250ms шагах
        gc.collect()
        time.sleep(0.25)
        tries += 1


# ===================== HTTP helpers ========================
def http_json(url: str, params: Optional[dict] = None, timeout: int = 60) -> dict:
    s = _get_session()
    r = s.get(url, params=params, timeout=timeout)
    try:
        r.raise_for_status()
    except requests.HTTPError:
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "1") or 1)
            time.sleep(max(1, wait))
            s = _get_session()
            r = s.get(url, params=params, timeout=timeout)
            r.raise_for_status()
        else:
            raise
    return r.json()


def http_download_to_tmp(url: str, stem: str) -> Optional[Path]:
    """Скачиваем в файл потоком, без хранения всего в памяти. Возвращаем путь или None."""
    try:
        s = _get_session()
        with s.get(url, timeout=30, stream=True) as r:
            r.raise_for_status()
            tmp = TMP_DIR / f"{stem}_{int(time.time() * 1000)}.bin"
            with open(tmp, "wb") as out:
                for chunk in r.iter_content(chunk_size=64 * 1024):
                    if chunk:
                        out.write(chunk)
            return tmp
    except Exception:
        return None


# ===================== Внешние API =========================
def gbif_to_latin(gbif_id: str) -> str:
    js = http_json(GBIF_SPECIES_API.format(key=gbif_id), timeout=30)
    latin = js.get("canonicalName") or js.get("scientificName")
    if not latin:
        raise ValueError("GBIF did not return canonical name")
    return latin


def inat_taxon_by_id(tid: int) -> Optional[dict]:
    js = http_json(f"{INAT_TAXA_API}/{tid}", timeout=30)
    res = js.get("results", [])
    return res[0] if res else None


def inat_taxon_by_query(q: str) -> Optional[dict]:
    js = http_json(INAT_TAXA_API, params={"q": q, "rank": "species", "per_page": 1}, timeout=30)
    res = js.get("results", [])
    return res[0] if res else None


def _resolve_species_dir(taxon_id: int) -> Optional[Path]:
    pat = str(DATASET_OUT_DIR / f"{taxon_id}__*")
    hits = sorted(glob.glob(pat))
    return Path(hits[0]) if hits else None


def best_photo_urls(ph: dict) -> Tuple[str, str, Optional[int], Optional[int]]:
    thumb = ph.get("url") or ""
    orig = ph.get("original_url") or thumb

    def up(u: str) -> str:
        return re.sub(r"/(square|thumb|small|medium)\.", "/large.", u)

    if thumb and any(x in thumb for x in ("/square.", "/thumb.", "/small.", "/medium.")):
        thumb = re.sub(r"/(square|thumb|small|medium)\.", "/small.", thumb)
    if orig:
        orig = up(orig)
    dims = ph.get("original_dimensions") or {}
    return thumb, orig, dims.get("width"), dims.get("height")


def md5_bytes(b: bytes) -> str:
    h = hashlib.md5();
    h.update(b);
    return h.hexdigest()


def slugify_latin(name: str) -> str:
    s = name.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "species"


# -------- WEBP --------
def file_to_webp_bytes(src: Path) -> bytes:
    """Открываем файл, даунскейлим, сохраняем в WEBP (в память уже сжатым)."""
    with _webp_sem:  # лимитируем параллелизм
        _wait_mem()  # дождёмся свободной RAM
        with Image.open(src) as im:
            im = ImageOps.exif_transpose(im)
            try:
                im.draft("RGB", (MAX_WEBP_SIDE * 2, MAX_WEBP_SIDE * 2))
            except Exception:
                pass
            if im.mode not in ("RGB", "RGBA"):
                im = im.convert("RGB")
            # даунскейл по длинной стороне
            im.thumbnail((MAX_WEBP_SIDE, MAX_WEBP_SIDE))
            buf = BytesIO()
            im.save(buf, format="WEBP", quality=WEBP_QUALITY, method=WEBP_METHOD)
            return buf.getvalue()


def convert_file_to_webp(path: Path) -> Tuple[Path, str]:
    """Оставлено для совместимости — но не пишем рядом. Не используется в основном потоке."""
    try:
        webp = file_to_webp_bytes(path)
        new_path = path.with_suffix(".webp")
        with open(new_path, "wb") as out:
            out.write(webp)
        try:
            os.remove(path)
        except OSError:
            pass
        return new_path, md5_bytes(webp)
    except Exception:
        return path, ""


def _next_image_index(images_dir: Path) -> int:
    mx = -1
    pat = _re.compile(r"^(\d{6})_")
    for p in images_dir.glob("*.webp"):
        m = pat.match(p.name)
        if m:
            try:
                mx = max(mx, int(m.group(1)))
            except ValueError:
                pass
    return (mx + 1) if mx >= 0 else 0


# ===================== CSV helpers ========================
def read_selected_csv(csv_path: Path) -> Dict[str, Dict[str, str]]:
    data: Dict[str, Dict[str, str]] = {}
    if not csv_path.exists():
        return data

    def _read(enc: str):
        with csv_path.open("r", newline="", encoding=enc, errors="strict" if enc == "utf-8-sig" else "ignore") as f:
            rd = csv.DictReader(f)
            for raw in rd:
                row = {(k or "").strip().lstrip("\ufeff").lower(): (v or "") for k, v in raw.items()}
                pid = row.get("photo_id")
                if pid:
                    data[pid] = row

    try:
        _read("utf-8-sig")
    except UnicodeDecodeError:
        _read("cp1251")
    return data


def write_selected_csv(csv_path: Path, rows: List[Dict[str, str]]):
    header = ["taxon_id", "latin", "gbif_id", "photo_id", "observation_id", "license", "attribution", "best_url",
              "local_path", "md5", "width", "height", "observed_on", "time_observed_at", "user_login",
              "place_guess", "quality_grade", "saved_at"]
    tmp = csv_path.with_suffix(".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=header);
        wr.writeheader()
        for r in rows: wr.writerow(r)
    tmp.replace(csv_path)


# ===================== API: прочитать выбранные ===========
@picker_api_bp.get("/collect/selected")
def api_collect_selected():
    taxon_id = request.args.get("taxon_id", type=int)
    if not taxon_id:
        return jsonify({"ok": False, "error": "taxon_id required"}), 400
    root = _resolve_species_dir(taxon_id)
    if not root:
        return jsonify({"ok": True, "items": []})
    csv_path = root / "selected.csv"
    if not csv_path.exists():
        return jsonify({"ok": True, "items": []})
    rows = []
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        for r in rd:
            rows.append({
                "photo_id": r.get("photo_id") or "",
                "observation_id": r.get("observation_id") or "",
                "best_url": r.get("best_url") or "",
                "width": r.get("width") or "",
                "height": r.get("height") or "",
                "license": r.get("license") or "",
                "attribution": r.get("attribution") or "",
                "observed_on": r.get("observed_on") or "",
                "time_observed_at": r.get("time_observed_at") or "",
                "user_login": r.get("user_login") or "",
                "place_guess": r.get("place_guess") or "",
                "quality_grade": r.get("quality_grade") or "",
            })
    return jsonify({"ok": True, "items": rows})


# ===================== API: resolve =======================
@picker_api_bp.get("/resolve_taxon")
def api_resolve_taxon():
    q = (request.args.get("q") or "").strip()
    if not q:
        return jsonify({"ok": False, "error": "empty query"}), 400
    inat = None
    gbif_id = ""
    m = re.search(r"/species/(\d+)", q)
    try:
        if m:
            gbif_id = m.group(1)
            latin = gbif_to_latin(gbif_id)
            inat = inat_taxon_by_query(latin)
        else:
            if re.fullmatch(r"\d+", q):
                inat = inat_taxon_by_id(int(q))
                if not inat:
                    try:
                        latin = gbif_to_latin(q)
                        gbif_id = q
                        inat = inat_taxon_by_query(latin)
                    except Exception:
                        pass
            else:
                inat = inat_taxon_by_query(q)
    except Exception:
        inat = None
    if not inat:
        return jsonify({"ok": False, "error": "taxon not found"}), 404

    latin = inat.get("name") or inat.get("preferred_common_name") or ""
    common_en = inat.get("english_common_name") or inat.get("preferred_common_name") or ""
    common_ru = ""
    for nm in inat.get("names", []) or []:
        if (nm.get("lexicon") or "").lower() in ("russian", "ru") and nm.get("name"):
            common_ru = nm["name"];
            break
    return jsonify({
        "ok": True,
        "inat_taxon_id": int(inat["id"]),
        "latin": latin,
        "common_en": common_en,
        "common_ru": common_ru,
        "gbif_id": gbif_id,
    })


# ===================== API: список фото ===================
@picker_api_bp.get("/inat/photos")
def api_inat_photos():
    taxon_id = request.args.get("taxon_id", type=int)
    if not taxon_id:
        return jsonify({"ok": False, "error": "taxon_id required"}), 400
    page = max(1, request.args.get("page", default=1, type=int))
    per_page = max(1, min(50, request.args.get("per_page", default=10, type=int)))
    sort = request.args.get("sort", default="faves", type=str)
    licenses = "cc0,cc-by,cc-by-nc".split(",")
    params = {
        "taxon_id": taxon_id,
        "photos": "true",
        "order": "desc",
        "order_by": "faves" if sort == "faves" else "created_at",
        "per_page": per_page,
        "page": page,
        "quality_grade": "research,needs_id",
    }
    js = http_json(INAT_OBS_API, params=params, timeout=60)
    total = js.get("total_results", 0)
    items: List[Dict[str, Any]] = []
    for obs in js.get("results", []):
        photos = obs.get("photos") or []
        if not photos: continue
        ph = photos[0]
        lic = (ph.get("license_code") or "").lower()
        if lic and licenses and lic not in licenses:
            continue
        turl, burl, w, h = best_photo_urls(ph)
        if not burl: continue
        items.append({
            "photo_id": ph.get("id"),
            "observation_id": obs.get("id"),
            "thumb_url": turl,
            "best_url": burl,
            "width": w or "",
            "height": h or "",
            "license": lic,
            "attribution": ph.get("attribution") or "",
            "observed_on": obs.get("observed_on") or "",
            "time_observed_at": obs.get("time_observed_at") or "",
            "user_login": (obs.get("user") or {}).get("login") or "",
            "place_guess": obs.get("place_guess") or "",
            "quality_grade": obs.get("quality_grade") or "",
        })
    return jsonify({"ok": True, "page": page, "per_page": per_page, "total": total, "items": items})


# ===================== API: sync выбранных =================
@picker_api_bp.post("/collect/sync")
def api_collect_sync():
    """
    Истина — текущий список selected с фронта.
    • Удалённые: запись удаляется, файл пытаемся удалить (если не вышло — ок).
    • Добавленные: запись ВСЕГДА попадает в CSV; если скачать/сконвертировать не удалось — local_path="".
    • Оставленные: если файла нет/пустой путь/не webp — пытаемся восстановить.
    Никогда не шлём 500.
    """
    acquired = _collect_sem.acquire(timeout=300)  # ограничиваем одновременные sync-запросы
    if not acquired:
        return jsonify({"ok": False, "error": "server busy, try again later"}), 503
    try:
        try:
            js = request.get_json(silent=True) or {}
            _reset_http_session()

            taxon_id = js.get("taxon_id")
            latin = (js.get("latin") or "").strip() or "species"
            gbif_id = js.get("gbif_id") or ""
            common_en = js.get("common_en") or ""
            common_ru = js.get("common_ru") or ""
            selected = js.get("selected") or []
            if not taxon_id:
                return jsonify({"ok": False, "error": "taxon_id required"}), 200

            latin_slug = slugify_latin(latin)
            root = DATASET_OUT_DIR / f"{taxon_id}__{latin_slug}"
            images_dir = root / "images"
            images_dir.mkdir(parents=True, exist_ok=True)
            csv_path = root / "selected.csv"
            species_csv = root / "species.csv"

            # species.csv
            with species_csv.open("w", newline="", encoding="utf-8") as fsp:
                wrs = csv.DictWriter(fsp,
                                     fieldnames=["taxon_id", "latin", "common_en", "common_ru", "gbif_id",
                                                 "updated_at"])
                wrs.writeheader()
                wrs.writerow({
                    "taxon_id": taxon_id, "latin": latin, "common_en": common_en, "common_ru": common_ru,
                    "gbif_id": gbif_id, "updated_at": datetime.utcnow().isoformat()
                })

            # существующее
            existing = read_selected_csv(csv_path)
            existing_ids = set(existing.keys())

            # нормализуем приходящий список выбранных
            selected_map: Dict[str, Dict[str, Any]] = {}
            for it in selected if isinstance(selected, list) else []:
                pid = str(it.get("photo_id") or "").strip()
                if not pid: continue
                selected_map[pid] = {
                    "photo_id": pid,
                    "observation_id": str(it.get("observation_id") or ""),
                    "best_url": str(it.get("best_url") or ""),
                    "width": str(it.get("width") or ""),
                    "height": str(it.get("height") or ""),
                    "license": str(it.get("license") or ""),
                    "attribution": str(it.get("attribution") or ""),
                    "observed_on": str(it.get("observed_on") or ""),
                    "time_observed_at": str(it.get("time_observed_at") or ""),
                    "user_login": str(it.get("user_login") or ""),
                    "place_guess": str(it.get("place_guess") or ""),
                    "quality_grade": str(it.get("quality_grade") or "")
                }

            selected_ids = set(selected_map.keys())
            to_remove = existing_ids - selected_ids
            to_add = selected_ids - existing_ids
            kept_ids = selected_ids & existing_ids

            # remove
            for pid in sorted(to_remove):
                row = existing.get(pid) or {}
                lp = (row.get("local_path") or "").strip()
                if lp:
                    try:
                        os.remove(lp)
                    except OSError:
                        pass  # файл уже отсутствует — ок

            idx = _next_image_index(images_dir)
            new_rows: List[Dict[str, str]] = []
            failed_ids: List[str] = []
            processed = 0

            # keep + fix
            for pid in sorted(kept_ids):
                base = dict(existing.get(pid) or {})
                meta = selected_map.get(pid) or {}
                base.update(meta)

                lp = (base.get("local_path") or "").strip()
                p = Path(lp) if lp else None
                need = (not lp) or (p and not p.exists())

                if not need and p and p.exists() and p.suffix.lower() != ".webp":
                    new_p, new_md5 = convert_file_to_webp(p)
                    base["local_path"] = new_p.as_posix()
                    base["md5"] = new_md5 or base.get("md5", "")

                if need:
                    url = (base.get("best_url") or "").strip()
                    if url:
                        tmp = http_download_to_tmp(url, f"{pid}")
                        if tmp and tmp.exists():
                            try:
                                webp = file_to_webp_bytes(tmp)
                                fname = f"{idx:06d}_{pid}.webp";
                                idx += 1
                                outp = images_dir / fname
                                with open(outp, "wb") as fw:
                                    fw.write(webp)
                                base["local_path"] = outp.as_posix()
                                base["md5"] = md5_bytes(webp)
                                base["saved_at"] = datetime.utcnow().isoformat()
                            except Exception:
                                base["local_path"] = "";
                                base["md5"] = "";
                                failed_ids.append(pid)
                            finally:
                                try:
                                    os.remove(tmp)
                                except Exception:
                                    pass
                        else:
                            base["local_path"] = "";
                            base["md5"] = "";
                            failed_ids.append(pid)
                    else:
                        base["local_path"] = "";
                        base["md5"] = "";
                        failed_ids.append(pid)

                new_rows.append({
                    "taxon_id": str(taxon_id), "latin": latin, "gbif_id": gbif_id,
                    "photo_id": pid, "observation_id": base.get("observation_id", ""),
                    "license": base.get("license", ""), "attribution": base.get("attribution", ""),
                    "best_url": base.get("best_url", ""),
                    "local_path": base.get("local_path", ""), "md5": base.get("md5", ""),
                    "width": base.get("width", ""), "height": base.get("height", ""),
                    "observed_on": base.get("observed_on", ""), "time_observed_at": base.get("time_observed_at", ""),
                    "user_login": base.get("user_login", ""), "place_guess": base.get("place_guess", ""),
                    "quality_grade": base.get("quality_grade", ""), "saved_at": base.get("saved_at", "")
                })

                processed += 1
                if processed % MEM_CHECK_EVERY == 0:
                    gc.collect()

            # add (row ALWAYS written)
            for pid in sorted(to_add):
                it = selected_map[pid]
                url = (it.get("best_url") or "").strip()
                lp = "";
                md5 = ""

                if url:
                    tmp = http_download_to_tmp(url, f"{pid}")
                    if tmp and tmp.exists():
                        try:
                            webp = file_to_webp_bytes(tmp)
                            fname = f"{idx:06d}_{pid}.webp";
                            idx += 1
                            outp = images_dir / fname
                            with open(outp, "wb") as fw:
                                fw.write(webp)
                            lp = outp.as_posix();
                            md5 = md5_bytes(webp)
                        except Exception:
                            failed_ids.append(pid)
                        finally:
                            try:
                                os.remove(tmp)
                            except Exception:
                                pass
                    else:
                        failed_ids.append(pid)
                else:
                    failed_ids.append(pid)

                new_rows.append({
                    "taxon_id": str(taxon_id), "latin": latin, "gbif_id": gbif_id,
                    "photo_id": pid, "observation_id": it.get("observation_id") or "",
                    "license": it.get("license") or "", "attribution": it.get("attribution") or "",
                    "best_url": url, "local_path": lp, "md5": md5,
                    "width": it.get("width") or "", "height": it.get("height") or "",
                    "observed_on": it.get("observed_on") or "", "time_observed_at": it.get("time_observed_at") or "",
                    "user_login": it.get("user_login") or "", "place_guess": it.get("place_guess") or "",
                    "quality_grade": it.get("quality_grade") or "",
                    "saved_at": datetime.utcnow().isoformat() if lp else ""
                })

                processed += 1
                if processed % MEM_CHECK_EVERY == 0:
                    gc.collect()

            new_rows.sort(key=lambda r: r.get("photo_id", ""))
            write_selected_csv(csv_path, new_rows)

            ok_flag = (len(failed_ids) == 0)
            try:
                record_change_for_request_user(added=len(to_add), removed=len(to_remove))
            except Exception:
                pass

            return jsonify({
                "ok": ok_flag,
                "dir": root.as_posix(),
                "csv": csv_path.as_posix(),
                "species_csv": species_csv.as_posix(),
                "added": len(to_add),
                "removed": len(to_remove),
                "kept": len(kept_ids),
                "selected_total": len(selected_ids),
                "failed_ids": failed_ids
            }), 200

        except Exception as e:
            return jsonify({"ok": False, "error": f"sync failed: {e.__class__.__name__}: {e}"}), 200
    finally:
        _collect_sem.release()


# совместимость
@picker_api_bp.post("/collect/save")
def api_collect_save_compat():
    return api_collect_sync()


# ===================== Maintenance =========================
@picker_api_bp.post("/maintenance/flush")
def api_maintenance_flush():
    _reset_http_session()
    # подчистим tmp (на всякий случай)
    try:
        for p in TMP_DIR.glob("*"):
            try:
                os.remove(p)
            except Exception:
                pass
    except Exception:
        pass
    gc.collect()
    return jsonify({"ok": True})


# ===================== No-cache для /api/* =================
@picker_api_bp.after_app_request
def _api_no_cache(resp):
    if request.path.startswith("/api/"):
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
    return resp
