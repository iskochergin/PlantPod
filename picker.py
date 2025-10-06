# -*- coding: utf-8 -*-
from __future__ import annotations
import csv, re, hashlib, os
from io import BytesIO
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import glob

import requests
from PIL import Image, ImageOps  # <-- Pillow

from flask import Blueprint, jsonify, request, render_template

SESSION = requests.Session()

# === Папки вывода ===
DATASET_OUT_DIR = Path("dataset_collect")  # сюда падают выбранные
DATASET_OUT_DIR.mkdir(parents=True, exist_ok=True)

# === Внешние API ===
UA = "PlantPicker/1.3 (python-requests)"
GBIF_SPECIES_API = "https://api.gbif.org/v1/species/{key}"
INAT_TAXA_API = "https://api.inaturalist.org/v1/taxa"
INAT_OBS_API = "https://api.inaturalist.org/v1/observations"

# === Flask Blueprints ===
picker_page_bp = Blueprint("picker_page_bp", __name__, template_folder="templates")
picker_api_bp = Blueprint("picker_api_bp", __name__)


def init_picker(app):
    DATASET_OUT_DIR.mkdir(parents=True, exist_ok=True)


# -------------------- ROUTES (PAGE) --------------------
@picker_page_bp.route("/picker")
def picker_page():
    return render_template("picker.html")


# -------------------- HELPERS --------------------
def http_json(url: str, params: Optional[dict] = None, timeout: int = 60) -> dict:
    r = SESSION.get(url, params=params, timeout=timeout, headers={"User-Agent": UA})
    r.raise_for_status()
    return r.json()


def http_bytes(url: str, timeout: int = 60) -> Optional[bytes]:
    try:
        r = SESSION.get(url, timeout=timeout, headers={"User-Agent": UA})
        r.raise_for_status()
        return r.content
    except Exception:
        return None


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
    # dataset_collect/<taxon_id>__<latin_slug>/
    pat = str(DATASET_OUT_DIR / f"{taxon_id}__*")
    hits = sorted(glob.glob(pat))
    return Path(hits[0]) if hits else None


def best_photo_urls(ph: dict) -> Tuple[str, str, Optional[int], Optional[int]]:
    # (thumb_url, best_url, w, h)
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


# ---- WEBP tools ----
def bytes_to_webp(image_bytes: bytes, quality: int = 90, method: int = 6) -> bytes:
    """
    Конвертирует произвольный байтовый образ (jpeg/png/…) в WEBP-байты.
    Выправляет EXIF-ориентацию. Сохраняем с хорошим качеством/сжатием.
    """
    with Image.open(BytesIO(image_bytes)) as im:
        im = ImageOps.exif_transpose(im)
        if im.mode not in ("RGB", "RGBA"):
            im = im.convert("RGB")
        buf = BytesIO()
        im.save(buf, format="WEBP", quality=quality, method=method)
        return buf.getvalue()


def convert_file_to_webp(path: Path) -> Tuple[Path, str]:
    """
    Конвертирует существующий файл в .webp и удаляет исходник.
    Возвращает (новый_путь, md5_webp).
    """
    try:
        with path.open("rb") as f:
            raw = f.read()
        webp = bytes_to_webp(raw)
        new_path = path.with_suffix(".webp")
        with new_path.open("wb") as out:
            out.write(webp)
        try:
            os.remove(path)
        except OSError:
            pass
        return new_path, md5_bytes(webp)
    except Exception:
        # если что-то пошло не так — просто оставим как есть
        return path, ""


# -------------------- API: resolve --------------------
@picker_api_bp.get("/resolve_taxon")
def api_resolve_taxon():
    q = (request.args.get("q") or "").strip()
    if not q:
        return jsonify({"ok": False, "error": "empty query"}), 400

    inat = None
    gbif_id = ""

    m = re.search(r"/species/(\d+)", q)
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


# -------------------- API: paged photos --------------------
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
        if not photos:
            continue
        ph = photos[0]
        lic = (ph.get("license_code") or "").lower()
        if lic and licenses and lic not in licenses:
            continue
        turl, burl, w, h = best_photo_urls(ph)
        if not burl:
            continue
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


# -------------------- CSV helpers --------------------
def read_selected_csv(csv_path: Path) -> Dict[str, Dict[str, str]]:
    data: Dict[str, Dict[str, str]] = {}
    if not csv_path.exists():
        return data
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        for row in rd:
            pid = row.get("photo_id")
            if pid:
                data[pid] = row
    return data


def write_selected_csv(csv_path: Path, rows: List[Dict[str, str]]):
    header = [
        "taxon_id", "latin", "gbif_id", "photo_id", "observation_id",
        "license", "attribution", "best_url", "local_path", "md5", "width", "height",
        "observed_on", "time_observed_at", "user_login", "place_guess", "quality_grade",
        "saved_at"
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=header)
        wr.writeheader()
        for r in rows:
            wr.writerow(r)


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


# -------------------- API: sync picked (добавляет/удаляет, конвертирует в WEBP) --------------------
@picker_api_bp.post("/collect/sync")
def api_collect_sync():
    """
    JSON:
    {
      "taxon_id": 55971,
      "latin": "Tanacetum vulgare",
      "gbif_id": "3118274",
      "common_en": "...",
      "common_ru": "...",
      "selected": [ { photo_id, best_url, ... } ]   # РОВНО текущий набор выбранных
    }
    Приводит dataset_collect/<taxon>__<slug>/ к точному набору:
      - докачивает отсутствующие (сразу .webp)
      - удаляет снятое (файлы + строки CSV)
      - species.csv обновляется всегда
    """
    js = request.get_json(silent=True) or {}
    taxon_id = js.get("taxon_id")
    latin = (js.get("latin") or "").strip() or "species"
    gbif_id = js.get("gbif_id") or ""
    common_en = js.get("common_en") or ""
    common_ru = js.get("common_ru") or ""
    selected = js.get("selected") or []
    if not taxon_id:
        return jsonify({"ok": False, "error": "taxon_id required"}), 400

    latin_slug = slugify_latin(latin)
    root = DATASET_OUT_DIR / f"{taxon_id}__{latin_slug}"
    images_dir = root / "images"
    images_dir.mkdir(parents=True, exist_ok=True)
    csv_path = root / "selected.csv"
    species_csv = root / "species.csv"

    # species.csv — одна строка с описанием
    with species_csv.open("w", newline="", encoding="utf-8") as fsp:
        wrs = csv.DictWriter(fsp, fieldnames=["taxon_id", "latin", "common_en", "common_ru", "gbif_id", "updated_at"])
        wrs.writeheader()
        wrs.writerow({
            "taxon_id": taxon_id, "latin": latin, "common_en": common_en, "common_ru": common_ru,
            "gbif_id": gbif_id, "updated_at": datetime.utcnow().isoformat()
        })

    # существующее
    existing = read_selected_csv(csv_path)
    existing_ids = set(existing.keys())

    # ровно выбранное сейчас
    selected_map: Dict[str, Dict[str, Any]] = {}
    for it in selected:
        pid = str(it.get("photo_id"))
        if pid:
            # нормализуем на бэке для надёжности
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

    # удалить снятое
    removed = 0
    for pid in to_remove:
        row = existing.get(pid) or {}
        local_path = row.get("local_path")
        if local_path:
            try:
                os.remove(local_path)
            except OSError:
                pass
        removed += 1

    # оставить/конвертировать оставшееся, привести к .webp
    new_rows: List[Dict[str, str]] = []
    for pid in kept_ids:
        row = dict(existing[pid])
        lp = row.get("local_path") or ""
        p = Path(lp) if lp else None
        if p and p.exists() and p.suffix.lower() != ".webp":
            new_p, new_md5 = convert_file_to_webp(p)
            row["local_path"] = new_p.as_posix()
            if new_md5: row["md5"] = new_md5
        new_rows.append(row)

    # докачать недостающее — сразу WEBP
    added = 0
    start_idx = len(list(images_dir.glob("*.*")))
    idx = start_idx
    for pid in to_add:
        it = selected_map[pid]
        url = it.get("best_url")
        if not url:
            continue
        raw = http_bytes(url, timeout=60)
        if not raw:
            continue
        webp = bytes_to_webp(raw)
        fname = f"{idx:06d}_{pid}.webp"
        idx += 1
        outp = images_dir / fname
        with outp.open("wb") as fw:
            fw.write(webp)
        new_rows.append({
            "taxon_id": str(taxon_id),
            "latin": latin,
            "gbif_id": gbif_id,
            "photo_id": pid,
            "observation_id": it.get("observation_id") or "",
            "license": it.get("license") or "",
            "attribution": it.get("attribution") or "",
            "best_url": url,
            "local_path": outp.as_posix(),
            "md5": md5_bytes(webp),
            "width": it.get("width") or "",
            "height": it.get("height") or "",
            "observed_on": it.get("observed_on") or "",
            "time_observed_at": it.get("time_observed_at") or "",
            "user_login": it.get("user_login") or "",
            "place_guess": it.get("place_guess") or "",
            "quality_grade": it.get("quality_grade") or "",
            "saved_at": datetime.utcnow().isoformat()
        })
        added += 1

    new_rows.sort(key=lambda r: r.get("photo_id", ""))
    write_selected_csv(csv_path, new_rows)

    return jsonify({
        "ok": True,
        "dir": root.as_posix(),
        "csv": csv_path.as_posix(),
        "species_csv": species_csv.as_posix(),
        "added": added,
        "removed": removed,
        "kept": len(kept_ids),
        "selected_total": len(selected_ids)
    })


# совместимость
@picker_api_bp.post("/collect/save")
def api_collect_save_compat():
    return api_collect_sync()
