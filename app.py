import os, re, json, time, shutil, zipfile, threading, logging, tempfile, subprocess
from pathlib import Path
from flask import Flask, request, jsonify, render_template, send_from_directory, abort
from werkzeug.exceptions import RequestEntityTooLarge

# --- для прокси ---
import urllib.request
import urllib.parse
import urllib.error

from threed import (
    CFG, _load_port, UPLOAD_PASSWORD,
    DATA_DIR, UPLOADS_DIR, CACHE_DIR,
    ALLOWED_IMAGE_EXT, ALLOWED_MODEL_EXT, ORIGINAL_IMAGE_EXT, MAX_ZIP_MB, CLEAN_DELAY_SEC,
    safe_rel_path, safe_join_under,
    read_meta_title, write_meta,
    list_cached_webp, list_cached_webp_raw, resolve_leaf_rel, ensure_spin_cache,
    find_datasets,
    _safe_unlink, sweep_uploads, delete_originals_recursively, cleanup_empty_dirs,
    _start_background_sweeper, _leafs_under
)

# ---- Flask ----
BASE_DIR = Path(__file__).resolve().parent
app = Flask(__name__, static_folder=str(BASE_DIR / "static"),
            template_folder=str(BASE_DIR / "templates"))
app.config["MAX_CONTENT_LENGTH"] = MAX_ZIP_MB * 1024 * 1024


@app.errorhandler(RequestEntityTooLarge)
def handle_413(_e):
    return jsonify({"ok": False, "error": "file too large", "max_mb": MAX_ZIP_MB}), 413


# ---- API просмотра (существующее) ----
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/files/<path:subpath>")
def serve_from_data(subpath):
    try:
        rel = safe_rel_path(subpath)
        full = safe_join_under(DATA_DIR, rel)
    except Exception:
        abort(404)
    if not full.exists() or not full.is_file(): abort(404)
    return send_from_directory(full.parent, full.name)


@app.route("/spin-cache/<path:subpath>")
def serve_from_cache(subpath):
    try:
        rel = safe_rel_path(subpath)
        full = safe_join_under(CACHE_DIR, rel)
    except Exception:
        abort(404)
    if not full.exists() or not full.is_file(): abort(404)
    return send_from_directory(full.parent, full.name)


@app.route("/api/datasets")
def api_datasets():
    return jsonify(find_datasets())


def _numeric_from_url(u: str) -> tuple:
    name = Path(u).name
    stem = Path(name).stem
    if stem.isdigit(): return (0, int(stem), name.lower())
    m = re.search(r"\d+", stem)
    return (0, int(m.group(0)), name.lower()) if m else (1, name.lower())


@app.route("/api/spin/<path:dataset_rel>")
def api_spin(dataset_rel):
    rel = safe_rel_path(dataset_rel)
    data_node = safe_join_under(DATA_DIR, rel)

    max_w = int(request.args.get("w", CFG.get("spin_max_w", 1280)))
    max_frames = int(request.args.get("max", CFG.get("spin_max_frames", 90)))

    urls_rel = []
    if data_node.exists():
        leaf = resolve_leaf_rel(rel)
        urls_rel = list_cached_webp(leaf)
        if not urls_rel:
            urls_rel = ensure_spin_cache(leaf, max_w=max_w, max_frames=max_frames)
    else:
        # нет каталога датасета — читаем напрямую из кэша
        urls_rel = list_cached_webp_raw(rel)

    if urls_rel:
        urls = [f"/spin-cache/{rp}" for rp in urls_rel]
        urls.sort(key=_numeric_from_url)
        return jsonify(urls)

    return jsonify({"ok": False, "error": "no frames found"}), 404


# ---- Upload ZIP (существующее) ----
@app.route("/api/upload_zip", methods=["POST"])
def api_upload_zip():
    if request.form.get("password", "") != UPLOAD_PASSWORD:
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    if "zipfile" not in request.files:
        return jsonify({"ok": False, "error": "no file"}), 400

    file = request.files["zipfile"]
    if not file.filename.lower().endswith(".zip"):
        return jsonify({"ok": False, "error": "only .zip allowed"}), 400

    zip_stem = Path(file.filename).stem
    dataset_rel_raw = request.form.get("dataset_id") or zip_stem
    display_name = (request.form.get("display_name") or "").strip()

    try:
        dataset_rel = safe_rel_path(dataset_rel_raw)
        target_dir = safe_join_under(DATA_DIR, dataset_rel)
    except Exception:
        return jsonify({"ok": False, "error": "bad dataset path"}), 400

    up_path = UPLOADS_DIR / f"{dataset_rel.name}_{int(time.time())}.zip"
    try:
        file.save(str(up_path))
    except RequestEntityTooLarge:
        return jsonify({"ok": False, "error": "file too large", "max_mb": MAX_ZIP_MB}), 413

    target_dir.mkdir(parents=True, exist_ok=True)

    try:
        # 1) анализируем архив: если один верхний каталог — флэттим его
        with zipfile.ZipFile(str(up_path), "r") as zf:
            top_levels = set()
            file_members = []
            for m in zf.infolist():
                if m.is_dir(): continue
                parts = [pp for pp in Path(m.filename).parts if pp not in ("", ".", "..")]
                if not parts: continue
                top_levels.add(parts[0])
                file_members.append((m, parts))
            strip_depth = 1 if len(top_levels) == 1 else 0

            # 2) распаковка (с учётом strip_depth)
            for m, parts in file_members:
                rel_path = Path(*parts[strip_depth:])
                if not rel_path.parts:
                    continue
                ext = rel_path.suffix.lower()
                if ext in (ALLOWED_IMAGE_EXT | ALLOWED_MODEL_EXT):
                    out = safe_join_under(DATA_DIR, dataset_rel / rel_path)
                    out.parent.mkdir(parents=True, exist_ok=True)
                    with zf.open(m) as src, open(out, "wb") as dst:
                        shutil.copyfileobj(src, dst)

        write_meta(target_dir, display_name)

        # 3) чистим старый кэш под веткой
        try:
            cache_sub = safe_join_under(CACHE_DIR, dataset_rel)
            if cache_sub.exists(): shutil.rmtree(cache_sub)
        except Exception:
            pass

        # 4) строим webp для каждого ЛИСТА
        spin_max_w = int(CFG.get("spin_max_w", 1280))
        spin_max_frames = int(CFG.get("spin_max_frames", 90))

        leafs = _leafs_under(target_dir)
        if not leafs:
            leafs = [resolve_leaf_rel(dataset_rel)]

        built_for = []
        for rel in leafs:
            urls_rel = ensure_spin_cache(rel, max_w=spin_max_w, max_frames=spin_max_frames)
            if urls_rel:
                built_for.append(rel.as_posix())
                abs_leaf = safe_join_under(DATA_DIR, rel)
                delete_originals_recursively(abs_leaf)
                # не удаляем корень набора; чистим только пустые вложенные
                cleanup_empty_dirs(abs_leaf, stop_at=target_dir)

        _safe_unlink(up_path)  # zip удаляем после распаковки

        return jsonify({
            "ok": True,
            "dataset_id": dataset_rel.as_posix(),
            "display_name": read_meta_title(target_dir, target_dir.name),
            "optimized": bool(built_for),
            "built_for": built_for
        })

    except zipfile.BadZipFile:
        _safe_unlink(up_path)
        return jsonify({"ok": False, "error": "bad zip"}), 400
    except Exception as e:
        _safe_unlink(up_path)
        app.logger.exception("upload failed: %s", e)
        return jsonify({"ok": False, "error": f"upload failed: {e}"}), 500


@app.route("/api/delete_dataset", methods=["POST"])
def api_delete_dataset():
    pwd = request.form.get("password", "")
    ds = request.form.get("dataset_id", "")
    if pwd != UPLOAD_PASSWORD:
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    try:
        rel = safe_rel_path(ds)
        target = safe_join_under(DATA_DIR, rel)
        # удаляем и кэш даже если каталога данных нет
        try:
            shutil.rmtree(target)
        except Exception:
            pass
        try:
            shutil.rmtree(safe_join_under(CACHE_DIR, rel))
        except Exception:
            pass
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


# =========================
# Plant Picker (новое)
# =========================

# --- страница ---
@app.route("/picker")
def picker_page():
    return render_template("picker.html")


# --- прокси к iNaturalist/GBIF/Wikidata с троттлингом 1 req/s и бэкоффом 429 ---

INAT_BASE = "https://api.inaturalist.org/v1"
UA = CFG.get("plant_picker_ua") or "PlantPicker/1.0 (+contact@yourdomain)"
_last_request_ts = 0.0
_lock = threading.Lock()


def _throttle():
    global _last_request_ts
    with _lock:
        now = time.time()
        dt = now - _last_request_ts
        if dt < 1.10:
            time.sleep(1.10 - dt)
        _last_request_ts = time.time()


def _get_json(url: str, params: dict | None = None, headers: dict | None = None, retries: int = 4):
    if params:
        qs = urllib.parse.urlencode(params, doseq=True)
        url = f"{url}?{qs}"
    while True:
        _throttle()
        req = urllib.request.Request(url, headers={
            "User-Agent": UA,
            "Accept": "application/json",
            **(headers or {})
        })
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                status = resp.status
                body = resp.read()
                if status == 200:
                    return json.loads(body.decode("utf-8", errors="ignore"))
                if status == 429 and retries > 0:
                    ra = resp.headers.get("Retry-After")
                    delay = min(int(ra or 2), 10)
                    time.sleep(delay)
                    retries -= 1
                    continue
                if 500 <= status < 600 and retries > 0:
                    time.sleep(1.5)
                    retries -= 1
                    continue
                raise RuntimeError(f"HTTP {status}")
        except urllib.error.HTTPError as e:
            if e.code == 429 and retries > 0:
                ra = e.headers.get("Retry-After")
                delay = min(int(ra or 2), 10)
                time.sleep(delay)
                retries -= 1
                continue
            if 500 <= e.code < 600 and retries > 0:
                time.sleep(1.5)
                retries -= 1
                continue
            raise
        except Exception as e:
            if retries > 0:
                time.sleep(1.0)
                retries -= 1
                continue
            raise


@app.route("/api/inat/observations")
def inat_observations():
    allow = {"taxon_id", "place_id", "quality_grade", "verifiable", "photo_license", "per_page", "page", "order_by",
             "order", "id_above", "id_below", "locale", "geo"}
    q = {}
    for k, v in request.args.items():
        if k in allow and v is not None:
            q[k] = v
    q.setdefault("per_page", "200")
    q.setdefault("geo", "true")
    # нормализуем список лицензий к верхнему регистру
    if "photo_license" in q:
        q["photo_license"] = ",".join([s.strip().upper().replace("CC-", "CC-").replace("CC0", "CC0")
                                       for s in q["photo_license"].split(",") if s.strip()])
    url = f"{INAT_BASE}/observations"
    try:
        data = _get_json(url, q)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 502


@app.route("/api/inat/taxa")
def inat_taxa():
    allow = {"q", "rank", "is_active", "per_page", "page"}
    q = {}
    for k, v in request.args.items():
        if k in allow and v is not None:
            q[k] = v
    q.setdefault("per_page", "30")
    url = f"{INAT_BASE}/taxa"
    try:
        data = _get_json(url, q)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 502


@app.route("/api/latin2common")
def latin2common():
    name = (request.args.get("name") or "").strip()
    if not name:
        return jsonify({"en": [], "ru": []})
    en, ru = set(), set()

    # GBIF match
    try:
        gbif_m = _get_json("https://api.gbif.org/v1/species/match", {"name": name})
        key = gbif_m.get("usageKey")
        if key:
            v = _get_json(f"https://api.gbif.org/v1/species/{key}/vernacularNames", {"limit": 300})
            for r in (v.get("results") or []):
                lang = r.get("language")
                val = r.get("vernacularName")
                if not val: continue
                if lang == "eng": en.add(val)
                if lang == "rus": ru.add(val)
    except Exception:
        pass

    # Wikidata fallback
    if not en or not ru:
        try:
            sparql = """
            SELECT ?name ?lang WHERE {
              ?taxon wdt:P225 "%s" .
              ?taxon p:P1843 ?s . ?s ps:P1843 ?name .
              BIND(LANG(?name) AS ?lang)
            }""" % name.replace('"', '\\"')
            w = _get_json("https://query.wikidata.org/sparql",
                          {"query": sparql},
                          headers={"Accept": "application/sparql-results+json"})
            for b in (w.get("results", {}).get("bindings") or []):
                lang = b.get("lang", {}).get("value", "")
                val = b.get("name", {}).get("value", "")
                if not val: continue
                if lang == "en": en.add(val)
                if lang == "ru": ru.add(val)
        except Exception:
            pass

    return jsonify({"en": sorted(en), "ru": sorted(ru)})


# ---- Entrypoint ----
try:
    # удалить старые загруженные zip
    now = time.time()
    for p in UPLOADS_DIR.glob("*.zip"):
        try:
            if now - p.stat().st_mtime > CLEAN_DELAY_SEC:
                _safe_unlink(p)
        except FileNotFoundError:
            pass
except Exception:
    pass

_start_background_sweeper()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=_load_port(), debug=False)
