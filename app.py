# app.py — скан всей data/, загрузка/удаление по паролю, 3D, spin через webp-кэш (строгий порядок + авто-очистка оригиналов)
import os, re, json, time, shutil, zipfile, threading
from pathlib import Path
from flask import Flask, request, jsonify, render_template, send_from_directory, abort
from werkzeug.exceptions import RequestEntityTooLarge

# --- необязательная оптимизация изображений (Pillow) ---
try:
    from PIL import Image, ImageOps

    PIL_OK = True
    RESAMPLE = getattr(getattr(Image, "Resampling", Image), "LANCZOS")
except Exception:
    PIL_OK = False
    RESAMPLE = None

# ---------- Config ----------
BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"


def _load_config() -> dict:
    if CONFIG_PATH.exists():
        try:
            return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


CFG = _load_config()


def _load_data_dir() -> Path:
    dd = CFG.get("data_dir") or os.environ.get("GALLERY_DATA_DIR")
    p = Path(dd).expanduser().resolve() if dd else (BASE_DIR / "data").resolve()
    p.mkdir(parents=True, exist_ok=True)
    return p


def _load_port() -> int:
    if isinstance(CFG.get("port"), int):
        return int(CFG["port"])
    if os.environ.get("PORT"):
        try:
            return int(os.environ["PORT"])
        except ValueError:
            pass
    return 9013


UPLOAD_PASSWORD = str(CFG.get("upload_password", "admin67"))

DATA_DIR = _load_data_dir()
UPLOADS_DIR = DATA_DIR / "_uploads"
CACHE_DIR = DATA_DIR / "_cache" / "spin"
UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

ALLOWED_IMAGE_EXT = {".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff", ".webp"}
# оригиналы, которые можно удалять (webp НЕ трогаем)
ORIGINAL_IMAGE_EXT = {".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff"}
ALLOWED_MODEL_EXT = {".glb", ".gltf", ".obj", ".ply"}
MAX_ZIP_MB = 2048
CLEAN_DELAY_SEC = 300

# ---------- Flask ----------
app = Flask(
    __name__,
    static_folder=str(BASE_DIR / "static"),
    template_folder=str(BASE_DIR / "templates"),
)
app.config["MAX_CONTENT_LENGTH"] = MAX_ZIP_MB * 1024 * 1024


# Совместимые JSON-ошибки при больших файлах
@app.errorhandler(RequestEntityTooLarge)
def handle_413(_e):
    return jsonify({"ok": False, "error": "file too large", "max_mb": MAX_ZIP_MB}), 413


# ---------- Utils ----------
def safe_rel_path(rel: str) -> Path:
    rel = (rel or "").strip().replace("\\", "/")
    parts = [p for p in Path(rel).parts if p not in ("", ".", "..")]
    if not parts:
        raise ValueError("bad relative path")
    parts[-1] = parts[-1][:128]
    return Path(*parts)


def safe_join_under(base: Path, rel: Path) -> Path:
    full = (base / rel).resolve()
    if not str(full).startswith(str(base.resolve())):
        raise PermissionError("path traversal")
    return full


# натуральная сортировка
def _nat(s: str):
    return tuple(int(t) if t.isdigit() else t.lower() for t in re.findall(r"\d+|\D+", s))


# числовой ключ: родители — натурально, файл — по числу из stem (если есть)
def _numeric_path_key(relpath: str):
    relpath = relpath.replace("\\", "/")
    parts = relpath.split("/")
    parents, name = parts[:-1], parts[-1]
    stem = Path(name).stem
    parent_key = tuple(_nat(p) for p in parents)
    if stem.isdigit():
        num_key = (0, int(stem));
        name_key = ()
    else:
        m = re.search(r"\d+", stem)
        num_key = (0, int(m.group(0))) if m else (1,)
        name_key = _nat(name)
    return (parent_key, num_key, name_key)


def _sample_indices(n: int, k: int):
    if k >= n or k <= 0:
        return list(range(n))
    step = (n - 1) / (k - 1)
    raw = [round(i * step) for i in range(k)]
    seen, out = set(), []
    for idx in raw:
        if idx not in seen:
            seen.add(idx);
            out.append(idx)
    i = 0
    while len(out) < k and i < n:
        if i not in seen:
            out.append(i);
            seen.add(i)
        i += 1
    out.sort()
    return out


def list_images_recursive(dir_path: Path) -> list[str]:
    if not dir_path.exists(): return []
    rels = []
    for p in dir_path.rglob("*"):
        if p.is_file() and p.suffix.lower() in ALLOWED_IMAGE_EXT:
            rels.append(str(p.relative_to(dir_path)).replace("\\", "/"))
    rels.sort(key=_numeric_path_key)
    return rels


def list_images_direct(dir_path: Path) -> list[str]:
    return [p.name for p in dir_path.iterdir()
            if p.is_file() and p.suffix.lower() in ALLOWED_IMAGE_EXT] if dir_path.exists() else []


def detect_model_files(dir_path: Path) -> dict:
    for ext in [".glb", ".gltf", ".obj", ".ply"]:
        cand = dir_path / f"model{ext}"
        if cand.exists(): return {"type": ext[1:], "path": cand}
    for p in sorted(dir_path.rglob("*")):
        if p.is_file() and p.suffix.lower() in ALLOWED_MODEL_EXT:
            return {"type": p.suffix.lower()[1:], "path": p}
    return {}


def detect_model_files_direct(dir_path: Path) -> dict:
    if not dir_path.exists(): return {}
    for ext in [".glb", ".gltf", ".obj", ".ply"]:
        cand = dir_path / f"model{ext}"
        if cand.exists(): return {"type": ext[1:], "path": cand}
    for p in sorted(dir_path.iterdir()):
        if p.is_file() and p.suffix.lower() in ALLOWED_MODEL_EXT:
            return {"type": p.suffix.lower()[1:], "path": p}
    return {}


def read_meta_title(dir_path: Path, fallback: str) -> str:
    meta = dir_path / ".meta.json"
    if meta.exists():
        try:
            data = json.loads(meta.read_text(encoding="utf-8"))
            t = (data.get("display_name") or "").strip()
            if t: return t
        except Exception:
            pass
    return fallback


def write_meta(dir_path: Path, display_name: str | None):
    meta = dir_path / ".meta.json"
    data = {"display_name": (display_name or "").strip() or dir_path.name,
            "created_at": int(time.time())}
    meta.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def list_cached_webp(dataset_rel: Path) -> list[str]:
    """Список webp-кадров из кэша для датасета (отн. путь внутри CACHE_DIR), в правильном порядке."""
    cdir = (CACHE_DIR / dataset_rel).resolve()
    if not str(cdir).startswith(str(CACHE_DIR.resolve())) or not cdir.exists():
        return []
    files = sorted([p for p in cdir.glob("*.webp")], key=lambda p: p.name)
    return [str(p.relative_to(CACHE_DIR)).replace("\\", "/") for p in files]


def find_datasets() -> list[dict]:
    items = []
    for root, dirs, files in os.walk(DATA_DIR):
        p = Path(root)
        if p == DATA_DIR:
            for skip in ("_uploads", "_cache"):
                if skip in dirs: dirs.remove(skip)
            continue

        imgs_direct = list_images_direct(p)
        model_direct = detect_model_files_direct(p)
        has_direct = bool(imgs_direct or model_direct)
        if not has_direct:
            subdirs = [d for d in p.iterdir() if d.is_dir()]
            if len(subdirs) == 1:
                continue

        imgs_rec = list_images_recursive(p)  # оригиналы (если остались)
        model_any = model_direct or detect_model_files(p)
        rel = str(p.relative_to(DATA_DIR)).replace("\\", "/")
        title = read_meta_title(p, fallback=p.name)

        cached = list_cached_webp(Path(rel))  # webp из кэша
        images_total = len(imgs_rec) if imgs_rec else len(cached)

        if imgs_rec or cached or model_any:
            # превью: оригинал → webp → пусто
            if imgs_rec:
                thumb = f"/files/{rel}/{imgs_rec[0]}"
            elif cached:
                thumb = f"/spin-cache/{cached[0]}"
            else:
                thumb = ""
            mode = "model" if model_any else ("spin" if images_total > 0 else "empty")
            items.append({
                "id": rel,
                "title": title,
                "images": images_total,
                "mode": mode,
                "thumb": thumb,
                "model_url": f"/files/{rel}/{model_any['path'].name}" if model_any else "",
                "model_type": model_any.get("type", "") if model_any else ""
            })

    items.sort(key=lambda d: d["id"].lower())
    return items


# ---------- Uploads/originals auto-clean ----------
def _safe_unlink(p: Path):
    try:
        p.unlink(missing_ok=True)
    except Exception:
        pass


def schedule_delete(file_path: Path, delay_sec: int = CLEAN_DELAY_SEC):
    t = threading.Timer(delay_sec, _safe_unlink, args=(file_path,))
    t.daemon = True;
    t.start()


def sweep_uploads(dir_path: Path, older_than_sec: int = CLEAN_DELAY_SEC):
    now = time.time()
    for p in dir_path.glob("*"):
        try:
            if p.is_file() and now - p.stat().st_mtime > older_than_sec:
                _safe_unlink(p)
        except FileNotFoundError:
            pass


def ensure_spin_cache(dataset_rel: Path, max_w: int = 1280, max_frames: int = 90) -> list[str]:
    """
    Возвращает список ОТНОСИТЕЛЬНЫХ путей к webp-кадрам в CACHE_DIR/<dataset_rel>/####.webp.
    Порядок строго совпадает с порядком файлов (числовая сортировка).
    """
    src_dir = safe_join_under(DATA_DIR, dataset_rel)
    out_dir = safe_join_under(CACHE_DIR, dataset_rel)
    out_dir.mkdir(parents=True, exist_ok=True)

    existing = sorted([p for p in out_dir.glob("*.webp")], key=lambda p: p.name)
    if existing:
        return [str(p.relative_to(CACHE_DIR)).replace("\\", "/") for p in existing]

    src_files_rel = list_images_recursive(src_dir)
    if not src_files_rel:
        return []

    if max_frames and len(src_files_rel) > max_frames:
        idxs = _sample_indices(len(src_files_rel), max_frames)
        src_files_rel = [src_files_rel[i] for i in idxs]

    if not PIL_OK:
        return [str((src_dir / rp).relative_to(DATA_DIR)).replace("\\", "/") for rp in src_files_rel]

    for i, rp in enumerate(src_files_rel):
        src = src_dir / rp
        dst = out_dir / f"{i:04d}.webp"
        dst.parent.mkdir(parents=True, exist_ok=True)
        try:
            with Image.open(src) as im:
                im = ImageOps.exif_transpose(im)
                im.thumbnail((max_w, max_w * 10), RESAMPLE)
                im.save(dst, "WEBP", quality=85, method=6)
        except Exception:
            continue

    result = sorted([p for p in out_dir.glob("*.webp")], key=lambda p: p.name)
    return [str(p.relative_to(CACHE_DIR)).replace("\\", "/") for p in result]


def sweep_originals(data_dir: Path, older_than_sec: int = CLEAN_DELAY_SEC):
    """
    Для всех датасетов:
    - гарантируем webp-кэш (ensure_spin_cache)
    - удаляем оригиналы (jpg/png/tif/bmp) старше older_than_sec
    """
    now = time.time()

    for root, dirs, files in os.walk(data_dir):
        p = Path(root)
        if p == data_dir:
            for skip in ("_uploads", "_cache"):
                if skip in dirs: dirs.remove(skip)
            continue

        try:
            dataset_rel = p.relative_to(data_dir)
        except Exception:
            continue

        originals = [f for f in p.iterdir()
                     if f.is_file() and f.suffix.lower() in ORIGINAL_IMAGE_EXT]

        if originals:
            try:
                ensure_spin_cache(dataset_rel)
            except Exception:
                # если кэш не построился — оригиналы не трогаем
                continue

        for f in originals:
            try:
                if now - f.stat().st_mtime > older_than_sec:
                    f.unlink(missing_ok=True)
            except FileNotFoundError:
                pass


def _start_background_sweeper():
    """Каждую минуту: чистим _uploads и оригиналы (оставляя webp-кэш)."""

    def _loop():
        while True:
            try:
                sweep_uploads(UPLOADS_DIR, CLEAN_DELAY_SEC)
                sweep_originals(DATA_DIR, CLEAN_DELAY_SEC)
            except Exception:
                pass
            time.sleep(60)

    t = threading.Thread(target=_loop, daemon=True)
    t.start()


# ---------- Routes ----------
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
    if not full.exists() or not full.is_file():
        abort(404)
    return send_from_directory(full.parent, full.name)


@app.route("/spin-cache/<path:subpath>")
def serve_from_cache(subpath):
    try:
        rel = safe_rel_path(subpath)
        full = safe_join_under(CACHE_DIR, rel)
    except Exception:
        abort(404)
    if not full.exists() or not full.is_file():
        abort(404)
    return send_from_directory(full.parent, full.name)


@app.route("/api/datasets")
def api_datasets():
    return jsonify(find_datasets())


@app.route("/api/images/<path:dataset_rel>")
def api_images(dataset_rel):
    rel = safe_rel_path(dataset_rel)
    base = safe_join_under(DATA_DIR, rel)
    files = list_images_recursive(base)
    return jsonify([f"/files/{rel}/{rp}".replace("\\", "/") for rp in files])


def _numeric_from_url(u: str) -> tuple:
    name = Path(u).name
    stem = Path(name).stem
    if stem.isdigit(): return (0, int(stem), name.lower())
    m = re.search(r"\d+", stem)
    return (0, int(m.group(0)), name.lower()) if m else (1, name.lower())


@app.route("/api/spin/<path:dataset_rel>")
def api_spin(dataset_rel):
    rel = safe_rel_path(dataset_rel)
    max_w = int(request.args.get("w", "1280"))
    max_frames = int(request.args.get("max", "90"))
    rels = ensure_spin_cache(rel, max_w=max_w, max_frames=max_frames)
    urls = []
    for rp in rels:
        urls.append(f"/spin-cache/{rp}" if rp.lower().endswith(".webp") else f"/files/{rp}")
    urls.sort(key=_numeric_from_url)  # страховка: числовая сортировка
    return jsonify(urls)


@app.route("/api/upload_zip", methods=["POST"])
def api_upload_zip():
    # ВСЕГДА JSON-ответы
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
        # перехватывается глобальным handler'ом, но на всякий
        return jsonify({"ok": False, "error": "file too large", "max_mb": MAX_ZIP_MB}), 413

    schedule_delete(up_path, CLEAN_DELAY_SEC)

    target_dir.mkdir(parents=True, exist_ok=True)
    try:
        with zipfile.ZipFile(str(up_path), "r") as zf:
            for m in zf.infolist():
                if m.is_dir(): continue
                parts = [pp for pp in Path(m.filename).parts if pp not in ("", ".", "..")]
                if not parts: continue
                rel_path = Path(*parts)
                ext = rel_path.suffix.lower()
                if ext in (ALLOWED_IMAGE_EXT | ALLOWED_MODEL_EXT):
                    out = safe_join_under(target_dir, rel_path)
                    out.parent.mkdir(parents=True, exist_ok=True)
                    with zf.open(m) as src, open(out, "wb") as dst:
                        shutil.copyfileobj(src, dst)
        write_meta(target_dir, display_name)
        # сбросим кэш спина, чтобы пересобрался из новых кадров
        try:
            shutil.rmtree(safe_join_under(CACHE_DIR, dataset_rel))
        except Exception:
            pass
        rel_str = str(dataset_rel).replace("\\", "/")
        return jsonify(
            {"ok": True, "dataset_id": rel_str, "display_name": read_meta_title(target_dir, target_dir.name)})
    except zipfile.BadZipFile:
        return jsonify({"ok": False, "error": "bad zip"}), 400
    except Exception as e:
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
        if not target.exists() or not target.is_dir():
            return jsonify({"ok": False, "error": "not found"}), 404
        shutil.rmtree(target)
        try:
            shutil.rmtree(safe_join_under(CACHE_DIR, rel))
        except Exception:
            pass
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


# ---------- Entrypoint ----------
# разово при старте
try:
    sweep_uploads(UPLOADS_DIR, CLEAN_DELAY_SEC)
except Exception:
    pass
# периодически
_start_background_sweeper()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=_load_port(), debug=False)
