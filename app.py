# app.py — стабильный webp-пайплайн, кэш строго на «листе» (без обёрток), список наборов корректный
import os, re, json, time, shutil, zipfile, threading, logging, tempfile, subprocess
from pathlib import Path
from flask import Flask, request, jsonify, render_template, send_from_directory, abort
from werkzeug.exceptions import RequestEntityTooLarge

# ---- Pillow / WebP detection ----
try:
    from PIL import Image, ImageOps, ImageFile, features as PIL_features

    ImageFile.LOAD_TRUNCATED_IMAGES = True
    PIL_OK = True
    WEBP_OK = bool(PIL_features.check("webp"))
    RESAMPLE = getattr(getattr(Image, "Resampling", Image), "LANCZOS")
except Exception:
    PIL_OK = False
    WEBP_OK = False
    RESAMPLE = None


def _which(exe: str) -> str | None:
    from shutil import which
    return which(exe)


CWEBP = _which("cwebp")

# ---- logging ----
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("flask.log", encoding="utf-8"), logging.StreamHandler()]
)
log = logging.getLogger("spin")

# ---- Config ----
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
ORIGINAL_IMAGE_EXT = {".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff"}
ALLOWED_MODEL_EXT = {".glb", ".gltf", ".obj", ".ply"}
MAX_ZIP_MB = 2048
CLEAN_DELAY_SEC = 300

# ---- Flask ----
app = Flask(__name__, static_folder=str(BASE_DIR / "static"), template_folder=str(BASE_DIR / "templates"))
app.config["MAX_CONTENT_LENGTH"] = MAX_ZIP_MB * 1024 * 1024


@app.errorhandler(RequestEntityTooLarge)
def handle_413(_e):
    return jsonify({"ok": False, "error": "file too large", "max_mb": MAX_ZIP_MB}), 413


# ---- Utils ----
def safe_rel_path(rel: str) -> Path:
    rel = (rel or "").strip().replace("\\", "/")
    parts = [p for p in Path(rel).parts if p not in ("", ".", "..")]
    if not parts: raise ValueError("bad relative path")
    parts[-1] = parts[-1][:128]
    return Path(*parts)


def safe_join_under(base: Path, rel: Path) -> Path:
    full = (base / rel).resolve()
    if not str(full).startswith(str(base.resolve())):
        raise PermissionError("path traversal")
    return full


def _nat(s: str):
    return tuple(int(t) if t.isdigit() else t.lower() for t in re.findall(r"\d+|\D+", s))


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
    if k >= n or k <= 0: return list(range(n))
    step = (n - 1) / (k - 1)
    raw = [round(i * step) for i in range(k)]
    seen, out = set(), []
    for idx in raw:
        if idx not in seen:
            seen.add(idx);
            out.append(idx)
    i = 0
    while len(out) < k and i < n:
        if i not in seen: out.append(i); seen.add(i)
        i += 1
    out.sort();
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


# ---- «лист» (разворачиваем одиночную обёртку) ----
def resolve_leaf_rel(dataset_rel: Path) -> Path:
    """
    Если в папке нет кадров напрямую и есть ровно одна подпапка, рекурсивно спускаемся.
    Возвращаем ОТНОСИТЕЛЬНЫЙ путь (от DATA_DIR) к листу.
    """
    cur = safe_join_under(DATA_DIR, dataset_rel)
    rel_cur = dataset_rel
    while True:
        if list_images_direct(cur) or detect_model_files_direct(cur):
            return rel_cur
        subs = [d for d in cur.iterdir() if d.is_dir()]
        if len(subs) == 1:
            cur = subs[0]
            rel_cur = rel_cur / subs[0].name
            continue
        return rel_cur  # либо пусто, либо несколько веток — считаем текущий узлом


def list_cached_webp(dataset_rel: Path) -> list[str]:
    # читаем кэш по ЛИСТУ
    leaf = resolve_leaf_rel(dataset_rel)
    cdir = safe_join_under(CACHE_DIR, leaf)
    if not cdir.exists(): return []
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
                continue  # пропускаем «обёртку»

        imgs_rec = list_images_recursive(p)
        model_any = model_direct or detect_model_files(p)
        rel = Path(p.relative_to(DATA_DIR))
        title = read_meta_title(p, fallback=p.name)

        cached = list_cached_webp(rel)  # уже по листу
        images_total = len(imgs_rec) if imgs_rec else len(cached)

        if imgs_rec or cached or model_any:
            if imgs_rec:
                thumb = f"/files/{rel.as_posix()}/{imgs_rec[0]}"
            elif cached:
                thumb = f"/spin-cache/{cached[0]}"
            else:
                thumb = ""
            mode = "model" if model_any else ("spin" if images_total > 0 else "empty")
            items.append({
                "id": rel.as_posix(), "title": title, "images": images_total, "mode": mode,
                "thumb": thumb,
                "model_url": f"/files/{rel.as_posix()}/{model_any['path'].name}" if model_any else "",
                "model_type": model_any.get("type", "") if model_any else ""
            })
    items.sort(key=lambda d: d["id"].lower())
    return items


# ---- очистки ----
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


def delete_originals_recursively(base_dir: Path):
    if not base_dir.exists(): return
    for p in base_dir.rglob("*"):
        if p.is_file() and p.suffix.lower() in ORIGINAL_IMAGE_EXT:
            _safe_unlink(p)


# ---- WebP helpers ----
def _encode_webp_via_pillow(img: "Image.Image", dst: Path, quality=85):
    if img.mode not in ("RGB", "RGBA"): img = img.convert("RGB")
    img.save(dst, "WEBP", quality=quality, method=6)


def _encode_webp_via_cwebp(src_path: Path, dst_path: Path, max_w: int, quality=85):
    if not CWEBP: raise RuntimeError("cwebp not found in PATH")
    cmd = [CWEBP, str(src_path), "-q", str(quality), "-m", "6", "-mt"]
    if max_w > 0:
        cmd.extend(["-resize", str(max_w), "0"])
    cmd.extend(["-o", str(dst_path)])
    subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def _encode_webp_with_exif_fix(src_path: Path, dst_path: Path, max_w: int, quality=85):
    if PIL_OK:
        with Image.open(src_path) as im:
            im = ImageOps.exif_transpose(im)
            if max_w and max(im.size) > max_w:
                im.thumbnail((max_w, max_w * 10), RESAMPLE)
            if WEBP_OK:
                _encode_webp_via_pillow(im, dst_path, quality=quality)
            else:
                if not CWEBP:
                    raise RuntimeError("no webp backend")
                with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                    tmp_path = Path(tmp.name)
                try:
                    im.save(tmp_path, "PNG", optimize=True)
                    _encode_webp_via_cwebp(tmp_path, dst_path, 0, quality=quality)
                finally:
                    _safe_unlink(tmp_path)
    else:
        _encode_webp_via_cwebp(src_path, dst_path, max_w, quality=85)


# ---- WebP кэш (СТРОГО на листе) ----
def ensure_spin_cache(dataset_rel: Path, max_w: int = 1280, max_frames: int = 90) -> list[str]:
    leaf = resolve_leaf_rel(dataset_rel)
    src_dir = safe_join_under(DATA_DIR, leaf)
    out_dir = safe_join_under(CACHE_DIR, leaf)
    out_dir.mkdir(parents=True, exist_ok=True)

    existing = sorted([p for p in out_dir.glob("*.webp")], key=lambda p: p.name)
    if existing:
        return [str(p.relative_to(CACHE_DIR)).replace("\\", "/") for p in existing]

    # берём кадры ТОЛЬКО из текущей папки-листа (без рекурсии), иначе будет конфликт и «двойные уровни»
    src_files = list_images_direct(src_dir)
    src_files = [f for f in src_files if Path(f).suffix.lower() != ".webp"]
    src_files.sort(key=_numeric_path_key)
    if not src_files:
        # на листе нет кадров — пробуем рекурсивно (мульти-лист), но пишем кэш под КАЖДЫЙ подлист отдельно
        subdirs = [d for d in src_dir.iterdir() if d.is_dir()]
        all_written = []
        for sd in subdirs:
            sub_rel = (leaf / sd.name)
            all_written += ensure_spin_cache(sub_rel, max_w=max_w, max_frames=max_frames)
        return all_written

    if max_frames and len(src_files) > max_frames:
        idxs = _sample_indices(len(src_files), max_frames)
        src_files = [src_files[i] for i in idxs]

    ok_cnt = 0
    for i, name in enumerate(src_files):
        src = src_dir / name
        dst = out_dir / f"{i:04d}.webp"
        dst.parent.mkdir(parents=True, exist_ok=True)
        try:
            _encode_webp_with_exif_fix(src, dst, max_w, quality=85)
            ok_cnt += 1
        except Exception as e:
            log.error("webp encode failed for %s -> %s", src, e)
            _safe_unlink(dst)

    result = sorted([p for p in out_dir.glob("*.webp")], key=lambda p: p.name)
    return [str(p.relative_to(CACHE_DIR)).replace("\\", "/") for p in result]


# ---- периодическая чистка оригиналов (после успешного кэша) ----
def sweep_originals(data_dir: Path, older_than_sec: int = CLEAN_DELAY_SEC):
    now = time.time()
    for root, dirs, files in os.walk(data_dir):
        p = Path(root)
        if p == data_dir:
            for skip in ("_uploads", "_cache"):
                if skip in dirs: dirs.remove(skip)
            continue
        try:
            rel = p.relative_to(data_dir)
        except Exception:
            continue
        if list_cached_webp(rel):
            for f in p.iterdir():
                try:
                    if f.is_file() and f.suffix.lower() in ORIGINAL_IMAGE_EXT and now - f.stat().st_mtime > older_than_sec:
                        f.unlink(missing_ok=True)
                except FileNotFoundError:
                    pass


def _start_background_sweeper():
    def _loop():
        while True:
            try:
                sweep_uploads(UPLOADS_DIR, CLEAN_DELAY_SEC)
                sweep_originals(DATA_DIR, CLEAN_DELAY_SEC)
            except Exception:
                pass
            time.sleep(60)

    threading.Thread(target=_loop, daemon=True).start()


# ---- наборы-листья под базовой папкой ----
def _leafs_under(base_abs: Path) -> list[Path]:
    # собираем все потенциальные узлы и нормализуем к листам
    acc = set()
    for root, dirs, files in os.walk(base_abs):
        p = Path(root)
        if p == DATA_DIR: continue
        try:
            rel = p.relative_to(DATA_DIR)
        except Exception:
            continue
        acc.add(resolve_leaf_rel(rel))
    # оставляем уникальные
    uniq = []
    seen = set()
    for r in acc:
        s = r.as_posix()
        if s not in seen:
            uniq.append(r);
            seen.add(s)
    # короткие пути раньше (чисто косметика)
    uniq.sort(key=lambda r: (len(r.parts), r.as_posix()))
    return uniq


# ---- Upload ZIP ----
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
        # 1) распаковка
        with zipfile.ZipFile(str(up_path), "r") as zf:
            for m in zf.infolist():
                if m.is_dir(): continue
                parts = [pp for pp in Path(m.filename).parts if pp not in ("", ".", "..")]
                if not parts: continue
                rel_path = Path(*parts)
                ext = rel_path.suffix.lower()
                if ext in (ALLOWED_IMAGE_EXT | ALLOWED_MODEL_EXT):
                    out = safe_join_under(DATA_DIR, dataset_rel / rel_path)
                    out.parent.mkdir(parents=True, exist_ok=True)
                    with zf.open(m) as src, open(out, "wb") as dst:
                        shutil.copyfileobj(src, dst)

        write_meta(target_dir, display_name)

        # 2) чистим старый кэш под веткой
        try:
            cache_sub = safe_join_under(CACHE_DIR, dataset_rel)
            if cache_sub.exists(): shutil.rmtree(cache_sub)
        except Exception:
            pass

        # 3) строим webp для каждого ЛИСТА
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
                # удаляем оригиналы только в этом листе
                delete_originals_recursively(safe_join_under(DATA_DIR, rel))

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
        log.exception("upload failed: %s", e)
        return jsonify({"ok": False, "error": f"upload failed: {e}"}), 500


# ---- API просмотра ----
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
    leaf = resolve_leaf_rel(rel)
    max_w = int(request.args.get("w", CFG.get("spin_max_w", 1280)))
    max_frames = int(request.args.get("max", CFG.get("spin_max_frames", 90)))

    urls_rel = list_cached_webp(leaf)
    if not urls_rel:
        urls_rel = ensure_spin_cache(leaf, max_w=max_w, max_frames=max_frames)

    if urls_rel:
        urls = [f"/spin-cache/{rp}" for rp in urls_rel]
        urls.sort(key=_numeric_from_url)
        return jsonify(urls)

    # fallback — оригиналы ровно в ЛИСТЕ
    base = safe_join_under(DATA_DIR, leaf)
    originals = list_images_direct(base)
    originals = [f for f in originals if Path(f).suffix.lower() != ".webp"]
    if originals:
        originals.sort(key=_numeric_path_key)
        urls = [f"/files/{leaf.as_posix()}/{name}" for name in originals]
        urls.sort(key=_numeric_from_url)
        return jsonify(urls)

    return jsonify({"ok": False, "error": "no frames found"}), 404


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
