import os, re, json, time, shutil, zipfile, threading, logging, tempfile, subprocess
from pathlib import Path

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
    cur = safe_join_under(DATA_DIR, dataset_rel)
    rel_cur = dataset_rel
    while True:
        if list_images_direct(cur) or detect_model_files_direct(cur):
            return rel_cur
        subs = [d for d in cur.iterdir() if d.is_dir()]
        if len(subs) == 1:
            cur = subs[0];
            rel_cur = rel_cur / subs[0].name;
            continue
        return rel_cur


def list_cached_webp(dataset_rel: Path) -> list[str]:
    leaf = resolve_leaf_rel(dataset_rel)
    cdir = safe_join_under(CACHE_DIR, leaf)
    if not cdir.exists(): return []
    files = sorted([p for p in cdir.glob("*.webp")], key=lambda p: p.name)
    return [str(p.relative_to(CACHE_DIR)).replace("\\", "/") for p in files]


def list_cached_webp_raw(rel_under_cache: Path) -> list[str]:
    cdir = safe_join_under(CACHE_DIR, rel_under_cache)
    if not cdir.exists(): return []
    files = sorted([p for p in cdir.glob("*.webp")], key=lambda p: p.name)
    return [str((rel_under_cache / p.name).as_posix()) for p in files]


def find_datasets() -> list[dict]:
    items = []
    seen_ids = set()

    # 1) По данным в DATA_DIR
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
        rel_id = rel.as_posix()
        title = read_meta_title(p, fallback=p.name)

        cached = list_cached_webp(rel)  # уже по листу
        images_total = len(imgs_rec) if imgs_rec else len(cached)

        if imgs_rec or cached or model_any:
            if imgs_rec:
                thumb = f"/files/{rel_id}/{imgs_rec[0]}"
            elif cached:
                thumb = f"/spin-cache/{cached[0]}"
            else:
                thumb = ""
            mode = "model" if model_any else ("spin" if images_total > 0 else "empty")
            items.append({
                "id": rel_id, "title": title, "images": images_total, "mode": mode,
                "thumb": thumb,
                "model_url": f"/files/{rel_id}/{model_any['path'].name}" if model_any else "",
                "model_type": model_any.get("type", "") if model_any else ""
            })
            seen_ids.add(rel_id)

    # 2) Добавляем «осиротевшие» наборы по кэшу
    for root, dirs, files in os.walk(CACHE_DIR):
        p = Path(root)
        if p == CACHE_DIR:
            continue
        webps = sorted([f for f in p.glob("*.webp")], key=lambda q: q.name)
        if not webps:
            continue
        rel = p.relative_to(CACHE_DIR)  # путь набора относительно CACHE_DIR (он же ID)
        rel_id = rel.as_posix()
        if rel_id in seen_ids:
            continue
        data_node = DATA_DIR / rel
        title = read_meta_title(data_node, fallback=rel.name) if data_node.exists() else rel.name
        thumb = f"/spin-cache/{rel_id}/{webps[0].name}"
        items.append({
            "id": rel_id, "title": title, "images": len(webps), "mode": "spin",
            "thumb": thumb, "model_url": "", "model_type": ""
        })
        seen_ids.add(rel_id)

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


def cleanup_empty_dirs(start_dir: Path, stop_at: Path):
    """
    Удаляем пустые каталоги снизу вверх, НО не трогаем stop_at.
    """
    try:
        cur = start_dir.resolve()
        stop = stop_at.resolve()
    except Exception:
        return
    while True:
        if cur == stop:
            break
        try:
            if cur.exists() and cur.is_dir() and not any(cur.iterdir()):
                cur.rmdir()
            else:
                break
        except Exception:
            break
        if cur.parent == cur:
            break
        cur = cur.parent


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
                    _encode_webp_via_cwebp(tmp_path, dst_path, 0, quality=85)
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

    src_files = list_images_direct(src_dir)
    src_files = [f for f in src_files if Path(f).suffix.lower() != ".webp"]
    src_files.sort(key=_numeric_path_key)
    if not src_files:
        subdirs = [d for d in src_dir.iterdir() if d.is_dir()]
        all_written = []
        for sd in subdirs:
            sub_rel = (leaf / sd.name)
            all_written += ensure_spin_cache(sub_rel, max_w=max_w, max_frames=max_frames)
        return all_written

    if max_frames and len(src_files) > max_frames:
        idxs = _sample_indices(len(src_files), max_frames)
        src_files = [src_files[i] for i in idxs]

    for i, name in enumerate(src_files):
        src = src_dir / name
        dst = out_dir / f"{i:04d}.webp"
        dst.parent.mkdir(parents=True, exist_ok=True)
        try:
            _encode_webp_with_exif_fix(src, dst, max_w, quality=85)
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
    acc = set()
    for root, dirs, files in os.walk(base_abs):
        p = Path(root)
        if p == DATA_DIR: continue
        try:
            rel = p.relative_to(DATA_DIR)
        except Exception:
            continue
        acc.add(resolve_leaf_rel(rel))
    uniq, seen = [], set()
    for r in acc:
        s = r.as_posix()
        if s not in seen:
            uniq.append(r);
            seen.add(s)
    uniq.sort(key=lambda r: (len(r.parts), r.as_posix()))
    return uniq
