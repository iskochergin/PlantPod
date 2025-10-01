# make_webp_cache.py
# pip install Pillow

import os, re, sys, time
from pathlib import Path

# ==== НАСТРОЙКИ ====
ROOT_DIR = Path('/Users/ivankochergin/Yandex.Disk.localized/Data/1Projects/plant-capsule/3d_scans').resolve()  # <- ПУТЬ К ТВОЕЙ data/
CACHE_DIR = ROOT_DIR / "_cache" / "spin"
MAX_W = 1280  # макс. ширина кадра (высота подгонится пропорционально)
MAX_FRAMES = 90  # ограничение числа кадров; 0 или None — без ограничения
QUALITY = 85  # качество WebP (0..100)
# ====================

# --- Pillow (для webp) ---
try:
    from PIL import Image, ImageOps

    RESAMPLE = getattr(getattr(Image, "Resampling", Image), "LANCZOS")
except Exception as e:
    print("Pillow не установлен или не собирает webp. Установи: pip install Pillow")
    sys.exit(1)

ALLOWED_IMAGE_EXT = {".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff", ".webp"}


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


def list_images_recursive(dir_path: Path) -> list[str]:
    if not dir_path.exists(): return []
    rels = []
    for p in dir_path.rglob("*"):
        if p.is_file() and p.suffix.lower() in ALLOWED_IMAGE_EXT:
            rels.append(str(p.relative_to(dir_path)).replace("\\", "/"))
    rels.sort(key=_numeric_path_key)
    return rels


def sample_indices(n: int, k: int):
    if not k or k >= n or k <= 0: return list(range(n))
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


def ensure_spin_cache_for(dataset_abs: Path):
    """Строит cache для одной папки-датасета; возвращает количество созданных webp."""
    rel = dataset_abs.relative_to(ROOT_DIR)
    out_dir = (CACHE_DIR / rel).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    # если уже есть webp-кадры — пропускаем
    existing = sorted([p for p in out_dir.glob("*.webp")], key=lambda p: p.name)
    if existing:
        return len(existing)

    src_files_rel = list_images_recursive(dataset_abs)
    if not src_files_rel:
        return 0

    # ограничение кадров (равномерная выборка)
    if MAX_FRAMES and len(src_files_rel) > MAX_FRAMES:
        idxs = sample_indices(len(src_files_rel), MAX_FRAMES)
        src_files_rel = [src_files_rel[i] for i in idxs]

    created = 0
    for i, rp in enumerate(src_files_rel):
        src = dataset_abs / rp
        dst = out_dir / f"{i:04d}.webp"
        dst.parent.mkdir(parents=True, exist_ok=True)
        try:
            with Image.open(src) as im:
                im = ImageOps.exif_transpose(im)
                im.thumbnail((MAX_W, MAX_W * 10), RESAMPLE)
                im.save(dst, "WEBP", quality=QUALITY, method=6)
                created += 1
        except Exception as e:
            print(f"[WARN] Пропущен кадр {src}: {e}")
            continue
    return created


def looks_like_dataset(p: Path) -> bool:
    # «датасет» = есть хотя бы одна картинка на любом уровне
    for q in p.rglob("*"):
        if q.is_file() and q.suffix.lower() in ALLOWED_IMAGE_EXT:
            return True
    return False


def walk_datasets(root: Path):
    """Отдаём ВСЕ папки под ROOT_DIR (кроме _cache/_uploads), где есть кадры."""
    for base, dirs, files in os.walk(root):
        cur = Path(base)
        if cur == root:
            # не заходим в служебные
            for skip in ("_cache", "_uploads"):
                if skip in dirs: dirs.remove(skip)
        if cur == root:
            continue
        try:
            if looks_like_dataset(cur):
                yield cur
        except Exception:
            pass


def main():
    if not ROOT_DIR.exists():
        print(f"ROOT_DIR не найден: {ROOT_DIR}")
        sys.exit(1)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    start = time.time()
    total_sets = 0
    total_frames = 0

    print(f"ROOT_DIR = {ROOT_DIR}")
    print(f"CACHE_DIR = {CACHE_DIR}")
    print("Сканирую…")

    for ds in walk_datasets(ROOT_DIR):
        rel = ds.relative_to(ROOT_DIR)
        print(f"→ {rel} …", end="", flush=True)
        n = ensure_spin_cache_for(ds)
        print(f" {n} webp")
        total_sets += 1
        total_frames += n

    dt = time.time() - start
    print(f"\nГотово: наборов {total_sets}, кадров {total_frames}, заняло {dt:.1f}s")


if __name__ == "__main__":
    main()
