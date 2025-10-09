from __future__ import annotations
import csv
import sys
from pathlib import Path

DATASET_DIRNAME = "dataset_collect"


def find_dataset_root(start: Path) -> Path | None:
    """Ищет папку dataset_collect, поднимаясь вверх от start."""
    cur = start.resolve()
    for p in [cur] + list(cur.parents):
        candidate = p / DATASET_DIRNAME
        if candidate.is_dir():
            return candidate
    return None


def count_rows(csv_path: Path) -> int:
    """Количество строк (без заголовка) в selected.csv."""
    try:
        with csv_path.open("r", newline="", encoding="utf-8") as f:
            rd = csv.reader(f)
            # пропускаем заголовок
            try:
                header = next(rd)
            except StopIteration:
                return 0
            # считаем оставшиеся
            return sum(1 for _ in rd)
    except Exception as e:
        print(f"[warn] не удалось прочитать {csv_path}: {e}", file=sys.stderr)
        return 0


def main():
    # точка старта — директория этого файла (helpers/)
    here = Path(__file__).resolve().parent
    root = find_dataset_root(here)
    if not root:
        print(f"[err] Не найдена папка '{DATASET_DIRNAME}' выше {here}", file=sys.stderr)
        sys.exit(1)

    rows = []
    for sub in sorted(root.iterdir()):
        if not sub.is_dir():
            continue
        csv_path = sub / "selected.csv"
        if not csv_path.is_file():
            continue
        n = count_rows(csv_path)
        if n > 0:
            rows.append((sub.name, n))

    rows.sort(key=lambda x: x[1], reverse=True)

    if not rows:
        print("Нет таксонов с выбранными фото.")
        return

    for name, n in rows:
        print(f"{name}\t{n}")

    total_taxa = len(rows)
    total_photos = sum(n for _, n in rows)
    print("-" * 32)
    print(f"Итог: {total_taxa} таксонов, {total_photos} фото")


if __name__ == "__main__":
    main()
