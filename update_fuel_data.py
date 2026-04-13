#!/usr/bin/env python3
"""
1. รัน eppo_oil_fetcher/fetch_eppo_oil_prices.py (รอจนเสร็จ → ได้ oil_prices_merged.xlsx)
2. คัดลอก oil_prices_merged.xlsx ไปที่ fuel-data-cleaner/
3. รัน fuel-data-cleaner/clean_fuel_types.py กับไฟล์นั้น (รอจนเสร็จ)
4. คัดลอก fuel_prices_cleaned.xlsx มาไว้โฟลเดอร์เดียวก עםสคริปต์นี้

Usage:
  python update_fuel_data.py

หลังสำเร็จจะลบไฟล์ที่ดาวน์โหลด/ชั่วคราวใน eppo_oil_fetcher/eppo_oil_data
(เช่น oil_price_*.xls, oil_prices.db, CSV รวม) เหลือแค่ oil_prices_merged.xlsx
สำหรับรอบถัดไป — ตั้ง EPPO_KEEP_RAW_AFTER_UPDATE=1 ถ้าไม่ต้องการลบ

อัปเดตทุกวันด้วย cron (ตัวอย่าง — แก้ path ให้ตรงเครื่องคุณ):
  0 6 * * * cd /path/to/project && /path/to/python update_fuel_data.py
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
EPPO_DIR = ROOT / "eppo_oil_fetcher"
FETCH_SCRIPT = EPPO_DIR / "fetch_eppo_oil_prices.py"
EPPO_DATA_DIR = EPPO_DIR / "eppo_oil_data"
MERGED_XLSX = EPPO_DATA_DIR / "oil_prices_merged.xlsx"

CLEANER_DIR = ROOT / "fuel-data-cleaner"
CLEANER_SCRIPT = CLEANER_DIR / "clean_fuel_types.py"
MERGED_IN_CLEANER = CLEANER_DIR / "oil_prices_merged.xlsx"
CLEANED_XLSX = CLEANER_DIR / "fuel_prices_cleaned.xlsx"
OUT_ROOT = ROOT / "fuel_prices_cleaned.xlsx"


def _run(cmd: list[str], *, cwd: Path) -> None:
    print(f"\n→ {' '.join(cmd)}", flush=True)
    print(f"   (cwd: {cwd})", flush=True)
    subprocess.run(cmd, cwd=cwd, check=True)


def _prune_eppo_oil_data_after_update(data_dir: Path) -> None:
    """
    ลบไฟล์ใน eppo_oil_data ที่ไม่จำเป็น — เหลือเฉพาะ oil_prices_merged.xlsx
    (ไฟล์ดาวน์โหลด oil_price_*.xls และผลพลอยได้ .db / .csv จะถูกลบ)

    เก็บ merged xlsx ไว้เพื่อรอบถัดไป (อ่านวันที่ล่าสุด + ต่อท้ายข้อมูล)
    """
    v = os.environ.get("EPPO_KEEP_RAW_AFTER_UPDATE", "0").strip().lower()
    if v in ("1", "true", "yes", "on"):
        print("\n→ ข้ามการลบใน eppo_oil_data (EPPO_KEEP_RAW_AFTER_UPDATE=1)", flush=True)
        return
    if not data_dir.is_dir():
        return
    keep = {"oil_prices_merged.xlsx"}
    removed: list[str] = []
    for p in sorted(data_dir.iterdir()):
        if not p.is_file() or p.name in keep:
            continue
        try:
            p.unlink()
            removed.append(p.name)
        except OSError as e:
            print(f"   คำเตือน: ลบ {p.name} ไม่ได้ — {e}", flush=True)
    if removed:
        print(f"\n→ ลบใน eppo_oil_data ({len(removed)} ไฟล์) เหลือแค่ oil_prices_merged.xlsx:", flush=True)
        for name in removed:
            print(f"     - {name}", flush=True)
    else:
        print("\n→ eppo_oil_data: ไม่มีไฟล์อื่นนอก oil_prices_merged.xlsx", flush=True)


def run_pipeline() -> int:
    """
    ดึงจาก สนพ. → merge → clean → คัดลอก fuel_prices_cleaned.xlsx ไปที่โฟลเดอร์โปรเจกต์
    Returns exit code: 0 = สำเร็จ, 2–4 = ข้อผิดพลาด
    """
    if not FETCH_SCRIPT.is_file():
        print(f"ERROR: ไม่พบ {FETCH_SCRIPT}", file=sys.stderr)
        return 2
    if not CLEANER_SCRIPT.is_file():
        print(f"ERROR: ไม่พบ {CLEANER_SCRIPT}", file=sys.stderr)
        return 2

    _run([sys.executable, str(FETCH_SCRIPT)], cwd=EPPO_DIR)

    if not MERGED_XLSX.is_file():
        print(f"ERROR: หลัง fetch ไม่พบไฟล์ {MERGED_XLSX}", file=sys.stderr)
        return 3

    CLEANER_DIR.mkdir(parents=True, exist_ok=True)
    print(f"\n→ copy {MERGED_XLSX} → {MERGED_IN_CLEANER}", flush=True)
    shutil.copy2(MERGED_XLSX, MERGED_IN_CLEANER)

    _run(
        [sys.executable, CLEANER_SCRIPT.name, MERGED_IN_CLEANER.name],
        cwd=CLEANER_DIR,
    )

    if not CLEANED_XLSX.is_file():
        print(f"ERROR: ไม่พบผลลัพธ์ {CLEANED_XLSX}", file=sys.stderr)
        return 4

    print(f"\n→ copy {CLEANED_XLSX} → {OUT_ROOT}", flush=True)
    shutil.copy2(CLEANED_XLSX, OUT_ROOT)

    _prune_eppo_oil_data_after_update(EPPO_DATA_DIR)

    print(f"\nเสร็จแล้ว: {OUT_ROOT}", flush=True)
    return 0


def main() -> int:
    return run_pipeline()


if __name__ == "__main__":
    raise SystemExit(main())
