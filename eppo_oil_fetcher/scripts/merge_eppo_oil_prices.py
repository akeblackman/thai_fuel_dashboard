#!/usr/bin/env python3
"""
รวมไฟล์ oil_price_*.xls ในโฟลเดอร์ → Excel + SQLite (และ CSV ถ้าระบุ)

ค่าเริ่มต้น: อ่านวันที่ล่าสุดจากไฟล์ Excel รวม แล้วประมวลเฉพาะไฟล์ .xls ที่มีวันที่ใหม่กว่า
จากนั้นต่อท้ายข้อมูลเดิม (ไม่ต้องอ่าน .xls ย้อนหลังทั้งหมดทุกครั้ง)

ใช้ --full เพื่อรวมใหม่จากทุกไฟล์ .xls เหมือนเดิม
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from fetch_eppo_oil_prices import merge_and_export  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(
        description="รวมไฟล์ .xls รายวันจาก สนพ. เป็น long-format (Excel / SQLite / CSV)"
    )
    ap.add_argument(
        "--input-dir",
        default="./eppo_oil_data",
        help="โฟลเดอร์ที่มีไฟล์ oil_price_YYYY-MM-DD.xls",
    )
    ap.add_argument(
        "--excel-out",
        default=None,
        help="ไฟล์ Excel รวม (ค่าเริ่มต้น: <input-dir>/oil_prices_merged.xlsx)",
    )
    ap.add_argument(
        "--sqlite-out",
        default=None,
        help="ไฟล์ SQLite (ค่าเริ่มต้น: <input-dir>/oil_prices.db)",
    )
    ap.add_argument(
        "--csv-out",
        default=None,
        help="ถ้าระบุ จะเขียนไฟล์ CSV รวม (UTF-8 BOM) ไปยัง path นี้",
    )
    ap.add_argument(
        "--full",
        action="store_true",
        help="รวมใหม่จากทุกไฟล์ .xls (ไม่ใช้โหมดต่อท้ายจาก Excel)",
    )
    args = ap.parse_args()

    raw_dir = Path(args.input_dir).resolve()
    excel_out = Path(args.excel_out) if args.excel_out else raw_dir / "oil_prices_merged.xlsx"
    sqlite_out = Path(args.sqlite_out) if args.sqlite_out else raw_dir / "oil_prices.db"
    csv_out = Path(args.csv_out) if args.csv_out else None

    merge_and_export(
        raw_dir,
        excel_out,
        sqlite_out,
        csv_path=csv_out,
        incremental=not args.full,
    )


if __name__ == "__main__":
    main()
