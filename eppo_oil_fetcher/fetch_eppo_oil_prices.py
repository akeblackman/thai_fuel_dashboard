#!/usr/bin/env python3
"""
ดึงราคาน้ำมันปลีกจากฟอร์ม PRICE ON / Generate บนเว็บ สนพ.
( endpoint: https://www2.eppo.go.th/epporop/entryreport/ropbydaypublic.aspx )

ผลลัพธ์แต่ละวันคือไฟล์ .xls — หลังดึงครบจะรวมเป็น long-format → Excel + SQLite ใน output-dir
ถ้าไม่ระบุ --start จะอ่านวันที่ล่าสุดจาก oil_prices_merged.xlsx แล้วดึงต่อจนถึงวันนี้
(รวมเฉพาะจากไฟล์เดิมโดยไม่ดึงใหม่: ใช้ scripts/merge_eppo_oil_prices.py)
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Optional

import certifi
import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www2.eppo.go.th/epporop/entryreport/ropbydaypublic.aspx"
XLS_MAGIC = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"
MERGED_SHEET = "oil_prices_long"
OIL_XLS_FILENAME_DATE = re.compile(r"oil_price_(\d{4}-\d{2}-\d{2})\.xls$", re.IGNORECASE)
MERGED_COLUMNS = (
    "price_date",
    "product_name",
    "brand",
    "price_baht_per_litre",
    "source_file",
)
# เมื่อไม่มีไฟล์รวม / อ่านไม่ได้ — ใช้เป็นวันเริ่มดึงครั้งแรก
DEFAULT_START_FALLBACK = date(2026, 3, 23)


def date_from_oil_xls_filename(path: Path) -> Optional[date]:
    """ดึงวันที่จากชื่อไฟล์ oil_price_YYYY-MM-DD.xls (ถ้าไม่ตรงรูปแบบ → None)."""
    m = OIL_XLS_FILENAME_DATE.search(path.name)
    if not m:
        return None
    try:
        return date.fromisoformat(m.group(1))
    except ValueError:
        return None


def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def format_dd_mm_yyyy(d: date) -> str:
    return d.strftime("%d/%m/%Y")


def parse_title_date(cell: Any) -> Optional[date]:
    if cell is None or (isinstance(cell, float) and pd.isna(cell)):
        return None
    s = str(cell).strip()
    m = re.search(r"(\d{2})/(\d{2})/(\d{4})", s)
    if not m:
        return None
    dd, mm, yyyy = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return date(yyyy, mm, dd)
    except ValueError:
        return None


def _clean_brand_name(x: Any) -> Optional[str]:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip()
    if not s or s.lower().startswith("unit"):
        return None
    return s


def parse_xls_to_long(path: Path) -> tuple[Optional[date], pd.DataFrame]:
    """อ่านไฟล์ xls จาก สนพ. แปลงเป็น DataFrame แบบ long (product, brand, price)."""
    df = pd.read_excel(path, engine="xlrd", header=None)
    if df.empty:
        return None, pd.DataFrame()

    title_date = parse_title_date(df.iloc[0, 0])

    # หาแถวหัวคอลัมน์ยี่ห้อ (แถวที่มี PTT / Unit)
    header_row_idx = None
    for i in range(min(15, len(df))):
        row = df.iloc[i].astype(str)
        if row.str.contains("PTT", case=False, na=False).any():
            header_row_idx = i
            break
    if header_row_idx is None:
        header_row_idx = 11

    brands: list[str] = []
    for c in range(1, df.shape[1]):
        b = _clean_brand_name(df.iloc[header_row_idx, c])
        if b:
            brands.append(b)

    rows_out: list[dict[str, Any]] = []
    for r in range(header_row_idx + 2, len(df)):
        product = df.iloc[r, 0]
        if product is None or (isinstance(product, float) and pd.isna(product)):
            continue
        ps = str(product).strip()
        if not ps:
            continue
        if "มีผลตั้งแต่" in ps or "Effective Date" in ps:
            break
        if ps.startswith("หมายเหตุ") or ps.startswith("ทั้งนี้"):
            break

        for j, brand in enumerate(brands):
            col = 1 + j
            if col >= df.shape[1]:
                break
            val = df.iloc[r, col]
            price: Optional[float]
            if val is None or (isinstance(val, float) and pd.isna(val)):
                price = None
            else:
                try:
                    fv = float(val)
                except (TypeError, ValueError):
                    price = None
                else:
                    if fv == 0.0:
                        price = None
                    else:
                        price = fv
            rows_out.append(
                {
                    "product_name": ps,
                    "brand": brand,
                    "price_baht_per_litre": price,
                }
            )

    out = pd.DataFrame(rows_out)
    if title_date is not None:
        out.insert(0, "price_date", title_date)
    return title_date, out


def fetch_xls_for_date(session: requests.Session, day: date, timeout: int = 120) -> bytes:
    r = session.get(BASE_URL, verify=certifi.where(), timeout=timeout)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    vs = soup.find("input", {"id": "__VIEWSTATE"})
    ev = soup.find("input", {"id": "__EVENTVALIDATION"})
    vsg = soup.find("input", {"id": "__VIEWSTATEGENERATOR"})
    if not vs or not ev:
        raise RuntimeError("ไม่พบ __VIEWSTATE / __EVENTVALIDATION ในหน้าแรก")

    data = {
        "__VIEWSTATE": vs["value"],
        "__VIEWSTATEGENERATOR": vsg["value"] if vsg else "",
        "__EVENTVALIDATION": ev["value"],
        "TbxToDate": format_dd_mm_yyyy(day),
        "BtnGenerate.x": "5",
        "BtnGenerate.y": "5",
    }
    r2 = session.post(
        BASE_URL,
        data=data,
        verify=certifi.where(),
        timeout=timeout,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    r2.raise_for_status()
    return r2.content


def is_xls_payload(content: bytes) -> bool:
    return content[:8] == XLS_MAGIC


def read_latest_price_date_from_merged_excel(path: Path) -> Optional[date]:
    """อ่านวันที่ล่าสุดจากไฟล์ Excel รวม (คอลัมน์ price_date). ไม่มีไฟล์/อ่านไม่ได้ → None."""
    if not path.is_file() or path.stat().st_size == 0:
        return None
    try:
        df = pd.read_excel(
            path,
            sheet_name=MERGED_SHEET,
            engine="openpyxl",
            usecols=["price_date"],
        )
    except (ValueError, KeyError, OSError):
        try:
            df = pd.read_excel(path, sheet_name=MERGED_SHEET, engine="openpyxl")
        except (OSError, ValueError):
            return None
    if df.empty or "price_date" not in df.columns:
        return None
    parsed = pd.to_datetime(df["price_date"], errors="coerce")
    parsed = parsed.dropna()
    if parsed.empty:
        return None
    ts = parsed.max()
    out = ts.date() if hasattr(ts, "date") else ts
    return out if isinstance(out, date) else None


def _align_merged_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "source_file" not in out.columns:
        out["source_file"] = ""
    for c in MERGED_COLUMNS:
        if c not in out.columns:
            out[c] = pd.NA
    return out[list(MERGED_COLUMNS)]


def _normalize_price_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.normalize()


def _write_merged_outputs(
    all_df: pd.DataFrame,
    excel_path: Path,
    sqlite_path: Path,
    csv_path: Optional[Path],
) -> int:
    all_df = all_df.copy()
    all_df.sort_values(["price_date", "product_name", "brand"], inplace=True)
    all_df.drop_duplicates(
        subset=["price_date", "product_name", "brand"], keep="last", inplace=True
    )
    all_df.reset_index(drop=True, inplace=True)
    all_df["price_date"] = pd.to_datetime(all_df["price_date"]).dt.date

    excel_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        all_df.to_excel(writer, sheet_name=MERGED_SHEET, index=False)

    if csv_path is not None:
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        all_df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(sqlite_path)
    try:
        all_df.to_sql("oil_prices", conn, if_exists="replace", index=False)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_oil_date ON oil_prices(price_date)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_oil_product ON oil_prices(product_name)"
        )
        conn.commit()
    finally:
        conn.close()
    return len(all_df)


def merge_and_export(
    raw_dir: Path,
    excel_path: Path,
    sqlite_path: Path,
    csv_path: Optional[Path] = None,
    incremental: bool = False,
) -> None:
    """
    รวมไฟล์ oil_price_*.xls → Excel + SQLite (+ CSV ถ้าระบุ)

    incremental=True: อ่านวันที่ล่าสุดจาก excel_path แล้วประมวลเฉพาะไฟล์ .xls ที่ชื่อไฟล์มีวันที่
    หลังวันนั้น จากนั้นต่อท้าย DataFrame เดิม (เร็วขึ้นเมื่อมีไฟล์จำนวนมาก)
    ถ้าไม่มีไฟล์รวมหรืออ่านวันที่ไม่ได้ จะรวมแบบเต็มเหมือน incremental=False
    """
    last: Optional[date] = None
    if incremental:
        last = read_latest_price_date_from_merged_excel(excel_path)
        if last is None:
            print(
                "โหมดต่อท้าย: ไม่พบข้อมูลวันที่ในไฟล์รวม — รวมจากทุกไฟล์ .xls แทน",
                flush=True,
            )
            incremental = False

    if incremental and last is not None:
        files_all = sorted(raw_dir.glob("oil_price_*.xls"))
        files = [
            f
            for f in files_all
            if (fd := date_from_oil_xls_filename(f)) is not None and fd > last
        ]
        if not files:
            print(
                f"โหมดต่อท้าย: ไม่มีไฟล์ .xls ใหม่หลังวันที่ {last} — ไม่เปลี่ยนไฟล์รวม",
                flush=True,
            )
            return

        print(
            f"โหมดต่อท้าย: วันที่ล่าสุดใน Excel = {last} → อ่านเฉพาะ {len(files)} ไฟล์ใหม่",
            flush=True,
        )

        parts: list[pd.DataFrame] = []
        for f in files:
            try:
                _, long_df = parse_xls_to_long(f)
            except Exception as e:
                print("ข้าม (อ่านไม่ได้):", f, e, file=sys.stderr)
                continue
            if long_df.empty:
                continue
            long_df = long_df.copy()
            long_df["source_file"] = f.name
            parts.append(long_df)

        if not parts:
            print("ไม่มีข้อมูลที่อ่านได้จากไฟล์ใหม่", file=sys.stderr)
            sys.exit(1)

        try:
            old_df = pd.read_excel(
                excel_path, sheet_name=MERGED_SHEET, engine="openpyxl"
            )
        except (OSError, ValueError) as e:
            print("อ่านไฟล์ Excel เดิมไม่ได้ — รวมแบบเต็มแทน:", e, file=sys.stderr)
            merge_and_export(
                raw_dir, excel_path, sqlite_path, csv_path=csv_path, incremental=False
            )
            return

        old_df = _align_merged_columns(old_df)
        new_df = pd.concat(parts, ignore_index=True)
        new_df = _align_merged_columns(new_df)
        old_df["price_date"] = _normalize_price_date_series(old_df["price_date"])
        new_df["price_date"] = _normalize_price_date_series(new_df["price_date"])
        all_df = pd.concat([old_df, new_df], ignore_index=True)
        nrows = _write_merged_outputs(all_df, excel_path, sqlite_path, csv_path)
        msg = f"ต่อท้ายแล้ว: รวมทั้งหมด {nrows} แถว → {excel_path}, {sqlite_path}"
        if csv_path is not None:
            msg += f", {csv_path}"
        print(msg)
        return

    files = sorted(raw_dir.glob("oil_price_*.xls"))
    if not files:
        print("ไม่พบไฟล์ .xls ใน", raw_dir, file=sys.stderr)
        sys.exit(1)

    parts_full: list[pd.DataFrame] = []
    for f in files:
        try:
            _, long_df = parse_xls_to_long(f)
        except Exception as e:
            print("ข้าม (อ่านไม่ได้):", f, e, file=sys.stderr)
            continue
        if long_df.empty:
            continue
        long_df = long_df.copy()
        long_df["source_file"] = f.name
        parts_full.append(long_df)

    if not parts_full:
        print("ไม่มีข้อมูลที่อ่านได้", file=sys.stderr)
        sys.exit(1)

    all_df = pd.concat(parts_full, ignore_index=True)
    all_df = _align_merged_columns(all_df)
    all_df["price_date"] = _normalize_price_date_series(all_df["price_date"])
    nrows = _write_merged_outputs(all_df, excel_path, sqlite_path, csv_path)

    msg = f"รวมแล้ว: {nrows} แถว → {excel_path}, {sqlite_path}"
    if csv_path is not None:
        msg += f", {csv_path}"
    print(msg)


def main() -> None:
    ap = argparse.ArgumentParser(description="ดึงราคาน้ำมัน สนพ. ตามวันที่ (PRICE ON / Generate)")
    ap.add_argument(
        "--start",
        default=None,
        metavar="YYYY-MM-DD",
        help=(
            "วันเริ่มดึง (บังคับช่วง) — ถ้าไม่ระบุ จะอ่านวันที่ล่าสุดจาก "
            "<output-dir>/oil_prices_merged.xlsx แล้วเริ่มดึงจากวันถัดไป "
            f"(ถ้าไม่มีไฟล์/ไม่มีข้อมูล ใช้ {DEFAULT_START_FALLBACK.isoformat()})"
        ),
    )
    ap.add_argument("--end", default=None, help="วันสุดท้าย (YYYY-MM-DD) ค่าเริ่มต้นคือวันนี้")
    ap.add_argument(
        "--output-dir",
        default="./eppo_oil_data",
        help="โฟลเดอร์เก็บไฟล์ .xls ดิบ",
    )
    ap.add_argument("--delay", type=float, default=0.8, help="หน่วงระหว่างคำขอ (วินาที)")
    ap.add_argument("--retries", type=int, default=3, help="จำนวนครั้งที่ลองใหม่เมื่อล้มเหลว")
    args = ap.parse_args()

    out_dir = Path(args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    excel_out = out_dir / "oil_prices_merged.xlsx"
    sqlite_out = out_dir / "oil_prices.db"

    end = date.fromisoformat(args.end) if args.end else date.today()

    if args.start is not None:
        start = date.fromisoformat(args.start)
        print(f"ใช้วันเริ่มตามที่ระบุ: {start}", flush=True)
    else:
        last = read_latest_price_date_from_merged_excel(excel_out)
        if last is not None:
            start = last + timedelta(days=1)
            print(
                f"อ่านจาก {excel_out.name}: วันที่ล่าสุดในข้อมูลรวม = {last} → ดึงตั้งแต่ {start}",
                flush=True,
            )
        else:
            start = DEFAULT_START_FALLBACK
            print(
                f"ไม่พบวันที่ในไฟล์รวม (หรือยังไม่มี {excel_out.name}) — ใช้วันเริ่มต้น {start}",
                flush=True,
            )

    if end < start:
        print(
            f"ข้ามการดึงจากเว็บ: ไม่มีวันที่ใหม่ให้ดึง (วันเริ่มคำนวณ {start} หลังวันสิ้นสุดช่วง {end}) — น่าจะอัปเดตครบแล้ว",
            flush=True,
        )
        if excel_out.is_file():
            # ปกติกรณีนี้คือข้อมูลอัปเดตครบแล้ว และอาจลบไฟล์ .xls ดิบเพื่อประหยัดพื้นที่
            # จึงไม่ควรพยายาม merge จาก .xls อีก
            print(f"ใช้ไฟล์รวมเดิม: {excel_out} (ไม่ทำ merge เพิ่ม)", flush=True)
            return
        print(
            f"ERROR: ไม่มีไฟล์รวม {excel_out.name} และไม่มีวันใหม่ให้ดึง — ไม่สามารถสร้างฐานข้อมูลได้",
            file=sys.stderr,
            flush=True,
        )
        raise SystemExit(1)

    ndays = (end - start).days + 1
    print(
        f"ช่วงวันที่: {start} ถึง {end} ({ndays} วัน) — โฟลเดอร์: {out_dir}",
        flush=True,
    )
    if ndays > 400:
        print(
            "คำเตือน: จำนวนวันมาก การดึงอาจใช้เวลานานและพื้นที่ดิสก์หลายร้อย MB",
            file=sys.stderr,
        )

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (compatible; EPPOOilFetcher/1.0; +research)",
            "Accept": "*/*",
        }
    )

    ok = 0
    fail = 0
    total = (end - start).days + 1
    for i, day in enumerate(daterange(start, end), start=1):
        fname = f"oil_price_{day.isoformat()}.xls"
        fpath = out_dir / fname
        if fpath.exists() and fpath.stat().st_size > 1000 and is_xls_payload(fpath.read_bytes()[:8]):
            ok += 1
            if i % 50 == 0 or i == 1:
                print(f"[{i}/{total}] ข้าม (มีไฟล์แล้ว): {fname}")
            continue

        content: Optional[bytes] = None
        last_err: Optional[BaseException] = None
        for attempt in range(args.retries):
            try:
                content = fetch_xls_for_date(session, day)
                if not is_xls_payload(content):
                    raise RuntimeError("ได้ข้อมูลที่ไม่ใช่ไฟล์ Excel (.xls)")
                break
            except Exception as e:
                last_err = e
                time.sleep(1.5 * (attempt + 1))

        if content is None:
            print(f"[{i}/{total}] ล้มเหลว {day}: {last_err}", file=sys.stderr)
            fail += 1
            time.sleep(args.delay)
            continue

        fpath.write_bytes(content)
        ok += 1
        if i % 20 == 0 or i == 1:
            print(f"[{i}/{total}] บันทึก {fname} ({len(content)} bytes)")

        time.sleep(args.delay)

    print(f"สรุปดึงข้อมูล: สำเร็จ {ok}, ล้มเหลว {fail} (รวม {total} วัน)")
    # ใช้ incremental เสมอเพื่อรองรับกรณีเก็บเฉพาะไฟล์รวม (ประหยัดพื้นที่)
    merge_and_export(out_dir, excel_out, sqlite_out, incremental=True)


if __name__ == "__main__":
    main()
