#!/usr/bin/env python3
"""
Clean and standardize fuel type names for the Thailand fuel price dataset.

Inputs (load & combine before cleaning):
  - oil_prices_merged.xlsx (primary; long schema: price_date, product_name, brand, …)
  - fuel_prices_th_2004_6apr2026.xlsx (optional base history; same schema as before)

Duplicate / update rule:
  Rows that share the same business key (publish_date, fuel_name, company) are treated
  as one observation: the last row wins (typically the row from oil_prices_merged after
  concat). This is “update” semantics — not blind merge of duplicate keys.

Outputs:
  - fuel_prices_cleaned.xlsx
  - fuel_name_mapping_review.csv
"""

from __future__ import annotations

import re
import sys
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd


# Primary input (merged / incremental updates)
INPUT_MERGED_XLSX = "oil_prices_merged.xlsx"

# Optional longer history; if the file exists it is loaded first, then merged rows are
# appended so duplicate keys resolve to merged data (keep="last").
INPUT_LEGACY_XLSX = "fuel_prices_th_2004_6apr2026.xlsx"
INCLUDE_LEGACY_BASE_IF_EXISTS = True

# oil_prices_merged.xlsx → canonical column names (aligned with legacy export)
MERGED_COLUMN_MAP = {
    "price_date": "publish_date",
    "product_name": "fuel_name",
    "brand": "company",
    "price_baht_per_litre": "price",
}

# Business key for “update” deduplication (last row wins)
DEDUPE_KEY_COLUMNS = ["publish_date", "fuel_name", "company"]

OUTPUT_XLSX = "fuel_prices_cleaned.xlsx"
REVIEW_CSV = "fuel_name_mapping_review.csv"
# เคยเขียน fuel_prices_cleaned.csv — ไม่ใช้ใน pipeline / เว็บ; ลบซากหลังรัน
LEGACY_CLEANED_CSV = "fuel_prices_cleaned.csv"


# ----------------------------
# Core cleaning/classification
# ----------------------------
STANDARD_GROUP_UNKNOWN = "Unknown"

# Values to ignore/remove entirely (after normalization)
IGNORE_VALUES = {
    "มีผลตั้งแต่ (effective date)",
    "มีผลตั้งแต่(effective date)",
    "effective date",
}

_DASHES_RE = re.compile(r"[\u2010\u2011\u2012\u2013\u2014\u2212\uFE58\uFE63\uFF0D]")
_MULTISPACE_RE = re.compile(r"[ \t\u00A0\u2009\u202F]+")
_LINEBREAKS_RE = re.compile(r"[\r\n]+")
_SPACE_AROUND_PARENS_RE = [
    (re.compile(r"\(\s+"), "("),
    (re.compile(r"\s+\)"), ")"),
]
_SPACES_AROUND_DASH_RE = re.compile(r"\s*-\s*")


def normalize_text(value: object) -> str:
    """
    Normalize fuel type text:
    - trim whitespace
    - replace line breaks with spaces
    - collapse multiple spaces into one
    - normalize dash / hyphen
    - normalize spacing inside parentheses
    - remove obvious header noise such as "มีผลตั้งแต่ (Effective Date)" if it appears
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""

    s = str(value)
    s = unicodedata.normalize("NFKC", s)
    s = _LINEBREAKS_RE.sub(" ", s)
    s = _DASHES_RE.sub("-", s)

    # Normalize spacing around hyphen, but avoid destroying hyphenated codes too much
    s = _SPACES_AROUND_DASH_RE.sub(" - ", s)

    for rx, rep in _SPACE_AROUND_PARENS_RE:
        s = rx.sub(rep, s)

    s = _MULTISPACE_RE.sub(" ", s).strip()

    # Remove leading header noise if embedded
    s_cf = s.casefold()
    if "มีผลตั้งแต่" in s_cf and "effective date" in s_cf:
        # If the value contains both, it's almost certainly a header; drop it.
        return ""

    if s_cf in IGNORE_VALUES:
        return ""

    return s


# ----------------------------
# Mapping logic (editable)
# ----------------------------
def _norm_key(s: str) -> str:
    """
    Create a canonical key for exact alias matching.
    Keep this consistent with normalize_text() so the mapping is stable.
    """
    return normalize_text(s).casefold()


ALIAS_TO_GROUP: dict[str, str] = {
    # Group: Diesel B5
    _norm_key("Biodiesel (B5)"): "Diesel B5",
    _norm_key("ดีเซลหมุนเร็ว บี5 (HSD - B5)"): "Diesel B5",
    _norm_key("ดีเซลหมุนเร็วบี5 (B5)"): "Diesel B5",
    # Group: Fuel Oil 1500
    _norm_key("FO 1500 (2) 2%S"): "Fuel Oil 1500",
    _norm_key("น้ำมันเตา ชนิดที่ 2 (FUEL 1500 2%S)"): "Fuel Oil 1500",
    # Group: Fuel Oil 600
    _norm_key("FO 600 (1) 2%S"): "Fuel Oil 600",
    _norm_key("น้ำมันเตา ชนิดที่ 1 (FUEL 600 2%S)"): "Fuel Oil 600",
    # Group: Gasohol 91 E10
    _norm_key("GASOHOL91"): "Gasohol 91 E10",
    _norm_key("แก๊สโซฮอล ออกเทน 91 (Gasohol 91 - E10)"): "Gasohol 91 E10",
    _norm_key("แก๊สโซฮอล ออกเทน 91 (Gasohol 91-E10)"): "Gasohol 91 E10",
    _norm_key("แก๊สโซฮอล์ออกเทน 91 (GASOHOL)"): "Gasohol 91 E10",
    # Group: Gasohol 95 E10
    _norm_key("GASOHOL95 E10"): "Gasohol 95 E10",
    _norm_key("แก๊สโซฮอล ออกเทน 95 (Gasohol 95 - E10)"): "Gasohol 95 E10",
    _norm_key("แก๊สโซฮอล ออกเทน 95 (Gasohol 95-E10)"): "Gasohol 95 E10",
    _norm_key("แก๊สโซฮอล์ออกเทน 95 (E10)"): "Gasohol 95 E10",
    _norm_key("แก๊สโซฮอล์ออกเทน 95 (GASOHOL)"): "Gasohol 95 E10",
    _norm_key("โซฮอล ออกเทน 95 (Gasohol 95-E10)"): "Gasohol 95 E10",
    # Group: Gasohol 95 E20
    _norm_key("GASOHOL95 E20"): "Gasohol 95 E20",
    _norm_key("แก๊สโซฮอล ออกเทน 95 (Gasohol 95 - E20)"): "Gasohol 95 E20",
    _norm_key("แก๊สโซฮอล ออกเทน 95 (Gasohol 95-E20)"): "Gasohol 95 E20",
    _norm_key("แก๊สโซฮอล์ออกเทน 95 (E20)"): "Gasohol 95 E20",
    # Group: Gasohol 95 E85
    _norm_key("GASOHOL95 E85"): "Gasohol 95 E85",
    _norm_key("แก๊สโซฮอล ออกเทน 95 (Gasohol 95 - E85)"): "Gasohol 95 E85",
    _norm_key("แก๊สโซฮอล ออกเทน 95 (Gasohol 95-E85)"): "Gasohol 95 E85",
    # Group: H-Diesel 0.035%S
    _norm_key("H-DIESEL(0.035%S)"): "H-Diesel 0.035%S",
    _norm_key("ดีเซลหมุนเร็ว (HSD, 0.035%S)"): "H-Diesel 0.035%S",
    # Group: H-Diesel 0.7%S
    _norm_key("H-DIESEL(0.7%S)"): "H-Diesel 0.7%S",
    # Group: Kerosene
    _norm_key("KEROSENE"): "Kerosene",
    _norm_key("น้ำมันก๊าด (KERO)"): "Kerosene",
    # Group: LSD
    _norm_key("L-DIESEL"): "LSD",
    _norm_key("ดีเซลหมุนช้า (LSD)"): "LSD",
    # Group: LPG
    _norm_key("LPG (B/KG.)"): "LPG",
    # Group: LPG Auto
    _norm_key("ก๊าซสำหรับยานพาหนะ (LPG-AUTO)"): "LPG Auto",
    # Group: Benzine 91
    _norm_key("ULG 91R ; UNL"): "Benzine 91",
    _norm_key("เบนซิน ออกเทน 91 (UGR 91 RON)"): "Benzine 91",
    _norm_key("เบนซินออกเทน 91 (UGR 91 RON)"): "Benzine 91",
    # Group: Benzine 95
    _norm_key("ULG 95R ; UNL"): "Benzine 95",
    _norm_key("เบนซิน ออกเทน 95 (ULG 95 RON)"): "Benzine 95",
    _norm_key("เบนซินออกเทน  95 (ULG 95 RON)"): "Benzine 95",
    _norm_key("เบนซินออกเทน 95 (ULG 95 RON)"): "Benzine 95",
    # Group: Premium Gasohol 95
    _norm_key("แก๊สโซฮอล ออกเทน 95 พรีเมียม"): "Premium Gasohol 95",
    # Group: Premium Benzine 95
    _norm_key("เบนซิน ออกเทน 95 พรีเมียม"): "Premium Benzine 95",
    # Group: Palm Diesel
    _norm_key("ดีเซล - ปาล์ม (บริสุทธิ์)"): "Palm Diesel",
    # Group: HSD 0.05%S
    _norm_key("ดีเซลหมุนเร็ว (HSD 0.05% S)"): "HSD 0.05%S",
    _norm_key("ดีเซลหมุนเร็ว (HSD, 0.05%S)"): "HSD 0.05%S",
    # Group: HSD 0.005%S
    _norm_key("ดีเซลหมุนเร็ว (HSD, 0.005%S)"): "HSD 0.005%S",
    # Group: HSD 0.35%S
    _norm_key("ดีเซลหมุนเร็ว (HSD, 0.35%S)"): "HSD 0.35%S",
    # Group: Diesel B10
    _norm_key("ดีเซลหมุนเร็ว HSD B10"): "Diesel B10",
    # Group: Diesel B20
    _norm_key("ดีเซลหมุนเร็ว HSD B20"): "Diesel B20",
    # Group: Diesel B7
    _norm_key("ดีเซลหมุนเร็ว HSD B7"): "Diesel B7",
    # Group: Premium Diesel
    _norm_key("ดีเซลหมุนเร็ว พรีเมียม"): "Premium Diesel",
    _norm_key("พรีเมียมดีเซล"): "Premium Diesel",
    # Group: Fuel Oil 2500
    _norm_key("น้ำมันเตา ชนิดที่ 4 (FUEL 2500)"): "Fuel Oil 2500",
}


REGEX_FALLBACKS: list[tuple[re.Pattern[str], str]] = [
    # Gasohol variants
    (re.compile(r"\bgasohol\s*91\b", re.I), "Gasohol 91 E10"),
    (re.compile(r"\bgasohol\s*95\b.*\be10\b", re.I), "Gasohol 95 E10"),
    (re.compile(r"\bgasohol\s*95\b.*\be20\b", re.I), "Gasohol 95 E20"),
    (re.compile(r"\bgasohol\s*95\b.*\be85\b", re.I), "Gasohol 95 E85"),
    # FO variants
    (re.compile(r"\bfo\s*600\b", re.I), "Fuel Oil 600"),
    (re.compile(r"\bfo\s*1500\b", re.I), "Fuel Oil 1500"),
    (re.compile(r"\bfuel\s*600\b", re.I), "Fuel Oil 600"),
    (re.compile(r"\bfuel\s*1500\b", re.I), "Fuel Oil 1500"),
    (re.compile(r"\bfuel\s*2500\b", re.I), "Fuel Oil 2500"),
    # LPG variants
    (re.compile(r"\blpg\b", re.I), "LPG"),
    # Kerosene
    (re.compile(r"\bkerosene\b", re.I), "Kerosene"),
    (re.compile(r"\bkero\b", re.I), "Kerosene"),
    # Diesel sulfur grades
    (re.compile(r"h-?diesel\s*\(\s*0\.035\s*%?\s*s\s*\)", re.I), "H-Diesel 0.035%S"),
    (re.compile(r"h-?diesel\s*\(\s*0\.7\s*%?\s*s\s*\)", re.I), "H-Diesel 0.7%S"),
    # Biodiesel blends (safe fallback only; exact aliases take priority)
    (re.compile(r"\bb\s*5\b", re.I), "Diesel B5"),
    (re.compile(r"\bb\s*7\b", re.I), "Diesel B7"),
    (re.compile(r"\bb\s*10\b", re.I), "Diesel B10"),
    (re.compile(r"\bb\s*20\b", re.I), "Diesel B20"),
]


def classify_fuel_type(clean_text: str) -> str:
    """
    Map normalized (clean_text) into a standardized fuel_type_group.
    Rules:
      1) exact alias mapping (case-insensitive key)
      2) regex fallbacks for close variants
      3) Unknown
    """
    if not clean_text:
        return STANDARD_GROUP_UNKNOWN

    key = clean_text.casefold()
    if key in ALIAS_TO_GROUP:
        return ALIAS_TO_GROUP[key]

    for rx, group in REGEX_FALLBACKS:
        if rx.search(clean_text):
            return group

    return STANDARD_GROUP_UNKNOWN


def build_mapping_review(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a review mapping table of unique raw->clean->group with counts.
    """
    out = (
        df.groupby(["fuel_type_raw", "fuel_type_clean", "fuel_type_group"], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(["count", "fuel_type_group", "fuel_type_clean", "fuel_type_raw"], ascending=[False, True, True, True])
    )
    return out


# ----------------------------
# Fuel column detection
# ----------------------------
@dataclass(frozen=True)
class DetectedFuelColumn:
    sheet_name: str
    column_name: str
    score: float


def _score_candidate_fuel_column(col_name: str, series: pd.Series) -> float:
    """
    Score a candidate column being the fuel type column.
    Higher is better.
    """
    name_cf = str(col_name).casefold()
    score = 0.0

    # Column name hints
    if any(k in name_cf for k in ["fuel", "product", "type", "name", "grade"]):
        score += 3.0
    if any(k in name_cf for k in ["ชื่อ", "น้ำมัน", "เชื้อเพลิง"]):
        score += 3.0
    if name_cf in {"fuel_name", "fuel", "fueltype", "fuel_type"}:
        score += 5.0

    # Content hints
    s = series.dropna()
    if len(s) == 0:
        return -1.0

    # Prefer object/string columns
    if series.dtype == object:
        score += 2.0

    vals = s.astype(str).head(200).map(normalize_text)
    vals = vals[vals != ""]
    if len(vals) == 0:
        return -1.0

    unique = vals.unique()
    unique_n = len(unique)

    # Fuel name columns usually have moderate cardinality (not too low/high)
    if 2 <= unique_n <= 200:
        score += 2.0

    # Boost if many values map to known groups
    mapped = 0
    for v in unique[:150]:
        grp = classify_fuel_type(v)
        if grp != STANDARD_GROUP_UNKNOWN:
            mapped += 1
    score += (mapped / max(1, min(len(unique), 150))) * 5.0

    # Penalize date-like columns
    if any(k in name_cf for k in ["date", "วันที่", "เวลา", "time"]):
        score -= 3.0

    # Penalize numeric-ish columns
    try:
        pd.to_numeric(vals.head(50), errors="coerce").notna().mean()
        numeric_ratio = pd.to_numeric(vals.head(50), errors="coerce").notna().mean()
        if numeric_ratio > 0.8:
            score -= 4.0
    except Exception:
        pass

    return score


def detect_fuel_type_column(xlsx_path: str | Path) -> DetectedFuelColumn:
    """
    Detect the fuel type column by scanning sheets and scoring columns.
    """
    xl = pd.ExcelFile(xlsx_path)
    best: Optional[DetectedFuelColumn] = None

    for sh in xl.sheet_names:
        # Read a small sample for detection (fast)
        sample = pd.read_excel(xlsx_path, sheet_name=sh, nrows=300)
        for col in sample.columns:
            series = sample[col]
            sc = _score_candidate_fuel_column(col, series)
            cand = DetectedFuelColumn(sheet_name=sh, column_name=str(col), score=float(sc))
            if best is None or cand.score > best.score:
                best = cand

    if best is None:
        raise RuntimeError("Could not detect any columns in the Excel file.")
    return best


_NON_FUEL_COLUMNS_FOR_DETECTION = frozenset(
    {
        "publish_date",
        "company",
        "price",
        "source_file",
        "fuel_type_raw",
        "fuel_type_clean",
        "fuel_type_group",
    }
)


def detect_fuel_type_column_from_dataframe(df: pd.DataFrame, nrows: int = 300) -> DetectedFuelColumn:
    """
    Same scoring as Excel-based detection, but on an in-memory DataFrame.
    """
    sample = df.head(nrows)
    best: Optional[DetectedFuelColumn] = None
    for col in sample.columns:
        col_str = str(col)
        if col_str in _NON_FUEL_COLUMNS_FOR_DETECTION:
            continue
        series = sample[col]
        sc = _score_candidate_fuel_column(col, series)
        cand = DetectedFuelColumn(sheet_name="<dataframe>", column_name=col_str, score=float(sc))
        if best is None or cand.score > best.score:
            best = cand
    if best is None:
        raise RuntimeError("Could not detect a fuel type column in the DataFrame.")
    return best


def read_and_standardize_workbook(path: Path) -> pd.DataFrame:
    """
    Read the first worksheet from an Excel workbook and map to canonical columns
    when the merged long schema is detected.
    """
    xl = pd.ExcelFile(path)
    sheet0 = xl.sheet_names[0]
    df = pd.read_excel(path, sheet_name=sheet0)

    if "product_name" in df.columns:
        df = df.rename(columns=dict(MERGED_COLUMN_MAP))

    required = ["publish_date", "fuel_name", "company", "price"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{path}: missing columns after standardize: {missing} (have: {list(df.columns)})")

    df["publish_date"] = pd.to_datetime(df["publish_date"], errors="coerce")
    return df


def combine_with_update_semantics(frames: list[pd.DataFrame]) -> tuple[pd.DataFrame, int, int, int]:
    """
    Stack frames in order; for duplicate keys keep the last row (update / overlay).

    Returns:
      (deduped_df, rows_before, rows_after, dup_keys_dropped)
    """
    if not frames:
        raise ValueError("No frames to combine.")
    df = pd.concat(frames, ignore_index=True)
    before = len(df)
    for col in DEDUPE_KEY_COLUMNS:
        if col not in df.columns:
            raise ValueError(f"Missing dedupe key column: {col!r}")

    df = df.drop_duplicates(subset=DEDUPE_KEY_COLUMNS, keep="last", ignore_index=True)
    after = len(df)
    return df, before, after, before - after


def main(argv: list[str]) -> int:
    merged_path = Path(argv[1]) if len(argv) > 1 else Path(INPUT_MERGED_XLSX)
    if not merged_path.exists():
        print(f"ERROR: merged input not found: {merged_path}", file=sys.stderr)
        return 2

    legacy_path = Path(argv[2]) if len(argv) > 2 else Path(INPUT_LEGACY_XLSX)

    print(f"Loading merged workbook: {merged_path}")
    df_merged = read_and_standardize_workbook(merged_path)

    frames: list[pd.DataFrame] = []
    if INCLUDE_LEGACY_BASE_IF_EXISTS and legacy_path.exists():
        print(f"Loading legacy base (keys duplicate → merged wins): {legacy_path}")
        df_legacy = read_and_standardize_workbook(legacy_path)
        frames.append(df_legacy)
    elif INCLUDE_LEGACY_BASE_IF_EXISTS:
        print(f"Note: legacy base not found, using merged only: {legacy_path}")

    frames.append(df_merged)

    df, rows_before, rows_after, n_dup = combine_with_update_semantics(frames)
    print(
        f"Update dedupe (publish_date+fuel_name+company, keep=last): "
        f"{rows_before} → {rows_after} rows ({n_dup} duplicate key row(s) replaced)"
    )

    if "fuel_name" in df.columns:
        fuel_col = "fuel_name"
        print(f"Using fuel column: {fuel_col!r}")
    else:
        detected = detect_fuel_type_column_from_dataframe(df)
        fuel_col = detected.column_name
        print(f"Detected fuel type column: {fuel_col!r}, score={detected.score:.2f}")

    # Preserve raw
    df["fuel_type_raw"] = df[fuel_col].astype(object)

    # Clean intermediate
    df["fuel_type_clean"] = df["fuel_type_raw"].map(normalize_text)

    # Drop rows where fuel_type_clean is empty (noise)
    # Keep the row otherwise intact.
    df["fuel_type_group"] = df["fuel_type_clean"].map(classify_fuel_type)

    # Summary
    total_rows = len(df)
    total_unique_raw = df["fuel_type_raw"].dropna().astype(str).nunique()
    total_groups = df["fuel_type_group"].nunique()

    unmapped = (
        df.loc[df["fuel_type_group"].eq(STANDARD_GROUP_UNKNOWN) & df["fuel_type_clean"].ne(""), "fuel_type_clean"]
        .dropna()
        .value_counts()
    )

    print("\nSummary")
    print(f"  total rows: {total_rows}")
    print(f"  total unique raw fuel names: {total_unique_raw}")
    print(f"  total standardized groups: {total_groups}")
    print(f"  unmapped unique names (non-empty): {len(unmapped)}")
    if len(unmapped) > 0:
        print("  top unmapped (up to 30):")
        for name, cnt in unmapped.head(30).items():
            print(f"    - {name} ({cnt})")

    # Exports
    out_xlsx = Path(OUTPUT_XLSX)
    review_csv = Path(REVIEW_CSV)

    # Mapping review
    review = build_mapping_review(df)
    review.to_csv(review_csv, index=False, encoding="utf-8-sig")

    # Cleaned dataset (เว็บ / update_fuel_data ใช้แค่ .xlsx)
    df.to_excel(out_xlsx, index=False)

    legacy_csv = Path(LEGACY_CLEANED_CSV)
    if legacy_csv.is_file():
        try:
            legacy_csv.unlink()
            print(f"\nRemoved unused legacy file: {legacy_csv}")
        except OSError as e:
            print(f"\nWarning: could not remove {legacy_csv}: {e}", file=sys.stderr)

    print("\nWrote outputs")
    print(f"  - {out_xlsx}")
    print(f"  - {review_csv}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

