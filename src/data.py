from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd


@dataclass(frozen=True)
class DataQualityReport:
    rows_raw: int
    rows_clean: int
    rows_dropped_missing_price: int
    rows_dropped_invalid_date: int
    rows_deduped: int
    min_date: pd.Timestamp | None
    max_date: pd.Timestamp | None


EXPECTED_COLUMNS = ["publish_date", "fuel_name", "company", "price"]


def _coerce_to_standard_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize known alternate schemas (e.g. EPPO merged long format) to EXPECTED_COLUMNS.
    EPPO-style: price_date, product_name, brand, price_baht_per_litre.
    """
    out = df.copy()
    rename: dict[str, str] = {}
    if "publish_date" not in out.columns and "price_date" in out.columns:
        rename["price_date"] = "publish_date"
    if "fuel_name" not in out.columns and "product_name" in out.columns:
        rename["product_name"] = "fuel_name"
    if "company" not in out.columns and "brand" in out.columns:
        rename["brand"] = "company"
    if "price" not in out.columns and "price_baht_per_litre" in out.columns:
        rename["price_baht_per_litre"] = "price"
    return out.rename(columns=rename)


def _normalize_company(s: pd.Series) -> pd.Series:
    s = s.astype("string").str.strip()
    # Keep as-is but standardize a few common variants
    s = s.replace(
        {
            "PTT Public Company Limited": "PTT",
            "Bangchak": "BCP",
            "Bangchak Petroleum": "BCP",
        }
    )
    return s


def _normalize_fuel_name(s: pd.Series) -> pd.Series:
    s = s.astype("string").str.strip()
    s = s.str.replace(r"\s+", " ", regex=True)
    return s


def _attach_fuel_group(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prefer standardized group from cleaner (fuel_type_group); fall back to fuel_name.
    """
    if "fuel_type_group" in df.columns:
        g = df["fuel_type_group"].astype("string").str.strip()
        g = g.str.replace(r"\s+", " ", regex=True)
        g = g.mask(g.eq(""))
    else:
        g = pd.Series(pd.NA, index=df.index, dtype="string")
    name = df["fuel_name"].astype("string").str.strip()
    df["fuel_group"] = g.fillna(name)
    return df


def load_raw_excel(path: str) -> pd.DataFrame:
    xl = pd.ExcelFile(path, engine="openpyxl")
    last_cols: list[str] = []
    for sheet in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name=sheet)
        df = _coerce_to_standard_columns(df)
        missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
        if not missing:
            return df
        last_cols = list(df.columns)
    raise ValueError(
        f"Missing expected columns (need {EXPECTED_COLUMNS}). "
        f"Last sheet tried had: {last_cols}"
    )


def clean_fuel_prices(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, DataQualityReport]:
    df = df_raw.copy()
    rows_raw = len(df)

    df["publish_date"] = pd.to_datetime(df["publish_date"], errors="coerce")
    rows_dropped_invalid_date = int(df["publish_date"].isna().sum())
    df = df.dropna(subset=["publish_date"])

    df["company"] = _normalize_company(df["company"])
    df["fuel_name"] = _normalize_fuel_name(df["fuel_name"])
    df = _attach_fuel_group(df)
    fg = df["fuel_group"].astype("string")
    df = df[fg.notna() & fg.str.strip().ne("")].copy()

    # Price can be missing in source; keep only usable rows for charts
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    rows_dropped_missing_price = int(df["price"].isna().sum())
    df = df.dropna(subset=["price"])

    df["price"] = df["price"].astype("float64")

    # Remove duplicates by key (same day, same company, same fuel group)
    before = len(df)
    df = (
        df.sort_values(["publish_date", "company", "fuel_group", "price"])
        .drop_duplicates(subset=["publish_date", "company", "fuel_group"], keep="last")
        .reset_index(drop=True)
    )
    rows_deduped = before - len(df)

    df = df.sort_values(["publish_date", "fuel_group", "company"]).reset_index(drop=True)

    report = DataQualityReport(
        rows_raw=rows_raw,
        rows_clean=len(df),
        rows_dropped_missing_price=rows_dropped_missing_price,
        rows_dropped_invalid_date=rows_dropped_invalid_date,
        rows_deduped=rows_deduped,
        min_date=(df["publish_date"].min() if len(df) else None),
        max_date=(df["publish_date"].max() if len(df) else None),
    )
    return df, report


def filter_df(
    df: pd.DataFrame,
    date_start: pd.Timestamp,
    date_end: pd.Timestamp,
    companies: Iterable[str],
    fuel_groups: Iterable[str],
) -> pd.DataFrame:
    companies = list(companies)
    fuel_groups = list(fuel_groups)
    m = (df["publish_date"] >= date_start) & (df["publish_date"] <= date_end)
    if companies:
        m &= df["company"].isin(companies)
    if fuel_groups:
        m &= df["fuel_group"].isin(fuel_groups)
    return df.loc[m].copy()

