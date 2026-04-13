from __future__ import annotations

import html
import os
import pathlib
import subprocess
import sys
from datetime import date, datetime

import pandas as pd
import plotly.express as px
import streamlit as st
import streamlit.components.v1 as components

from src.data import clean_fuel_prices, filter_df, load_raw_excel
from src.insights import (
    NOTE_TWELVE_DAY_WAR_TH,
    build_insights_dict,
    build_thai_summary,
    daily_mean_by_group,
    events_in_range,
    generate_executive_summary,
    max_price_latest_by_type,
    min_max_in_period_by_group,
    normalize_series,
    pct_change_period_by_group,
    strongest_move_window,
    trend_latest,
    yoy_mom_for_groups,
)


APP_TITLE = "Thailand Fuel Price Monitor"
CREDIT_BYLINE = "By Dr.Akeky"
DATA_FILE = "fuel_prices_cleaned.xlsx"

ROOT_DIR = pathlib.Path(__file__).resolve().parent
UPDATE_SCRIPT = ROOT_DIR / "update_fuel_data.py"
# บันทึกชุดข้อมูลหลังประมวลผล (ใช้บนกราฟ) ทับไฟล์เดิม — กำหนด path เองได้ด้วย env
DEFAULT_EXPORT_CSV = "fuel_prices_dashboard_export.csv"


def _dashboard_export_path() -> pathlib.Path:
    raw = os.environ.get("FUEL_DASHBOARD_EXPORT_PATH", "").strip()
    if raw:
        path = pathlib.Path(raw).expanduser()
        return path if path.is_absolute() else (ROOT_DIR / path)
    return ROOT_DIR / DEFAULT_EXPORT_CSV


def _maybe_auto_export_csv(df: pd.DataFrame, source_mtime_ns: int) -> pathlib.Path | None:
    """เขียนทับ CSV เมื่อไฟล์ต้นทาง (fuel_prices_cleaned.xlsx) เปลี่ยน — ไม่เขียนซ้ำทุกครั้งที่กด widget"""
    key = "_dashboard_export_mtime_ns"
    if st.session_state.get(key) == source_mtime_ns:
        return None
    out = _dashboard_export_path()
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False, encoding="utf-8-sig")
    st.session_state[key] = source_mtime_ns
    return out


def _auto_update_enabled() -> bool:
    # ค่าเริ่มต้น 0 = หน้าเว็บไม่ดึงข้อมูลทุกครั้งที่โหลด (แนะนำให้ใช้ cron + scripts/daily_update.sh)
    v = os.environ.get("AUTO_UPDATE_FUEL_DATA", "0").strip().lower()
    return v not in ("0", "false", "no", "off")


def _ga4_measurement_id() -> str:
    mid = os.environ.get("GA4_MEASUREMENT_ID", "").strip()
    try:
        if not mid:
            mid = str(st.secrets.get("ga4", {}).get("measurement_id", "")).strip()
    except Exception:
        pass
    return mid


def _inject_ga4() -> None:
    """Inject GA4 tracker (optional; enabled when GA4_MEASUREMENT_ID is set)."""
    mid = _ga4_measurement_id()
    if not mid:
        return
    components.html(
        f"""
<!-- Google tag (gtag.js) -->
<script async src="https://www.googletagmanager.com/gtag/js?id={mid}"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){{dataLayer.push(arguments);}}
  gtag('js', new Date());
  gtag('config', '{mid}', {{'anonymize_ip': true}});
</script>
""",
        height=0,
        width=0,
    )


def _clamp_date_range_tuple(
    rng: tuple | date | None,
    min_d: date,
    max_d: date,
    fallback: tuple[date, date],
) -> tuple[date, date]:
    """จำกัดช่วงวันที่ให้อยู่ใน [min_d, max_d] (กรณีข้อมูลในฐานเปลี่ยน)"""
    if isinstance(rng, tuple) and len(rng) == 2:
        s, e = rng[0], rng[1]
    elif isinstance(rng, date):
        s, e = rng, rng
    else:
        return fallback
    s = max(s, min_d)
    e = min(e, max_d)
    if s > e:
        return fallback
    return (s, e)


def _preset_date_range(end: date, min_d: date, preset: str) -> tuple[date, date]:
    """ช่วงสิ้นสุดที่ end (วันล่าสุดในฐาน) ย้อนหลังตาม preset — clamp ที่ min_d"""
    te = pd.Timestamp(end)
    if preset == "1d":
        start = end
    elif preset == "3d":
        start = (te - pd.Timedelta(days=2)).date()
    elif preset == "7d":
        start = (te - pd.Timedelta(days=6)).date()
    elif preset == "1m":
        start = (te - pd.DateOffset(months=1)).date()
    elif preset == "3m":
        start = (te - pd.DateOffset(months=3)).date()
    elif preset == "6m":
        start = (te - pd.DateOffset(months=6)).date()
    elif preset == "1y":
        start = (te - pd.DateOffset(years=1)).date()
    elif preset == "3y":
        start = (te - pd.DateOffset(years=3)).date()
    elif preset == "5y":
        start = (te - pd.DateOffset(years=5)).date()
    else:
        start = end
    if start < min_d:
        start = min_d
    if start > end:
        start = end
    return (start, end)


def _active_time_preset_key(ds: date, de: date, max_d: date, min_d: date) -> str | None:
    """ถ้าช่วงวันที่ตรงกับผลของปุ่ม preset ใด ๆ (เทียบกับวันสุดท้ายในฐาน) คืนรหัส preset — ไม่ตรงเลยคืน None (เลือกช่วงเอง)"""
    for pk in ("1d", "3d", "7d", "1m", "3m", "6m", "1y", "3y", "5y"):
        s, e = _preset_date_range(max_d, min_d, pk)
        if (ds, de) == (s, e):
            return pk
    return None


# ชื่อ fuel_group ในฐาน (คอลัมน์ fuel_group) — ปุ่มเลือกด่วน
FUEL_QUICK_PRESETS: tuple[str, ...] = (
    "Diesel B7",
    "Gasohol 91 E10",
    "Gasohol 95 E10",
    "Gasohol 95 E20",
    "Gasohol 95 E85",
)


def _resolve_fuel_group_name(groups_all: list[str], want: str) -> str | None:
    """คืนชื่อจริงใน groups_all ถ้ามี (รองรับตัวพิมพ์เล็ก/ใหญ่)"""
    if want in groups_all:
        return want
    by_lower = {g.lower(): g for g in groups_all}
    return by_lower.get(want.lower())


def _toggle_in_list(current: list[str], item: str) -> list[str]:
    """กดปุ่มด่วน: ยังไม่มีในรายการ → เพิ่ม; มีแล้ว → เอาออก (multi / toggle)"""
    cur = list(current)
    if item in cur:
        return [x for x in cur if x != item]
    return cur + [item]


def _toggle_fuel_group_selection(current: list[str], resolved: str) -> list[str]:
    return _toggle_in_list(current, resolved)


def _invalidate_fuel_group_checkbox_keys(n_groups: int) -> None:
    """ล้าง key ของ checkbox ใน popover เพื่อให้ซิงค์กับ fuel_groups_sel หลังกดปุ่มด่วน"""
    for i in range(max(n_groups, 0) + 32):
        st.session_state.pop(f"fgp_cb_{i}", None)


def _invalidate_company_checkbox_keys(n_companies: int) -> None:
    for i in range(max(n_companies, 0) + 32):
        st.session_state.pop(f"cop_cb_{i}", None)


# ปุ่มด่วนบริษัท — ชื่อต้องตรงกับคอลัมน์ company ในฐาน
COMPANY_QUICK_PRESETS: tuple[str, ...] = (
    "PTT",
    "BCP",
    "Shell",
    "Chevron",
    "IRPC",
)


def _resolve_company_name(companies_all: list[str], want: str) -> str | None:
    if want in companies_all:
        return want
    by_lower = {c.lower(): c for c in companies_all}
    return by_lower.get(want.lower())


def _default_companies_preset(companies_all: list[str], fallback_ranked: list[str]) -> list[str]:
    """ค่าเริ่มต้นครั้งแรก / หลังกดช่วงเวลา: PTT, BCP, Shell ถ้ามีในฐาน — ไม่ครบให้ fallback"""
    picked = [c for c in ("PTT", "BCP", "Shell") if c in companies_all]
    if picked:
        return picked
    return fallback_ranked[:4] if fallback_ranked else []


REFERENCE_PRICE_COMPANY = "PTT"


def _ptt_popular_fuel_rows(df: pd.DataFrame) -> tuple[list[dict | None], pd.Timestamp | None]:
    """
    ราคาล่าสุดต่อกลุ่มตาม FUEL_QUICK_PRESETS สำหรับ PTT จากทั้งฐาน
    คืนรายการยาวเท่า FUEL_QUICK_PRESETS — แต่ละช่องเป็น None ถ้าไม่มีข้อมูล
    """
    d = df.copy()
    d["publish_date"] = pd.to_datetime(d["publish_date"])
    ptt = d.loc[d["company"] == REFERENCE_PRICE_COMPANY]
    if ptt.empty:
        return [None] * len(FUEL_QUICK_PRESETS), None
    groups_in_data = sorted(ptt["fuel_group"].dropna().unique().tolist())
    out: list[dict | None] = []
    refs: list[pd.Timestamp] = []
    for want in FUEL_QUICK_PRESETS:
        fg = _resolve_fuel_group_name(groups_in_data, want)
        if fg is None:
            out.append(None)
            continue
        sub = ptt.loc[ptt["fuel_group"] == fg]
        if sub.empty:
            out.append(None)
            continue
        sub = sub.sort_values("publish_date")
        last = sub.iloc[-1]
        pdt = pd.Timestamp(last["publish_date"]).normalize()
        out.append(
            {
                "fuel_group": fg,
                "price": float(last["price"]),
                "publish_date": pdt,
            }
        )
        refs.append(pdt)
    ref_max = max(refs) if refs else None
    return out, ref_max


def _company_snapshot_dff(dff: pd.DataFrame, company: str) -> dict | None:
    """สรุปราคา ณ วันล่าสุดในช่วงที่กรอง ต่อบริษัท"""
    sub = dff.loc[dff["company"] == company].copy()
    if sub.empty:
        return None
    sub["publish_date"] = pd.to_datetime(sub["publish_date"])
    ld = sub["publish_date"].max()
    last = sub.loc[sub["publish_date"] == ld]
    return {
        "latest_date": ld,
        "n_fuels": int(last["fuel_group"].nunique()),
        "avg_price": float(last["price"].mean()),
        "price_min": float(last["price"].min()),
        "price_max": float(last["price"].max()),
    }


# คู่กับ FUEL_QUICK_PRESETS — ใช้กำหนดสี accent การ์ด
FUEL_CARD_VARIANTS: tuple[str, ...] = (
    "v-diesel",
    "v-g91",
    "v-g95e10",
    "v-g95e20",
    "v-g85",
)


def _fuel_snapshot_card_html(
    label: str,
    price_text: str,
    date_line: str | None,
    variant: str,
) -> str:
    """การ์ดราคาแบบ HTML (escape แล้ว) — ไม่ใช้ st.metric เพื่อควบคุมสไตล์"""
    safe_label = html.escape(label)
    safe_price = html.escape(price_text)
    date_html = ""
    if date_line:
        safe_date = html.escape(date_line)
        date_html = f'<div class="fuel-mini-card__date">{safe_date}</div>'
    unit_html = ""
    if price_text != "—":
        unit_html = '<div class="fuel-mini-card__unit">บาท/ลิตร</div>'
    empty_cls = " fuel-mini-card--empty" if price_text == "—" else ""
    return (
        f'<div class="fuel-mini-card fuel-mini-card--{variant}{empty_cls}">'
        '<div class="fuel-mini-card__accent" aria-hidden="true"></div>'
        '<div class="fuel-mini-card__inner">'
        f'<div class="fuel-mini-card__label">{safe_label}</div>'
        f'<div class="fuel-mini-card__price">{safe_price}</div>'
        f"{unit_html}"
        f"{date_html}"
        "</div></div>"
    )


@st.cache_data(ttl=86400, show_spinner="กำลังดึงข้อมูลจาก สนพ. และคลีนข้อมูล…")
def _run_update_pipeline() -> None:
    """รัน update_fuel_data.py (fetch → clean → คัดลอกไฟล์) — ใช้เมื่อเปิด AUTO_UPDATE_FUEL_DATA=1 เท่านั้น"""
    r = subprocess.run(
        [sys.executable, str(UPDATE_SCRIPT)],
        cwd=str(ROOT_DIR),
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        msg = (r.stderr or r.stdout or "").strip() or f"exit code {r.returncode}"
        raise RuntimeError(msg)


PLOTLY_CONFIG = {
    "displayModeBar": True,
    "displaylogo": False,
    "modeBarButtonsToRemove": [
        "lasso2d",
        "select2d",
        "autoScale2d",
        "hoverCompareCartesian",
        "toggleSpikelines",
    ],
    "toImageButtonOptions": {"format": "png", "filename": "fuel_chart"},
}


def _inject_css() -> None:
    st.markdown(
        """
<style>
@import url("https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,400;0,9..40,500;0,9..40,600;0,9..40,700;1,9..40,400&display=swap");

html, body, [class*="css"]  {
  font-family: "DM Sans", "Sarabun", system-ui, sans-serif;
}

/* พื้นหลังแอป — ไล่สี + แสงมุม แบบ tech / dashboard */
.stApp {
  background-color: #0f172a;
  background-image:
    radial-gradient(ellipse 90% 70% at 85% -10%, rgba(59, 130, 246, 0.35), transparent 55%),
    radial-gradient(ellipse 70% 50% at 5% 105%, rgba(99, 102, 241, 0.2), transparent 50%),
    linear-gradient(165deg, #0f172a 0%, #1e293b 35%, #f1f5f9 35%, #e8edf5 100%);
  background-attachment: fixed;
}

/* พื้นที่เนื้อหาหลัก — การ์ดโปร่งบนพื้นหลังมืด */
[data-testid="stAppViewContainer"] > .main {
  background: transparent;
}

.block-container {
  padding-top: 1.5rem;
  padding-bottom: 3rem;
  max-width: 1180px;
  background: rgba(255, 255, 255, 0.72);
  backdrop-filter: blur(12px);
  -webkit-backdrop-filter: blur(12px);
  border-radius: 0 20px 0 0;
  border: 1px solid rgba(148, 163, 184, 0.25);
  border-left: none;
  box-shadow: 0 0 0 1px rgba(255,255,255,0.5) inset, 8px 8px 40px rgba(15, 23, 42, 0.12);
}

#MainMenu {visibility: hidden;}
header[data-testid="stHeader"] {
  background: rgba(15, 23, 42, 0.4);
  backdrop-filter: blur(10px);
  border-bottom: 1px solid rgba(148, 163, 184, 0.2);
}
footer {visibility: hidden;}
/* ไม่ซ่อน stToolbar / stDecoration — จะทำให้ปุ่มเปิด/ปิด sidebar หาย */

.hero-wrap {
  background: linear-gradient(135deg, #0f172a 0%, #1e3a5f 50%, #2563eb 120%);
  border-radius: 20px;
  padding: 2rem 1.5rem 1.75rem 1.5rem;
  margin-bottom: 1.75rem;
  box-shadow: 0 12px 40px rgba(37, 99, 235, 0.18);
  text-align: center;
  max-width: 920px;
  margin-left: auto;
  margin-right: auto;
}
.hero-wrap h1 {
  color: #f8fafc !important;
  font-weight: 800;
  letter-spacing: -0.03em;
  font-size: clamp(1.65rem, 4vw, 2.35rem) !important;
  margin: 0 0 0.5rem 0 !important;
  border: none !important;
  line-height: 1.15 !important;
  text-shadow: 0 2px 24px rgba(37, 99, 235, 0.35);
}
.hero-wrap p {
  color: rgba(248, 250, 252, 0.85) !important;
  margin: 0 auto !important;
  font-size: 1rem;
  max-width: 36rem;
  line-height: 1.45;
}
.hero-wrap .hero-byline {
  color: rgba(248, 250, 252, 0.52) !important;
  margin: 1.1rem auto 0 auto !important;
  font-size: 0.8rem;
  font-weight: 500;
  letter-spacing: 0.05em;
  max-width: 36rem;
  line-height: 1.4;
}

.card {
  background: #ffffff;
  border: 1px solid rgba(15, 23, 42, 0.06);
  border-radius: 16px;
  padding: 1.25rem 1.35rem;
  box-shadow: 0 1px 3px rgba(15, 23, 42, 0.04);
  margin-bottom: 1.25rem;
}

.fuel-snapshot-wrap {
  background: linear-gradient(165deg, rgba(255,255,255,0.98) 0%, rgba(248, 250, 252, 0.95) 100%);
  border: 1px solid rgba(148, 163, 184, 0.35);
  border-radius: 18px;
  padding: 1.25rem 1.35rem 1.1rem 1.35rem;
  margin-bottom: 1.25rem;
  box-shadow:
    0 1px 2px rgba(15, 23, 42, 0.04),
    0 12px 40px -12px rgba(37, 99, 235, 0.12);
}
.fuel-snapshot-wrap h5 {
  margin: 0 0 0.15rem 0 !important;
  font-size: 1.12rem !important;
  font-weight: 700 !important;
  color: #0f172a !important;
  letter-spacing: -0.02em !important;
}
.fuel-snapshot-wrap .fuel-snapshot-sub {
  font-size: 0.82rem !important;
  color: #64748b !important;
  margin: 0 0 1rem 0 !important;
  font-weight: 500 !important;
}

/* การ์ดราคา 5 ชนิด — modern */
.fuel-mini-card {
  position: relative;
  border-radius: 14px;
  overflow: hidden;
  background: linear-gradient(155deg, #ffffff 0%, #f1f5f9 55%, #e2e8f0 130%);
  border: 1px solid rgba(148, 163, 184, 0.35);
  box-shadow:
    0 2px 4px rgba(15, 23, 42, 0.04),
    0 8px 24px -8px rgba(15, 23, 42, 0.12),
    inset 0 1px 0 rgba(255, 255, 255, 0.85);
  min-height: 122px;
  transition: transform 0.18s ease, box-shadow 0.18s ease;
}
.fuel-mini-card:hover {
  transform: translateY(-2px);
  box-shadow:
    0 4px 8px rgba(15, 23, 42, 0.06),
    0 14px 32px -10px rgba(37, 99, 235, 0.18),
    inset 0 1px 0 rgba(255, 255, 255, 0.9);
}
.fuel-mini-card__accent {
  position: absolute;
  left: 0;
  top: 0;
  bottom: 0;
  width: 5px;
  border-radius: 14px 0 0 14px;
}
.fuel-mini-card--v-diesel .fuel-mini-card__accent {
  background: linear-gradient(180deg, #475569, #1e293b);
}
.fuel-mini-card--v-g91 .fuel-mini-card__accent {
  background: linear-gradient(180deg, #22c55e, #15803d);
}
.fuel-mini-card--v-g95e10 .fuel-mini-card__accent {
  background: linear-gradient(180deg, #3b82f6, #1d4ed8);
}
.fuel-mini-card--v-g95e20 .fuel-mini-card__accent {
  background: linear-gradient(180deg, #a855f7, #7c3aed);
}
.fuel-mini-card--v-g85 .fuel-mini-card__accent {
  background: linear-gradient(180deg, #f59e0b, #d97706);
}
.fuel-mini-card__inner {
  padding: 0.95rem 0.9rem 0.95rem 1.15rem;
}
.fuel-mini-card__label {
  font-size: 0.62rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: #64748b;
  line-height: 1.25;
}
.fuel-mini-card__price {
  font-size: 1.42rem;
  font-weight: 800;
  color: #0f172a;
  letter-spacing: -0.03em;
  margin: 0.4rem 0 0.05rem 0;
  line-height: 1.15;
  font-variant-numeric: tabular-nums;
}
.fuel-mini-card__unit {
  font-size: 0.72rem;
  font-weight: 500;
  color: #94a3b8;
  letter-spacing: 0.02em;
}
.fuel-mini-card__date {
  font-size: 0.68rem;
  font-weight: 500;
  color: #64748b;
  margin-top: 0.55rem;
  padding-top: 0.45rem;
  border-top: 1px solid rgba(148, 163, 184, 0.35);
}
.fuel-mini-card--empty .fuel-mini-card__price {
  color: #94a3b8;
  font-size: 1.15rem;
  font-weight: 600;
}
.fuel-mini-card--empty {
  background: linear-gradient(155deg, #f8fafc 0%, #f1f5f9 100%);
  opacity: 0.92;
}
.fuel-mini-card--empty:hover {
  transform: none;
}

.company-card-wrap {
  background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
  border: 1px solid rgba(15, 23, 42, 0.08);
  border-radius: 14px;
  padding: 1rem 1.05rem;
  margin-bottom: 0.35rem;
  min-height: 118px;
}
.company-card-wrap strong {
  font-size: 1.05rem;
  color: #0f172a;
}

div[data-testid="stMetric"] {
  background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
  border: 1px solid rgba(15, 23, 42, 0.07);
  padding: 1rem 1.1rem 0.85rem 1.1rem;
  border-radius: 14px;
  box-shadow: 0 2px 8px rgba(15, 23, 42, 0.04);
}
div[data-testid="stMetric"] label { opacity: 0.72; font-size: 0.72rem !important; text-transform: uppercase; letter-spacing: 0.04em; }
div[data-testid="stMetric"] [data-testid="stMetricValue"] { font-size: 1.15rem !important; }

/* Sidebar ติดซ้ายเต็มความสูง — กระจกโปร่ง อ่านง่าย ไม่ทับปุ่มเปิด/ปิดของ Streamlit */
section[data-testid="stSidebar"] {
  position: sticky !important;
  top: 0 !important;
  align-self: flex-start !important;
  height: 100vh !important;
  max-height: 100vh !important;
  overflow-y: auto !important;
  overflow-x: hidden !important;
  z-index: 100 !important;
  background: rgba(248, 250, 252, 0.88) !important;
  backdrop-filter: blur(14px);
  -webkit-backdrop-filter: blur(14px);
  border-right: 1px solid rgba(148, 163, 184, 0.45) !important;
  box-shadow: 6px 0 32px rgba(15, 23, 42, 0.12) !important;
}

/* สำรอง: ซ่อนแถบนำทางหน้า (ป้าย app) ถ้าเวอร์ชัน Streamlit ไม่รับ config showSidebarNavigation */
[data-testid="stSidebarNav"],
nav[data-testid="stSidebarNav"] {
  display: none !important;
}

h3 { font-weight: 600 !important; color: #0f172a !important; letter-spacing: -0.01em; }

.snapshot-head {
  font-size: 1.05rem;
  color: #334155;
  margin: 0.25rem 0 1rem 0;
  padding: 0.65rem 1rem;
  background: linear-gradient(90deg, rgba(59,130,246,0.08), rgba(99,102,241,0.05));
  border-radius: 12px;
  border: 1px solid rgba(148, 163, 184, 0.35);
}
.snapshot-head strong { color: #1d4ed8; font-size: 1.1rem; }
</style>
""",
        unsafe_allow_html=True,
    )


def _hero(title: str, subtitle: str) -> None:
    t = html.escape(title)
    s = html.escape(subtitle)
    c = html.escape(CREDIT_BYLINE)
    st.markdown(
        f'<div class="hero-wrap"><h1>{t}</h1><p>{s}</p><p class="hero-byline">{c}</p></div>',
        unsafe_allow_html=True,
    )


def _aggregate_period(df: pd.DataFrame, period: str) -> pd.DataFrame:
    """รายวัน = ข้อมูลเดิม; รายเดือน/รายปี = เฉลี่ยราคาในช่วงนั้น."""
    d = df.copy()
    d["publish_date"] = pd.to_datetime(d["publish_date"])
    if period == "รายวัน":
        return d.sort_values("publish_date")
    freq = "MS" if period == "รายเดือน" else "YS"
    g = (
        d.groupby(["company", "fuel_group", pd.Grouper(key="publish_date", freq=freq)], sort=True)["price"]
        .mean()
        .reset_index()
    )
    return g.sort_values("publish_date")


def _apply_shock_highlights(
    fig,
    date_start: date,
    date_end: date,
    facet_row: str | None,
    row_order: list[str] | None,
) -> None:
    """ทับช่วงเหตุการณ์ตลาด (COVID / Oil spike) บนกราฟ"""
    d0 = pd.Timestamp(date_start).date()
    d1 = pd.Timestamp(date_end).date()
    evs = events_in_range(d0, d1)
    if not evs:
        return
    if facet_row and row_order and len(row_order) > 1:
        for ri, _ in enumerate(row_order, start=1):
            for ev in evs:
                xs = max(ev.start, d0)
                xe = min(ev.end, d1)
                fig.add_vrect(
                    x0=datetime.combine(xs, datetime.min.time()),
                    x1=datetime.combine(xe, datetime.min.time()),
                    fillcolor=ev.fill_rgba,
                    layer="below",
                    line_width=0,
                    row=ri,
                    col=1,
                )
    else:
        for ev in evs:
            xs = max(ev.start, d0)
            xe = min(ev.end, d1)
            fig.add_vrect(
                x0=datetime.combine(xs, datetime.min.time()),
                x1=datetime.combine(xe, datetime.min.time()),
                fillcolor=ev.fill_rgba,
                layer="below",
                line_width=0,
            )


@st.cache_data(show_spinner="Loading & cleaning data…")
def _load_clean(path: str, file_mtime_ns: int) -> tuple[pd.DataFrame, dict]:
    _ = file_mtime_ns  # เมื่อไฟล์ถูกเขียนใหม่ mtime เปลี่ยน → โหลดชุดข้อมูลใหม่
    raw = load_raw_excel(path)
    df, report = clean_fuel_prices(raw)
    return df, report.__dict__


def main() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide", page_icon="⛽", initial_sidebar_state="expanded")
    _inject_ga4()
    _inject_css()

    data_path = ROOT_DIR / DATA_FILE

    pipeline_error: str | None = None
    if _auto_update_enabled():
        try:
            _run_update_pipeline()
        except Exception as e:
            pipeline_error = str(e)

    if not data_path.is_file():
        st.error(
            f"ไม่พบไฟล์ `{DATA_FILE}` — รัน `python update_fuel_data.py` หรือ `scripts/daily_update.sh` "
            "หรือตั้ง `AUTO_UPDATE_FUEL_DATA=1` ให้แอปลองดึงเมื่อโหลดหน้า"
        )
        if pipeline_error:
            st.code(pipeline_error)
        st.stop()

    if pipeline_error:
        st.warning(f"การอัปเดตข้อมูลล่าสุดไม่สำเร็จ — ใช้ไฟล์ที่มีอยู่แล้ว\n\n{pipeline_error}")

    try:
        st_info = data_path.stat()
        mtime_key = int(getattr(st_info, "st_mtime_ns", int(st_info.st_mtime * 1_000_000_000)))
    except OSError:
        mtime_key = 0
    df, report = _load_clean(str(data_path), mtime_key)
    _maybe_auto_export_csv(df, mtime_key)

    _hero(
        APP_TITLE,
        "เทียบราคาน้ำมัน — เลือกช่วงเวลาและมุมมองรายวัน / เดือน / ปี",
    )

    with st.sidebar:
        min_d = pd.to_datetime(report["min_date"]).date()
        max_d = pd.to_datetime(report["max_date"]).date()

        default_start = (pd.Timestamp(max_d) - pd.Timedelta(days=365 * 2)).date()
        if default_start < min_d:
            default_start = min_d

        date_range_key = "fuel_date_range"
        if date_range_key not in st.session_state:
            st.session_state[date_range_key] = (default_start, max_d)
        else:
            st.session_state[date_range_key] = _clamp_date_range_tuple(
                st.session_state[date_range_key],
                min_d,
                max_d,
                (default_start, max_d),
            )

        groups_all = sorted(df["fuel_group"].dropna().unique().tolist())
        companies_all = sorted(df["company"].dropna().unique().tolist())

        df_recent = filter_df(
            df, pd.to_datetime(default_start), pd.to_datetime(max_d), companies=[], fuel_groups=[]
        )
        recent_counts = df_recent["fuel_group"].value_counts(dropna=True) if not df_recent.empty else None

        default_groups: list[str] = []
        if recent_counts is not None and not recent_counts.empty:
            dieselish = [f for f in recent_counts.index.tolist() if ("ดีเซล" in f) or ("Diesel" in f)]
            default_groups = dieselish[:1] if dieselish else [recent_counts.index[0]]
        elif groups_all:
            default_groups = [df["fuel_group"].value_counts(dropna=True).index[0]]

        fuel_groups_key = "fuel_groups_sel"
        if fuel_groups_key not in st.session_state:
            st.session_state[fuel_groups_key] = list(default_groups)

        _rng = st.session_state[date_range_key]
        _ds = pd.Timestamp(_rng[0]).date()
        _de = pd.Timestamp(_rng[1]).date()
        active_preset = _active_time_preset_key(_ds, _de, max_d, min_d)

        st.caption(
            "เลือกช่วงเวลา (สิ้นสุด ณ วันล่าสุดในฐาน)"
        )
        for row in (
            [("1 วัน", "1d"), ("3 วัน", "3d"), ("7 วัน", "7d")],
            [("1 เดือน", "1m"), ("3 เดือน", "3m"), ("6 เดือน", "6m")],
            [("1 ปี", "1y"), ("3 ปี", "3y"), ("5 ปี", "5y")],
        ):
            cols = st.columns(3)
            for col, (label, pk) in zip(cols, row):
                with col:
                    if st.button(
                        label,
                        key=f"preset_{pk}",
                        use_container_width=True,
                        type="primary" if active_preset == pk else "secondary",
                    ):
                        st.session_state[date_range_key] = _preset_date_range(max_d, min_d, pk)
                        ds, de = st.session_state[date_range_key]
                        seed_preset = filter_df(
                            df,
                            pd.to_datetime(ds),
                            pd.to_datetime(de),
                            companies=[],
                            fuel_groups=st.session_state[fuel_groups_key],
                        )
                        top_preset = (
                            seed_preset["company"].value_counts().head(6).index.tolist()
                            if not seed_preset.empty
                            else companies_all[:6]
                        )
                        st.session_state["companies_sel"] = _default_companies_preset(companies_all, top_preset)
                        st.rerun()

        date_range = st.date_input(
            "ช่วงวันที่",
            min_value=min_d,
            max_value=max_d,
            key=date_range_key,
        )
        if isinstance(date_range, tuple):
            date_start, date_end = date_range
        else:
            date_start, date_end = date_range, date_range

        st.caption("กลุ่มน้ำมันยอดนิยม — กดเพิ่มกลุ่มลงกราฟ · กดซ้ำที่เดิมเพื่อเอาออก (เลือกหลายกลุ่มได้)")
        _fg_state = list(st.session_state.get(fuel_groups_key, []))
        for ri, chunk in enumerate((FUEL_QUICK_PRESETS[:3], FUEL_QUICK_PRESETS[3:])):
            cols = st.columns(len(chunk))
            for ci, want in enumerate(chunk):
                resolved = _resolve_fuel_group_name(groups_all, want)
                with cols[ci]:
                    _fg_on = bool(resolved) and resolved in _fg_state
                    if st.button(
                        want,
                        key=f"fuel_quick_{ri}_{ci}",
                        use_container_width=True,
                        type="primary" if _fg_on else "secondary",
                        disabled=resolved is None,
                    ):
                        if resolved:
                            st.session_state[fuel_groups_key] = _toggle_fuel_group_selection(
                                st.session_state.get(fuel_groups_key, []),
                                resolved,
                            )
                            st.session_state["_fg_popover_sync"] = True
                            st.rerun()

        if st.session_state.pop("_fg_popover_sync", False):
            _invalidate_fuel_group_checkbox_keys(len(groups_all))

        _fg_cur = list(st.session_state.get(fuel_groups_key, []))
        _pop_label = (
            f"กลุ่มน้ำมัน ({len(_fg_cur)} กลุ่ม) ▼"
            if _fg_cur
            else "กลุ่มน้ำมัน — ติ๊กเลือก ▼"
        )
        with st.popover(_pop_label, use_container_width=True):
            st.caption("ติ๊กเลือกกลุ่ม (หลายกลุ่มได้) — หรือใช้ปุ่มด่วนด้านบน")
            _fg_new: list[str] = []
            _fg_base = list(st.session_state.get(fuel_groups_key, []))
            with st.container(height=280):
                for i, g in enumerate(groups_all):
                    k = f"fgp_cb_{i}"
                    if k not in st.session_state:
                        st.session_state[k] = g in _fg_base
                    if st.checkbox(g, key=k):
                        _fg_new.append(g)
            st.session_state[fuel_groups_key] = _fg_new

        fuel_groups = list(st.session_state.get(fuel_groups_key, []))

        d0 = pd.to_datetime(date_start)
        d1 = pd.to_datetime(date_end)
        seed = filter_df(df, d0, d1, companies=[], fuel_groups=fuel_groups)
        top_companies = (
            seed["company"].value_counts().head(6).index.tolist()
            if not seed.empty
            else companies_all[:6]
        )

        companies_key = "companies_sel"
        if companies_key not in st.session_state:
            st.session_state[companies_key] = _default_companies_preset(companies_all, top_companies)

        st.caption("บริษัทยอดนิยม — กดเพิ่ม/ถอด (หลายแห่งได้)")
        _co_state = list(st.session_state.get(companies_key, []))
        for ri, chunk in enumerate((COMPANY_QUICK_PRESETS[:3], COMPANY_QUICK_PRESETS[3:])):
            cols = st.columns(len(chunk))
            for ci, want in enumerate(chunk):
                resolved = _resolve_company_name(companies_all, want)
                with cols[ci]:
                    _co_on = bool(resolved) and resolved in _co_state
                    if st.button(
                        want,
                        key=f"co_quick_{ri}_{ci}",
                        use_container_width=True,
                        type="primary" if _co_on else "secondary",
                        disabled=resolved is None,
                    ):
                        if resolved:
                            st.session_state[companies_key] = _toggle_in_list(
                                st.session_state.get(companies_key, []),
                                resolved,
                            )
                            st.session_state["_co_popover_sync"] = True
                            st.rerun()

        if st.session_state.pop("_co_popover_sync", False):
            _invalidate_company_checkbox_keys(len(companies_all))

        _co_cur = list(st.session_state.get(companies_key, []))
        _co_pop_label = (
            f"บริษัท ({len(_co_cur)} แห่ง) ▼"
            if _co_cur
            else "บริษัท — ติ๊กเลือก ▼"
        )
        with st.popover(_co_pop_label, use_container_width=True):
            st.caption("ติ๊กเลือกบริษัท (หลายแห่งได้) — หรือใช้ปุ่มด่วนด้านบน")
            _co_new: list[str] = []
            _co_base = list(st.session_state.get(companies_key, []))
            with st.container(height=280):
                for i, c in enumerate(companies_all):
                    ck = f"cop_cb_{i}"
                    if ck not in st.session_state:
                        st.session_state[ck] = c in _co_base
                    if st.checkbox(c, key=ck):
                        _co_new.append(c)
            st.session_state[companies_key] = _co_new

        companies = list(st.session_state.get(companies_key, []))

        st.divider()
        chart_period = st.selectbox(
            "มุมมองกราฟ",
            ["รายวัน", "รายเดือน", "รายปี"],
            index=0,
            help="รายเดือน/รายปี = ค่าเฉลี่ยราคาในช่วงนั้น",
        )
        smoothing = st.selectbox(
            "ความเรียบของเส้น (รายวันเท่านั้น)",
            ["ปิด", "เฉลี่ยเลื่อน 7 วัน", "เฉลี่ยเลื่อน 30 วัน"],
            index=1,
            disabled=(chart_period != "รายวัน"),
        )
        show_points = st.toggle("จุดบนเส้น", value=False)
        show_norm = st.toggle("แสดงกราฟ Normalized (ฐาน 100 ณ วันเริ่มช่วง)", value=False)
        show_roll_on_norm = st.toggle("บนกราฟ Normalized: แสดงเส้น Rolling 7 / 30 วัน", value=False)

    d0 = pd.to_datetime(date_start)
    d1 = pd.to_datetime(date_end)
    dff = filter_df(df, d0, d1, companies=companies, fuel_groups=fuel_groups)

    if dff.empty:
        st.warning("ไม่มีข้อมูลในช่วงที่เลือก")
        st.stop()

    latest_day = dff["publish_date"].max()
    latest = dff.loc[dff["publish_date"] == latest_day]

    #st.markdown('<div class="card">', unsafe_allow_html=True)
    #c1, c2, c3 = st.columns(3)
    #c1.metric("ช่วงข้อมูลในฐาน", f"{pd.to_datetime(report['min_date']).date()} → {pd.to_datetime(report['max_date']).date()}")
    #c2.metric("วันล่าสุด (หลังกรอง)", f"{latest_day.date()}")
    #c3.metric("จำนวนบริษัท (หลังกรอง)", f"{dff['company'].nunique()}")
    #st.markdown("</div>", unsafe_allow_html=True)

    popular_rows, ptt_ref_date = _ptt_popular_fuel_rows(df)
    st.markdown('<div class="fuel-snapshot-wrap">', unsafe_allow_html=True)
    st.markdown("##### ภาพรวมราคาล่าสุด", unsafe_allow_html=True)
    st.markdown(
        '<p class="fuel-snapshot-sub">5 กลุ่มยอดนิยม',
        unsafe_allow_html=True,
    )
    fs_cols = st.columns(5)
    for i, want in enumerate(FUEL_QUICK_PRESETS):
        with fs_cols[i]:
            r = popular_rows[i] if i < len(popular_rows) else None
            variant = FUEL_CARD_VARIANTS[i] if i < len(FUEL_CARD_VARIANTS) else "v-diesel"
            if r is None:
                st.markdown(
                    _fuel_snapshot_card_html(want, "—", None, variant),
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    _fuel_snapshot_card_html(
                        want,
                        f"{r['price']:.2f}",
                        f"ณ {r['publish_date'].strftime('%d/%m/%Y')}",
                        variant,
                    ),
                    unsafe_allow_html=True,
                )
    if ptt_ref_date is not None:
        st.caption(
            f"หมายเหตุ: ราคาที่แสดงเป็นราคาของ **{REFERENCE_PRICE_COMPANY}**"
            #f"— วันที่ล่าสุดในกลุ่มที่แสดงครบ: **{ptt_ref_date.strftime('%d/%m/%Y')}**"
        )
    else:
        st.caption(
            f"หมายเหตุ: ไม่พบข้อมูล **{REFERENCE_PRICE_COMPANY}** สำหรับกลุ่มยอดนิยมในฐาน — โปรดตรวจไฟล์ข้อมูล"
        )
    st.markdown("</div>", unsafe_allow_html=True)

    
    # —— Insight summary (Thai) ——
    dm = daily_mean_by_group(dff)
    max_px = max_price_latest_by_type(dff, latest_day)
    min_max_period_df = min_max_in_period_by_group(dff)
    pct_df = pct_change_period_by_group(dm)
    swing_df = strongest_move_window(dm, window=7)
    trend_df = trend_latest(dm)
    try:
        yoy_mom_df = yoy_mom_for_groups(df, list(companies), list(fuel_groups), latest_day)
    except Exception:
        yoy_mom_df = pd.DataFrame()
    ds = date_start if isinstance(date_start, date) else pd.Timestamp(date_start).date()
    de = date_end if isinstance(date_end, date) else pd.Timestamp(date_end).date()
    evs = events_in_range(ds, de)

    summary_md = build_thai_summary(
        latest_day,
        ds,
        de,
        max_px,
        min_max_period_df,
        pct_df,
        swing_df,
        trend_df,
        yoy_mom_df,
    )

    st.markdown("### สรุปเชิงลึก (Insight)")
    st.markdown(generate_executive_summary(build_insights_dict(pct_df, trend_df)))
    st.markdown("---")
    st.markdown(summary_md)
    with st.expander("รายละเอียดตัวเลข (สูงสุดต่อประเภท · % เปลี่ยนในช่วง · YoY / MoM)", expanded=False):
        st.caption("สูงสุด / ต่ำสุดของราคาในช่วงวันที่เลือก (ทุกบริษัทที่ติ๊ก) — พร้อมวันที่เกิดค่านั้น")
        st.dataframe(
            min_max_period_df,
            hide_index=True,
            use_container_width=True,
            column_config={
                "fuel_group": st.column_config.TextColumn("กลุ่มน้ำมัน"),
                "price_max": st.column_config.NumberColumn("ราคาสูงสุด (฿/L)", format="%.2f"),
                "date_max": st.column_config.DatetimeColumn("วันที่ (max)", format="DD/MM/YYYY"),
                "company_at_max": st.column_config.TextColumn("บริษัท (max)"),
                "price_min": st.column_config.NumberColumn("ราคาต่ำสุด (฿/L)", format="%.2f"),
                "date_min": st.column_config.DatetimeColumn("วันที่ (min)", format="DD/MM/YYYY"),
                "company_at_min": st.column_config.TextColumn("บริษัท (min)"),
            },
        )
        c_ins1, c_ins2 = st.columns(2)
        with c_ins1:
            st.caption("ราคาสูงสุดต่อกลุ่ม ณ วันล่าสุด (ในช่วงที่กรอง)")
            st.dataframe(max_px, hide_index=True, use_container_width=True)
            st.caption("% เปลี่ยนในช่วง (เฉลี่ยรายวันต่อกลุ่ม วันแรก → วันสุดท้าย)")
            st.dataframe(pct_df, hide_index=True, use_container_width=True)
        with c_ins2:
            st.caption("ช่วง 7 วันที่ swing แรงสุด")
            st.dataframe(swing_df, hide_index=True, use_container_width=True)
            st.caption("แนวโน้มล่าสุด (~10 วัน vs 10 วันก่อน)")
            st.dataframe(trend_df, hide_index=True, use_container_width=True)
        if not yoy_mom_df.empty:
            st.caption("YoY / MoM (เฉลี่ย 7 วันล่าสุด เทียบช่วงย้อนหลัง ~1 ปี / ~1 เดือน)")
            st.dataframe(
                yoy_mom_df,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "yoy_pct": st.column_config.NumberColumn("YoY %", format="%.2f"),
                    "mom_pct": st.column_config.NumberColumn("MoM %", format="%.2f"),
                    "avg_7d_recent": st.column_config.NumberColumn("เฉลี่ย 7 วันล่าสุด", format="%.2f"),
                },
            )
    if evs:
        st.caption(
            "ช่วง **Shock / เหตุการณ์** ที่ไฮไลต์บนกราฟ: "
            + " · ".join(f"{e.label_en} ({e.label_th})" for e in evs)
        )
        
    st.markdown("### แนวโน้มราคา")
    plot_df = _aggregate_period(dff, chart_period)

    if chart_period == "รายวัน" and smoothing != "ปิด":
        window = 7 if "7" in smoothing else 30
        plot_df = plot_df.sort_values(["company", "fuel_group", "publish_date"])
        plot_df["price_smooth"] = (
            plot_df.groupby(["company", "fuel_group"])["price"]
            .rolling(window=window, min_periods=max(2, window // 3))
            .mean()
            .reset_index(level=[0, 1], drop=True)
        )
        y_col = "price_smooth"
        y_title = f"ราคา (เฉลี่ยเลื่อน {window} วัน)"
    else:
        y_col = "price"
        y_title = "ราคา (บาท/ลิตร)" if chart_period == "รายวัน" else f"ราคาเฉลี่ย ({chart_period})"

    facet = "fuel_group" if len(fuel_groups) > 1 else None
    fig = px.line(
        plot_df,
        x="publish_date",
        y=y_col,
        color="company",
        facet_row=facet,
        hover_data=None,
        labels={
            "publish_date": "วันที่",
            y_col: y_title,
            "company": "บริษัท",
            "fuel_group": "กลุ่มน้ำมัน",
        },
        height=480 if facet is None else min(320 + 220 * len(fuel_groups), 900),
        color_discrete_sequence=px.colors.qualitative.Bold,
    )
    if show_points:
        fig.update_traces(mode="lines+markers")
    else:
        fig.update_traces(mode="lines")

    # Hover สั้น ๆ — แสดงแค่บริษัท วันที่ ราคา (ไม่โหลดกล่องข้อมูลยาว)
    ht = (
        "<b>%{fullData.name}</b><br>"
        "%{x|%Y-%m-%d}<br>"
        "<b>฿%{y:.2f}</b>/L"
        "<extra></extra>"
    )
    fig.update_traces(hovertemplate=ht, line=dict(width=2.2))

    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(248,250,252,0.6)",
        legend_title_text="บริษัท",
        margin=dict(l=8, r=8, t=32, b=8),
        hovermode="closest",
        xaxis=dict(showgrid=True, gridcolor="rgba(15,23,42,0.06)"),
        yaxis=dict(showgrid=True, gridcolor="rgba(15,23,42,0.06)"),
    )
    fig.update_xaxes(tickformat="%Y-%m-%d")

    row_order = list(plot_df["fuel_group"].unique()) if facet else None
    _apply_shock_highlights(fig, date_start, date_end, facet, row_order)
    st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    if show_norm and chart_period == "รายวัน":
        st.markdown("### มุมมอง Normalized (ฐาน = 100 ณ วันเริ่มช่วงที่เลือก)")
        st.caption("เปรียบเทียบการเปลี่ยนแปลงเชิงสัมพัทธ์ระหว่างบริษัท — คนละจุดเริ่มราคาเท่ากันที่ 100")
        norm_df = normalize_series(dff)
        if norm_df.empty:
            st.info("ไม่มีข้อมูลสำหรับ normalized")
        else:
            fig_n = px.line(
                norm_df,
                x="publish_date",
                y="norm_roll_7" if show_roll_on_norm else "norm_100",
                color="company",
                facet_row="fuel_group" if len(fuel_groups) > 1 else None,
                labels={
                    "publish_date": "วันที่",
                    "norm_100": "ดัชนี (ฐาน 100)",
                    "norm_roll_7": "ดัชนี (เฉลี่ยเลื่อน 7 วัน / ฐานเดิม)",
                    "company": "บริษัท",
                    "fuel_group": "กลุ่มน้ำมัน",
                },
                height=420 if len(fuel_groups) == 1 else min(280 + 200 * len(fuel_groups), 850),
                color_discrete_sequence=px.colors.qualitative.Bold,
            )
            fig_n.update_traces(line=dict(width=2), hovertemplate="%{fullData.name}<br>%{x|%Y-%m-%d}<br>%{y:.2f}<extra></extra>")
            fig_n.update_layout(
                template="plotly_white",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(248,250,252,0.6)",
                margin=dict(l=8, r=8, t=28, b=8),
            )
            row_n = list(norm_df["fuel_group"].unique()) if len(fuel_groups) > 1 else None
            _apply_shock_highlights(
                fig_n,
                date_start,
                date_end,
                "fuel_group" if len(fuel_groups) > 1 else None,
                row_n,
            )
            st.plotly_chart(fig_n, use_container_width=True, config=PLOTLY_CONFIG)
            if show_roll_on_norm:
                st.caption("เส้นที่แสดง = ราคาเฉลี่ยเลื่อน 7 วัน แปลงเป็นดัชนีเทียบฐานเดียวกับวันแรกของช่วง")
            roll_tbl = (
                norm_df.sort_values(["fuel_group", "company", "publish_date"])
                .groupby(["fuel_group", "company"], as_index=False)
                .tail(1)[["fuel_group", "company", "norm_100", "norm_roll_7", "norm_roll_30"]]
            )
            st.caption("ดัชนีล่าสุด + Rolling (เทียบฐาน 100)")
            st.dataframe(roll_tbl, hide_index=True, use_container_width=True)
    elif show_norm and chart_period != "รายวัน":
        st.info("มุมมอง Normalized ใช้ได้เมื่อเลือกมุมมองกราฟ **รายวัน**")

    st.markdown("### สรุปราคาน้ำมันล่าสุดตามประเภทที่เลือก")
    ds = date_start.strftime("%d/%m/%Y") if hasattr(date_start, "strftime") else str(date_start)
    de = date_end.strftime("%d/%m/%Y") if hasattr(date_end, "strftime") else str(date_end)
    latest_str = latest_day.strftime("%d/%m/%Y")
    st.markdown(
        f'<div class="snapshot-head">'
        f'อ้างอิง <strong>วันที่ {latest_str}</strong> '
        f'<span style="opacity:.85">(วันที่ล่าสุดในช่วงที่กรอง {ds} – {de})</span>'
        f"</div>",
        unsafe_allow_html=True,
    )
    snap = (
        latest.groupby(["fuel_group", "company"], as_index=False)["price"]
        .mean()
        .sort_values(["fuel_group", "price"], ascending=[True, False])
    )
    st.dataframe(
        snap,
        use_container_width=True,
        hide_index=True,
        column_config={
            "fuel_group": st.column_config.TextColumn("กลุ่มน้ำมัน"),
            "company": st.column_config.TextColumn("บริษัท"),
            "price": st.column_config.NumberColumn("ราคา (฿/L)", format="%.2f"),
        },
    )


if __name__ == "__main__":
    main()
