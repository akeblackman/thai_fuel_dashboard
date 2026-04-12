"""
สรุปเชิงลึก ราคา normalized YoY/MoM และช่วงเหตุการณ์ตลาด (highlight บนกราฟ)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import pandas as pd


@dataclass(frozen=True)
class MarketEvent:
    start: date
    end: date
    label_th: str
    label_en: str
    fill_rgba: str


# ช่วงอ้างอิงสำหรับ annotation (ทับซ้อนช่วงที่ผู้ใช้เลือกได้)
MARKET_EVENTS: tuple[MarketEvent, ...] = (
    MarketEvent(
        date(2020, 3, 1),
        date(2020, 5, 31),
        "ช่วง COVID-19 (ราคาน้ำมันปรับลง)",
        "COVID dip",
        "rgba(239, 68, 68, 0.12)",
    ),
    MarketEvent(
        date(2022, 2, 24),
        date(2022, 9, 30),
        "วิกฤติพลังงาน / ราคาพุ่ง (RU-UA)",
        "Oil spike",
        "rgba(234, 179, 8, 0.15)",
    ),
    MarketEvent(
        date(2022, 7, 1),
        date(2022, 8, 31),
        "จุดสูงช่วงกลางปี 2565",
        "Mid-2022 high",
        "rgba(59, 130, 246, 0.1)",
    ),
    # สงครามสิบสองวัน (อิหร่าน–อิสราเอล–เวสต์แบงก์) — หยุดยิง 24 มิ.ย. 2025
    MarketEvent(
        date(2025, 6, 13),
        date(2025, 6, 24),
        "สงครามสิบสองวัน (อิหร่าน–อิสราเอล–เวสต์แบงก์) — หยุดยิง 24 มิ.ย. 2025",
        "Twelve-Day War (Jun 2025)",
        "rgba(220, 38, 38, 0.14)",
    ),
    # ความตึงเครียด / การโจมตีช่วง ก.พ. 2569 (อ้างอิงต่อเนื่องจากภูมิภาค)
    MarketEvent(
        date(2026, 2, 26),
        date.today(),
        ###date(2026, 3, 5)
        "ความตึงเครียดอิหร่าน–อิสราเอล–สหรัฐ (ก.พ. 2569)",
        "Iran tensions (Feb 2026)",
        "rgba(185, 28, 28, 0.11)",
    ),
)


# คำอธิบายยาวสำหรับ UI (อ้างอิงทั่วไป ไม่ใช่คำแนะนำการลงทุน)
NOTE_TWELVE_DAY_WAR_TH = (
    "**สงครามสิบสองวัน (13–24 มิถุนายน ค.ศ. 2025)** — สถานที่เกี่ยวข้อง: อิหร่าน อิสราเอล เวสต์แบงก์ "
    "(ช่วง 12 วัน) — สถานะหยุดยิงตั้งแต่ **24 มิถุนายน ค.ศ. 2025** "
    "ต่อมามีความตึงเครียดและการโจมตีอีกครั้งในบริบทอิหร่าน–อิสราเอล–สหรัฐ "
    "(เช่น ช่วง **28 กุมภาพันธ์ ค.ศ. 2026 / พ.ศ. 2569**) — ตลาดน้ำมันมักตอบสนองต่อความเสี่ยงภูมิรัฐศาสตร์และซัพพลาย"
)


def daily_mean_by_group(dff: pd.DataFrame) -> pd.DataFrame:
    """เฉลี่ยราคารายวันต่อกลุ่มน้ำมัน (ข้ามบริษัทที่เลือก)"""
    d = dff.copy()
    d["publish_date"] = pd.to_datetime(d["publish_date"])
    return (
        d.groupby(["publish_date", "fuel_group"], as_index=False)["price"]
        .mean()
        .sort_values(["fuel_group", "publish_date"])
    )


def daily_mean_by_company_group(dff: pd.DataFrame) -> pd.DataFrame:
    d = dff.copy()
    d["publish_date"] = pd.to_datetime(d["publish_date"])
    return d.sort_values(["company", "fuel_group", "publish_date"])


def max_price_latest_by_type(dff: pd.DataFrame, latest_day: pd.Timestamp) -> pd.DataFrame:
    """ราคาสูงสุดต่อกลุ่ม ณ วันล่าสุด (max ข้ามบริษัทในวันนั้น)"""
    sub = dff.loc[dff["publish_date"] == latest_day]
    if sub.empty:
        return pd.DataFrame(columns=["fuel_group", "price_max", "company_at_max"])
    rows = []
    for g, gdf in sub.groupby("fuel_group"):
        imax = gdf["price"].idxmax()
        rows.append(
            {
                "fuel_group": g,
                "price_max": float(gdf.loc[imax, "price"]),
                "company_at_max": str(gdf.loc[imax, "company"]),
            }
        )
    return pd.DataFrame(rows).sort_values("fuel_group")


def min_max_in_period_by_group(dff: pd.DataFrame) -> pd.DataFrame:
    """
    ราคาสูงสุด / ต่ำสุดในช่วงที่กรอง ต่อกลุ่มน้ำมัน
    (จากทุกแถว: ทุกบริษัท × ทุกวันในช่วง) พร้อมวันที่และบริษัทที่เกิดค่านั้น
    """
    d = dff.copy()
    d["publish_date"] = pd.to_datetime(d["publish_date"])
    if d.empty:
        return pd.DataFrame(
            columns=[
                "fuel_group",
                "price_max",
                "date_max",
                "company_at_max",
                "price_min",
                "date_min",
                "company_at_min",
            ]
        )
    rows = []
    for g, gdf in d.groupby("fuel_group"):
        imax = gdf["price"].idxmax()
        imin = gdf["price"].idxmin()
        rows.append(
            {
                "fuel_group": g,
                "price_max": float(gdf.loc[imax, "price"]),
                "date_max": pd.Timestamp(gdf.loc[imax, "publish_date"]).normalize(),
                "company_at_max": str(gdf.loc[imax, "company"]),
                "price_min": float(gdf.loc[imin, "price"]),
                "date_min": pd.Timestamp(gdf.loc[imin, "publish_date"]).normalize(),
                "company_at_min": str(gdf.loc[imin, "company"]),
            }
        )
    return pd.DataFrame(rows).sort_values("fuel_group")


def pct_change_period_by_group(dm: pd.DataFrame) -> pd.DataFrame:
    """% เปลี่ยนจากวันแรก → วันสุดท้ายของชุด daily mean ต่อกลุ่ม"""
    out = []
    for g, s in dm.groupby("fuel_group"):
        s = s.sort_values("publish_date")
        if len(s) < 2:
            out.append({"fuel_group": g, "pct_change": float("nan"), "price_start": None, "price_end": None})
            continue
        p0, p1 = float(s["price"].iloc[0]), float(s["price"].iloc[-1])
        pct = (p1 - p0) / p0 * 100.0 if p0 else float("nan")
        out.append(
            {
                "fuel_group": g,
                "pct_change": pct,
                "price_start": p0,
                "price_end": p1,
            }
        )
    return pd.DataFrame(out)


def strongest_move_window(
    dm: pd.DataFrame, window: int = 7
) -> pd.DataFrame:
    """
    ช่วงที่ราคาเคลื่อนไหวแรงสุด: ใช้ rolling min/max ใน window วัน
    คืนต่อกลุ่ม: ช่วงวันที่ + % swing (max-min)/min * 100
    """
    rows = []
    for g, s in dm.groupby("fuel_group"):
        s = s.sort_values("publish_date").reset_index(drop=True)
        if len(s) < window:
            continue
        roll_max = s["price"].rolling(window, min_periods=window).max()
        roll_min = s["price"].rolling(window, min_periods=window).min()
        swing = (roll_max - roll_min) / roll_min.replace(0, pd.NA) * 100.0
        i = swing.idxmax()
        if pd.isna(i):
            continue
        d0 = s.loc[i - window + 1, "publish_date"]
        d1 = s.loc[i, "publish_date"]
        rows.append(
            {
                "fuel_group": g,
                "window_start": d0,
                "window_end": d1,
                "swing_pct": float(swing.loc[i]),
            }
        )
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def trend_latest(
    dm: pd.DataFrame, recent_days: int = 10, prev_days: int = 10, flat_pct: float = 0.15
) -> pd.DataFrame:
    """เปรียบเทียบค่าเฉลี่ย recent_days วันสุดท้าย กับ prev_days วันก่อนหน้า"""
    rows = []
    for g, s in dm.groupby("fuel_group"):
        s = s.sort_values("publish_date").drop_duplicates("publish_date")
        if len(s) < recent_days + prev_days:
            rows.append({"fuel_group": g, "trend": "ไม่พอข้อมูล", "delta_pct": None})
            continue
        tail = s.tail(recent_days + prev_days)
        recent = tail.tail(recent_days)["price"].mean()
        prev = tail.head(prev_days)["price"].mean()
        if prev == 0 or pd.isna(prev):
            rows.append({"fuel_group": g, "trend": "—", "delta_pct": None})
            continue
        delta = (recent - prev) / prev * 100.0
        if delta > flat_pct:
            tr = "ขึ้น"
        elif delta < -flat_pct:
            tr = "ลง"
        else:
            tr = "ทรงตัว"
        rows.append({"fuel_group": g, "trend": tr, "delta_pct": float(delta)})
    return pd.DataFrame(rows)


def yoy_mom_for_groups(
    df_full: pd.DataFrame,
    companies: list[str],
    fuel_groups: list[str],
    as_of: pd.Timestamp,
) -> pd.DataFrame:
    """
    YoY / MoM จากค่าเฉลี่ย 7 วันสุดท้ายก่อน as_of เทียบกับช่วง 7 วันรอบ ~1 ปี / ~1 เดือนก่อน
    """
    df_full = df_full.copy()
    df_full["publish_date"] = pd.to_datetime(df_full["publish_date"])
    m = df_full["company"].isin(companies) & df_full["fuel_group"].isin(fuel_groups)
    base = df_full.loc[m]
    rows = []
    as_of = pd.Timestamp(as_of).normalize()

    def mean_7d(end: pd.Timestamp) -> dict[str, float]:
        end = pd.Timestamp(end).normalize()
        start = end - pd.Timedelta(days=6)
        sub = base[(base["publish_date"] >= start) & (base["publish_date"] <= end)]
        if sub.empty:
            return {}
        return sub.groupby("fuel_group")["price"].mean().to_dict()

    end_recent = as_of
    recent = mean_7d(end_recent)
    yoy_end = as_of - pd.DateOffset(years=1)
    mom_end = as_of - pd.DateOffset(months=1)
    yoy_block = mean_7d(pd.Timestamp(yoy_end))
    mom_block = mean_7d(pd.Timestamp(mom_end))

    for g in fuel_groups:
        r = recent.get(g)
        y = yoy_block.get(g)
        m_ = mom_block.get(g)
        yoy_pct = ((r - y) / y * 100.0) if r is not None and y and y != 0 else float("nan")
        mom_pct = ((r - m_) / m_ * 100.0) if r is not None and m_ and m_ != 0 else float("nan")
        rows.append(
            {
                "fuel_group": g,
                "avg_7d_recent": r,
                "yoy_pct": yoy_pct,
                "mom_pct": mom_pct,
            }
        )
    return pd.DataFrame(rows)


def normalize_series(dff: pd.DataFrame, value_col: str = "price") -> pd.DataFrame:
    """ฐาน 100 ณ วันแรกของแต่ละ (company, fuel_group) ในช่วงที่กรอง"""
    d = dff.copy()
    d["publish_date"] = pd.to_datetime(d["publish_date"])
    out_parts = []
    for (co, fg), s in d.groupby(["company", "fuel_group"]):
        s = s.sort_values("publish_date")
        if s.empty:
            continue
        p0 = float(s[value_col].iloc[0])
        if p0 == 0:
            continue
        s = s.copy()
        s["norm_100"] = s[value_col].astype(float) / p0 * 100.0
        s["rolling_7"] = s[value_col].rolling(7, min_periods=2).mean()
        s["rolling_30"] = s[value_col].rolling(30, min_periods=2).mean()
        s["norm_roll_7"] = s["rolling_7"] / p0 * 100.0
        s["norm_roll_30"] = s["rolling_30"] / p0 * 100.0
        out_parts.append(s)
    if not out_parts:
        return pd.DataFrame()
    return pd.concat(out_parts, ignore_index=True)


def events_in_range(d0: date, d1: date) -> list[MarketEvent]:
    out = []
    for ev in MARKET_EVENTS:
        if ev.end < d0 or ev.start > d1:
            continue
        out.append(ev)
    return out


def _fmt_day(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    t = pd.Timestamp(v)
    return t.strftime("%d/%m/%Y")


def build_insights_dict(pct_df: pd.DataFrame, trend_df: pd.DataFrame) -> dict[str, dict]:
    """
    รวม % เปลี่ยนในช่วง + แนวโน้มล่าสุด ต่อกลุ่มน้ำมัน
    โครงสร้าง: { ชื่อกลุ่ม: { "pct_change": float | None, "trend": str | None } }
    """
    insights: dict[str, dict] = {}
    if not pct_df.empty:
        for _, r in pct_df.iterrows():
            g = str(r["fuel_group"])
            pc = r.get("pct_change")
            insights[g] = {
                "pct_change": float(pc) if pd.notna(pc) else None,
                "trend": None,
            }
    if not trend_df.empty:
        for _, r in trend_df.iterrows():
            g = str(r["fuel_group"])
            if g not in insights:
                insights[g] = {"pct_change": None, "trend": None}
            insights[g]["trend"] = str(r.get("trend", "")) if pd.notna(r.get("trend")) else None
    return insights


def _interpretation_tone_from_valid(valid: dict[str, dict]) -> tuple[str, float, str]:
    """
    เลือก tone ตาม % เปลี่ยน — อิง **Diesel B7** ถ้ามีใน valid ไม่เช่นนั้นใช้ค่าเฉลี่ยทุกกลุ่ม
    คืน (tone, pct_baseline, คำอธิบายแหล่งที่มาสั้น ๆ)
    """
    pcs = [float(v["pct_change"]) for v in valid.values()]
    diesel_key = None
    for k in valid:
        if k == "Diesel B7":
            diesel_key = k
            break
        if ("B7" in k) and ("Diesel" in k or "ดีเซล" in k):
            diesel_key = k
            break
    if diesel_key is not None:
        pct_change_avg = float(valid[diesel_key]["pct_change"])
        src = f"กลุ่ม {diesel_key}"
    else:
        pct_change_avg = sum(pcs) / len(pcs) if pcs else 0.0
        src = "ค่าเฉลี่ย % เปลี่ยนทุกกลุ่มที่มีข้อมูล"

    if pct_change_avg > 20:
        tone = "ตลาดมีแรงส่งด้านราคาอย่างมีนัยสำคัญ"
    elif pct_change_avg < -10:
        tone = "ตลาดมีแรงกดดันด้านราคา"
    else:
        tone = "ตลาดยังเคลื่อนไหวในกรอบจำกัด"
    return tone, pct_change_avg, src


def generate_executive_summary(insights: dict[str, dict]) -> str:
    """
    สรุปภาพรวมแบบ executive (สั้น กระแทก) จาก dict ที่ build_insights_dict สร้าง

    หัวข้อ max/min ปรับตามทิศทาง % ของทุกกลุ่มที่มี pct_change:
    - all_positive → ขึ้นแรงสุด / ขึ้นน้อยสุด
    - all_negative → ติดลบน้อยสุด / ติดลบมากสุด
    - mixed (มีทั้งบวกและลบ หรือมีศูนย์ปน) → ผลตอบแทนดีที่สุด / ต่ำสุด

    Interpretation / 📊 Insight: tone จาก % เปลี่ยนของ Diesel B7 (ถ้ามี) ไม่เช่นนั้นใช้ค่าเฉลี่ย —
    >20 / <-10 / อื่น ๆ → ประโยคสรุปที่นำหน้าด้วย "ตลาด…"
    """
    valid = {
        k: v
        for k, v in insights.items()
        if v.get("pct_change") is not None
        and not (isinstance(v["pct_change"], float) and pd.isna(v["pct_change"]))
    }
    if not valid:
        return (
            "📊 **ภาพรวมตลาดน้ำมัน (Executive)**\n\n"
            "_ยังไม่มีตัวเลข % เปลี่ยนในช่วงที่เลือก (ข้อมูลไม่พอหรือช่วงสั้นเกินไป)_"
        )

    top_gainer = max(valid.items(), key=lambda x: x[1]["pct_change"])
    worst_gainer = min(valid.items(), key=lambda x: x[1]["pct_change"])

    trends = [v.get("trend") for v in insights.values()]
    up_count = sum(1 for t in trends if t == "ขึ้น")
    down_count = sum(1 for t in trends if t == "ลง")

    if up_count > down_count:
        overall_trend = "ภาพรวมเป็น**ขาขึ้น** (กลุ่มส่วนใหญ่แนวโน้มล่าสุด: ขึ้น)"
    elif down_count > up_count:
        overall_trend = "ภาพรวมเป็น**ขาลง** (กลุ่มส่วนใหญ่แนวโน้มล่าสุด: ลง)"
    else:
        overall_trend = "ภาพรวมยัง**ผันผวน / sideway** (ขึ้น–ลง–ทรงตัวใกล้เคียงกัน)"

    tg = top_gainer[1]["pct_change"]
    wg = worst_gainer[1]["pct_change"]

    # HANDLE SIGN OF RETURNS — หัวข้อ max/min ตามทิศทาง % ของทุกกลุ่มที่มีข้อมูล
    pcs = [float(v["pct_change"]) for v in valid.values()]
    all_positive = bool(pcs) and all(p > 0 for p in pcs)
    all_negative = bool(pcs) and all(p < 0 for p in pcs)
    # mixed: มีทั้งบวกและลบ (รวมกรณีมีศูนย์ปน หรือไม่เข้า all_*)

    if all_positive:
        label_top = "🔺 **กลุ่มที่ปรับขึ้นแรงสุด**"
        label_bottom = "🔻 **กลุ่มที่ปรับขึ้นน้อยสุด**"
    elif all_negative:
        label_top = "🔺 **กลุ่มที่ติดลบน้อยสุด**"
        label_bottom = "🔻 **กลุ่มที่ติดลบมากสุด**"
    else:
        label_top = "🔺 **กลุ่มที่ให้ผลตอบแทนดีที่สุด**"
        label_bottom = "🔻 **กลุ่มที่ให้ผลตอบแทนต่ำสุด**"

    tone, pct_change_avg, tone_src = _interpretation_tone_from_valid(valid)

    return f"""📊 **ภาพรวมตลาดน้ำมันในช่วงที่เลือก**

{overall_trend}

{label_top}  
**{top_gainer[0]}** ({tg:+.1f}%)

{label_bottom}  
**{worst_gainer[0]}** ({wg:+.1f}%)

🔍 **Interpretation:**  
แนวโน้มดังกล่าวอาจสะท้อนแรงกดดันด้านต้นทุนในภาคพลังงาน และมีนัยต่อราคาสินค้าและบริการในระยะถัดไป โดยเมื่อพิจารณาราคาน้ำมันดีเซล / ภาพรวมการเปลี่ยนแปลงในช่วงนี้ _(อิง {tone_src}: {pct_change_avg:+.1f}%)_

💡 **Insight:**  
ราคาน้ำมันในช่วงที่วิเคราะห์สะท้อนลักษณะ "cyclical behavior" อย่างชัดเจน โดยมีการตอบสนองต่อ shock ภายนอก (เช่น COVID และภาวะราคาพลังงานโลก) ค่อนข้างรวดเร็วในช่วงขาขึ้น แต่มีลักษณะปรับตัวลงแบบค่อยเป็นค่อยไป (price stickiness)
โครงสร้างดังกล่าวบ่งชี้ว่าการเคลื่อนไหวของราคาน้ำมันไม่ได้เป็นเพียง function ของ demand–supply ระยะสั้น แต่ยังได้รับอิทธิพลจาก policy และกลไกตลาดในประเทศ
ตลอดจนในช่วงที่วิเคราะห์ **{tone}**
""".strip()


def build_thai_summary(
    latest_day: pd.Timestamp,
    date_start: date,
    date_end: date,
    max_px: pd.DataFrame,
    min_max_period: pd.DataFrame,
    pct_df: pd.DataFrame,
    swing: pd.DataFrame,
    trend: pd.DataFrame,
    yoy_mom: pd.DataFrame,
) -> str:
    """ข้อความสรุปสั้น ๆ ภาษาไทย"""
    lines = [
        f"**สรุปช่วง {date_start.strftime('%d/%m/%Y')} – {date_end.strftime('%d/%m/%Y')}** "
        f"(ข้อมูลล่าสุด ณ {latest_day.strftime('%d/%m/%Y')})",
    ]
    if not min_max_period.empty:
        mm_parts = []
        for _, r in min_max_period.iterrows():
            mm_parts.append(
                f"**{r['fuel_group']}** — สูงสุด **{r['price_max']:.2f}** บาท/ลิตร ณ {_fmt_day(r['date_max'])} "
                f"({r['company_at_max']}) · ต่ำสุด **{r['price_min']:.2f}** บาท/ลิตร ณ {_fmt_day(r['date_min'])} ({r['company_at_min']})"
            )
        lines.append("**สูงสุด / ต่ำสุดของราคาในช่วงที่เลือก:**\n\n" + "\n\n".join(mm_parts))
    if not pct_df.empty and "pct_change" in pct_df.columns:
        parts = []
        for _, r in pct_df.iterrows():
            if pd.notna(r.get("pct_change")):
                parts.append(f"{r['fuel_group']}: **{r['pct_change']:+.2f}%**")
        if parts:
            lines.append("การเปลี่ยนแปลงในช่วงที่เลือก (เฉลี่ยรายวันต่อกลุ่ม): " + " · ".join(parts) + " ")
    if not swing.empty:
        lines.append(
            "**ช่วงที่เคลื่อนไหวแรงสุด (7 วัน):** "
            + " · ".join(
                f"{r['fuel_group']}: swing {r['swing_pct']:.1f}% ({r['window_start'].strftime('%Y-%m-%d')}–{r['window_end'].strftime('%Y-%m-%d')})"
                for _, r in swing.iterrows()
            )
        )
    if not trend.empty:
        lines.append(
            "**แนวโน้มล่าสุด (เทียบ ~10 วันล่าสุด กับ 10 วันก่อน):** "
            + " · ".join(
                f"{r['fuel_group']}: {r['trend']}"
                + (f" ({r['delta_pct']:+.2f}%)" if pd.notna(r.get("delta_pct")) else "")
                for _, r in trend.iterrows()
            )
        )
    if not yoy_mom.empty and "yoy_pct" in yoy_mom.columns:
        yparts = []
        for _, r in yoy_mom.iterrows():
            yv, mv = r.get("yoy_pct"), r.get("mom_pct")
            if pd.notna(yv) or pd.notna(mv):
                ys = f"{yv:+.1f}%" if pd.notna(yv) else "n/a"
                ms = f"{mv:+.1f}%" if pd.notna(mv) else "n/a"
                yparts.append(f"{r['fuel_group']}: YoY {ys} · MoM {ms}")
        if yparts:
            lines.append("**YoY / MoM** (เทียบค่าเฉลี่ย 7 วัน): " + " · ".join(yparts))
    return "\n\n".join(lines)
