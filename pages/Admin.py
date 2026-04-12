"""
หน้าผู้ดูแล — รัน pipeline อัปเดตข้อมูลด้วยมือ (ซ่อนจากเมนูข้าง — เข้า URL โดยตรง)
ตัวอย่าง: http://localhost:8501/Admin

ตั้งรหัสผ่านก่อนใช้งาน:
  export ADMIN_PASSWORD='รหัสของคุณ'
หรือใน .streamlit/secrets.toml:
  ADMIN_PASSWORD = "..."
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import streamlit as st

ROOT_DIR = Path(__file__).resolve().parent.parent
UPDATE_SCRIPT = ROOT_DIR / "update_fuel_data.py"


def _expected_password() -> str:
    v = os.environ.get("ADMIN_PASSWORD", "").strip()
    if v:
        return v
    try:
        return str(st.secrets["ADMIN_PASSWORD"]).strip()
    except (FileNotFoundError, KeyError, TypeError, AttributeError):
        return ""


def _require_login() -> bool:
    if st.session_state.get("_admin_ok"):
        return True
    expected = _expected_password()
    if not expected:
        st.error(
            "ยังไม่ได้ตั้งรหัสผ่านผู้ดูแล — ตั้งค่า environment variable `ADMIN_PASSWORD` "
            "หรือ `ADMIN_PASSWORD` ใน `.streamlit/secrets.toml` ก่อนใช้หน้านี้"
        )
        return False
    st.markdown("### เข้าสู่ระบบผู้ดูแล")
    pwd = st.text_input("รหัสผ่าน", type="password", autocomplete="current-password")
    if st.button("เข้าสู่ระบบ"):
        if pwd == expected:
            st.session_state._admin_ok = True
            st.rerun()
        else:
            st.error("รหัสผ่านไม่ถูกต้อง")
    return False


st.set_page_config(page_title="Admin — อัปเดตข้อมูล", layout="centered", page_icon="⚙️")

try:
    st.sidebar.page_link("app.py", label="← กลับแดชบอร์ดหลัก")
except Exception:
    pass

if not _require_login():
    st.stop()

st.title("อัปเดตข้อมูลน้ำมัน (ผู้ดูแล)")
st.caption(f"รัน `{UPDATE_SCRIPT.name}` — ดึงจาก สนพ. → clean → `fuel_prices_cleaned.xlsx`")

if st.button("รันดึงข้อมูล + clean ตอนนี้", type="primary"):
    if not UPDATE_SCRIPT.is_file():
        st.error(f"ไม่พบสคริปต์: {UPDATE_SCRIPT}")
    else:
        with st.spinner("กำลังรัน pipeline (อาจใช้เวลาหลายนาที)…"):
            r = subprocess.run(
                [sys.executable, str(UPDATE_SCRIPT)],
                cwd=str(ROOT_DIR),
                capture_output=True,
                text=True,
            )
        if r.returncode == 0:
            st.success("อัปเดตสำเร็จ — รีเฟรชหน้าแดชบอร์ดหลักเพื่อโหลดข้อมูลใหม่")
            if r.stdout:
                st.code(r.stdout, language="text")
        else:
            st.error("รันไม่สำเร็จ")
            st.code((r.stderr or r.stdout or "").strip() or f"exit {r.returncode}", language="text")

st.divider()
st.markdown(
    "สำหรับอัปเดตอัตโนมัติทุกวัน **06:00** ให้ตั้ง cron เรียก `scripts/daily_update.sh` "
    "(ดูคอมเมนต์ในสคริปต์)"
)
