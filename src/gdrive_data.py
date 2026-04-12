"""
ดึงไฟล์ชุดข้อมูล (Excel / CSV) จาก Google Drive ด้วย file ID หรือลิงก์แชร์แบบ Anyone with the link

ใช้แพ็กเกจ `gdown` — ไม่ต้องใช้ Google API OAuth สำหรับไฟล์ที่แชร์แบบลิงก์
"""

from __future__ import annotations

import re
from pathlib import Path


def extract_google_drive_file_id(url_or_id: str) -> str:
    """รับได้ทั้ง raw file id หรือ URL แบบ https://drive.google.com/file/d/ID/view"""
    s = (url_or_id or "").strip()
    if not s:
        return ""
    if "/file/d/" in s or "/d/" in s:
        part = s.split("/d/")[1] if "/d/" in s else s
        return part.split("/")[0].split("?")[0]
    m = re.search(r"[?&]id=([^&]+)", s)
    if m:
        return m.group(1)
    if re.fullmatch(r"[A-Za-z0-9_-]{10,}", s):
        return s
    return s


def download_gdrive_file(file_id: str, dest: Path) -> Path:
    """
    ดาวน์โหลดไฟล์ไปที่ dest (ระบุชื่อไฟล์เต็ม รวมนามสกุล .xlsx หรือ .csv)
    ไฟล์บน Drive ต้องตั้งค่าแชร์อย่างน้อยเป็น 'ผู้ที่มีลิงก์' (Anyone with the link)
    """
    import gdown

    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    fid = extract_google_drive_file_id(file_id)
    if not fid:
        raise ValueError("FUEL_DATA_GDRIVE_FILE_ID / gdrive.file_id ว่างหรือไม่ถูกต้อง")

    url = f"https://drive.google.com/uc?id={fid}"
    try:
        gdown.download(url, str(dest), quiet=False, fuzzy=False)
    except Exception as e:
        raise RuntimeError(
            "ดาวน์โหลดจาก Google Drive ไม่สำเร็จ — ตรวจว่าแชร์ไฟล์เป็น "
            "'Anyone with the link' / 'ผู้ที่มีลิงก์' และ file ID ถูกต้อง\n"
            f"รายละเอียด: {e}"
        ) from e

    if not dest.is_file():
        raise RuntimeError(f"หลังดาวน์โหลดไม่พบไฟล์ที่ {dest}")
    if dest.stat().st_size == 0:
        raise RuntimeError("ไฟล์ที่ดาวน์โหลดมีขนาด 0 ไบต์ — ตรวจสิทธิ์แชร์หรือลิงก์")

    return dest
