## Thailand Fuel Prices Dashboard (Python)

เว็บแดชบอร์ดสำหรับ monitoring ราคาน้ำมันในประเทศไทยจากไฟล์ Excel `fuel_prices_th_2004_6apr2026.xlsx`  
รองรับการเลือกช่วงเวลา, เลือกบริษัท/ชนิดน้ำมันเพื่อเปรียบเทียบแนวโน้มแบบ time series และดาวน์โหลดข้อมูลที่ clean แล้ว

### สิ่งที่ต้องมี
- Python 3.9+

### ติดตั้ง

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### รันเว็บ

```bash
streamlit run app.py
```

### โครงสร้างไฟล์หลัก
- `app.py`: หน้า dashboard (Streamlit)
- `src/data.py`: โหลด + clean + cache ข้อมูลจาก Excel
- `.streamlit/config.toml`: ธีม UI
- `fuel_prices_cleaned.xlsx`: ข้อมูลราคา (ใช้ในเครื่องพัฒนาได้) — บน deploy แนะนำดึงจาก Google Drive แทน (ไม่ต้องใส่ไฟล์ใหญ่ใน repo)

### ดึงชุดข้อมูลจาก Google Drive (ลดขนาด repo / deploy)

1. อัปโหลดไฟล์ `.xlsx` หรือ `.csv` ขึ้น Google Drive แล้วตั้ง **แชร์ → ผู้ที่มีลิงก์** (Anyone with the link)
2. คัดลอก **File ID** จาก URL (`https://drive.google.com/file/d/FILE_ID/view`) หรือวาง URL ทั้งแถวก็ได้
3. ตั้งค่าใดอย่างหนึ่ง:
   - **ตัวแปรแวดล้อม:** `FUEL_DATA_GDRIVE_FILE_ID` (บังคับ) และถ้าชื่อไฟล์บน Drive ไม่ใช่ `fuel_prices_cleaned.xlsx` ให้ตั้ง `FUEL_DATA_GDRIVE_FILENAME` ด้วย (เช่น `data.csv`)
   - **Streamlit Secrets (แนะนำบน Cloud):** สร้าง key `gdrive` ใน Secrets:

```toml
[gdrive]
file_id = "YOUR_FILE_ID_OR_FULL_URL"
filename = "fuel_prices_cleaned.xlsx"
```

4. ไฟล์จะถูกดาวน์โหลดไปที่ `.cache/gdrive_data/` (ไม่ commit — อยู่ใน `.gitignore`)
5. **ตัวเลือก:** `FUEL_GDRIVE_ALWAYS_REFRESH=1` = ดาวน์โหลดใหม่ทุกครั้งที่รันสคริปต์ (หนักกว่า แต่ได้ข้อมูลล่าสุดเสมอ); ค่าเริ่มต้นแคชต่อเซสชัน + ปุ่ม **รีโหลดจาก Google Drive** ในแถบข้าง
6. เมื่อใช้ Google Drive แล้ว แอปจะ **ไม่** รัน pipeline `update_fuel_data.py` อัตโนมัติแม้ตั้ง `AUTO_UPDATE_FUEL_DATA=1` (เพื่อไม่ให้ทับไฟล์ในเครื่องที่ไม่ใช่แหล่งจริง)

### Deploy บน Streamlit Community Cloud
1. Push โปรเจกต์ขึ้น GitHub (สาธารณะหรือ private ที่เชื่อม Streamlit)
2. ที่ [share.streamlit.io](https://share.streamlit.io) → **New app** → เลือก repo / branch / **Main file path:** `app.py`
3. **Python version:** 3.9+ (ตรงกับ `requirements.txt`)
4. ใส่ `fuel_prices_cleaned.xlsx` ใน repo **หรือ** ตั้ง `FUEL_DATA_GDRIVE_FILE_ID` / Secrets `gdrive.file_id` ให้ดึงจาก Drive (ไม่ต้อง commit ไฟล์ข้อมูล)
5. (ถ้าใช้ secrets) สร้าง `.streamlit/secrets.toml` บน Cloud ผ่ามเมนู Secrets — **อย่า** commit ไฟล์ secrets ลง git

หมายเหตุ: โฟลเดอร์ `.venv` ไม่ถูก push (ระบุใน `.gitignore`) — Cloud ติดตั้งแพ็กเกจจาก `requirements.txt` ให้อัตโนมัติ
# thai_fuel_dashboard
