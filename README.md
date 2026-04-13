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
- `fuel_prices_cleaned.xlsx`: ข้อมูลราคา (ต้องมีใน repo หรืออัปโหลดบนเครื่องรัน)

### Deploy บน Streamlit Community Cloud
1. Push โปรเจกต์ขึ้น GitHub (สาธารณะหรือ private ที่เชื่อม Streamlit)
2. ที่ [share.streamlit.io](https://share.streamlit.io) → **New app** → เลือก repo / branch / **Main file path:** `app.py`
3. **Python version:** 3.9+ (ตรงกับ `requirements.txt`)
4. ตรวจว่า `fuel_prices_cleaned.xlsx` อยู่ใน repo (หรือตั้งค่าให้แอปดึงข้อมูลแบบอื่น — ต้องแก้โค้ดเพิ่ม)
5. (ถ้าใช้ secrets) สร้าง `.streamlit/secrets.toml` บน Cloud ผ่ามเมนู Secrets — **อย่า** commit ไฟล์ secrets ลง git

หมายเหตุ: โฟลเดอร์ `.venv` ไม่ถูก push (ระบุใน `.gitignore`) — Cloud ติดตั้งแพ็กเกจจาก `requirements.txt` ให้อัตโนมัติ
# thai_fuel_dashboard
