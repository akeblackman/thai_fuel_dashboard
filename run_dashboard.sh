#!/usr/bin/env bash
# รัน pipeline ดึงข้อมูล + clean ก่อน แล้วเปิด Streamlit (เหมาะกับการ start แบบ one-shot)
# หน้าเว็บไม่ดึงข้อมูลซ้ำทุกครั้งที่โหลด (ค่าเริ่มต้น AUTO_UPDATE_FUEL_DATA=0)
set -euo pipefail
cd "$(dirname "$0")"
if [[ -f .venv/bin/activate ]]; then
  # shellcheck source=/dev/null
  source .venv/bin/activate
fi
python update_fuel_data.py
export AUTO_UPDATE_FUEL_DATA=0
exec streamlit run app.py
