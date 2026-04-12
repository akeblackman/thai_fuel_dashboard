#!/usr/bin/env bash
# อัปเดตข้อมูลน้ำมัน (ดึง + clean) — ใช้กับ cron ให้รันทุกวัน 06:00
#
# ตัวอย่าง crontab (แก้ path และ python ให้ตรงเครื่อง):
#   0 6 * * * /path/to/project/scripts/daily_update.sh >> /path/to/project/logs/daily_update.log 2>&1
#
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
if [[ -f .venv/bin/activate ]]; then
  # shellcheck source=/dev/null
  source .venv/bin/activate
fi
exec python update_fuel_data.py
