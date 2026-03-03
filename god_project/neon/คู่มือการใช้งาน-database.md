# คู่มือการใช้งาน Database (Neon) สำหรับ Trade Database Center

เอกสารนี้สรุปวิธีใช้งานฐานข้อมูล Neon ของโปรเจคปัจจุบัน (`god_project`) ตั้งแต่การเตรียมค่าเชื่อมต่อ, การรัน migration, การตรวจสอบ schema ไปจนถึงตัวอย่าง query ที่ใช้งานจริงสำหรับทีม fetch/API/web

## 1) โครงสร้างโดยรวม

ฐานข้อมูลแบ่งเป็น 4 schema หลัก:

- `ops` : บันทึกสถานะการรัน pipeline
- `raw` : ข้อมูลดิบจากแหล่งข้อมูล (calendar, fred, mt5, cme)
- `features` : ข้อมูลที่คำนวณเพิ่มเติม
- `normalized` : view สำหรับอ่านข้อมูลล่าสุดเพื่อนำไปใช้ใน API/web

ไฟล์ migration หลักของระบบ:

- `god_project/neon/migrations/202602280001_schema_v1.sql`

## 2) เตรียม environment (แบบทีละขั้น)

> สรุปสั้น: ให้ใช้ไฟล์ `neon.env` (ที่ได้จาก Neon Dashboard) เป็นฐาน แล้ว map ค่าไปยัง env ที่สคริปต์ใช้งาน
> - migration SQL ใช้ `NEON_URL` (แนะนำให้ map จาก `DATABASE_URL` ใน `neon.env`)
> - pipeline calendar ใช้ `SUPABASE_URL` + `SUPABASE_SERVICE_ROLE_KEY` (แนะนำให้ map `SUPABASE_URL` จาก `POSTGRES_URL`)

### 2.1 สิ่งที่ต้องเตรียมก่อน

1. มี Neon project ที่สร้างฐานข้อมูลไว้แล้ว
2. ดาวน์โหลดไฟล์ `neon.env` จากหน้า Dashboard ของ Neon แล้ววางไว้ที่ root repo (`/workspace/trade-database-center/neon.env`)
3. มี service role key สำหรับ REST write (กรณีรัน pipeline ที่เขียน DB)

### 2.2 ตัวแปร env ที่ใช้งานจริง

- `DATABASE_URL` : อยู่ใน `neon.env` (ค่า pooled connection จาก Neon)
- `POSTGRES_URL` : อยู่ใน `neon.env` (ใช้เป็น base URL สำหรับ map ไป `SUPABASE_URL`)
- `NEON_URL` : env ที่ใช้กับ `psql`/migration (แนะนำ map จาก `DATABASE_URL`)
- `SUPABASE_URL` : env ที่ pipeline calendar อ่านเพื่อยิง REST endpoint (แนะนำ map จาก `POSTGRES_URL`)
- `SUPABASE_SERVICE_ROLE_KEY` : key สำหรับเขียนข้อมูลผ่าน REST

> หมายเหตุ: pipeline ปัจจุบันที่ `god_project/fetch/calendar/main.py` ยังอ่านชื่อ env ชุด `SUPABASE_*` เป็นหลัก

### 2.3 แนะนำวิธีตั้งค่าแบบง่ายสุด (Bash)

```bash
# 1) โหลดค่าพื้นฐานจาก neon.env
set -a
source neon.env
set +a

# 2) map ค่าให้ตรงกับ env ที่เครื่องมือในโปรเจคใช้งาน
export NEON_URL="${DATABASE_URL}"
export SUPABASE_URL="${POSTGRES_URL}"
export SUPABASE_SERVICE_ROLE_KEY="<your_service_role_key>"
```

### 2.4 ทางเลือก: เก็บค่าใน `neon.env` ให้ครบและ source ก่อนรัน

ตัวอย่าง `neon.env` ที่เติมค่าสำหรับ pipeline:

```env
DATABASE_URL=postgresql://<user>:<password>@<host>/<db>?sslmode=require
POSTGRES_URL=https://<project-ref>.neon.tech
SUPABASE_SERVICE_ROLE_KEY=<your_service_role_key>
```

ก่อนรันคำสั่ง ให้ `source neon.env` และ map เป็น `NEON_URL`/`SUPABASE_URL` ตามข้อ 2.3

### 2.5 เช็กว่าตั้ง env ถูกแล้ว

```bash
# ต้องเห็นค่าไม่ว่าง
echo "$DATABASE_URL" | cut -c1-40
echo "$POSTGRES_URL"
echo "$NEON_URL" | cut -c1-40
echo "$SUPABASE_URL"
echo "$SUPABASE_SERVICE_ROLE_KEY" | wc -c
```

ถ้า `wc -c` ได้ค่ามากกว่า `1` แปลว่ามีค่า key แล้ว

## 3) วิธีรัน migration

### 3.1 รัน schema v1

```bash
psql "$NEON_URL" -f god_project/neon/migrations/202602280001_schema_v1.sql
```

### 3.2 ตรวจสอบว่า schema และตารางถูกสร้างแล้ว

```bash
psql "$NEON_URL" -c "\dn"
psql "$NEON_URL" -c "\dt ops.*"
psql "$NEON_URL" -c "\dt raw.*"
psql "$NEON_URL" -c "\dt features.*"
psql "$NEON_URL" -c "\dv normalized.*"
```

## 4) ตารางสำคัญที่ต้องรู้

- `ops.pipeline_runs` : เก็บสถานะ run (`running/success/failed/...`) และจำนวน rows
- `raw.calendar_events` : ข่าวเศรษฐกิจดิบ
- `raw.fred_observations` : time series ของ FRED
- `raw.mt5_ohlcv` : ราคา OHLCV จาก MT5
- `raw.cme_quotes` : ราคา/volume ของ CME
- `raw.cme_probabilities` : FedWatch probabilities
- `features.mt5_price_features` : feature ของราคา MT5
- `features.calendar_surprise` : surprise metric ของข่าว

## 5) แนวทางเขียนข้อมูล (Upsert Pattern)

ตารางในชั้น `raw` และ `features` ถูกออกแบบให้ upsert ได้จาก business key ที่กำหนดไว้ด้วย unique index

ตัวอย่าง: upsert `raw.calendar_events`

```sql
INSERT INTO raw.calendar_events (
  run_id, source, source_ref, event_id, event_time_utc,
  currency, impact, impact_score, event_name,
  actual, forecast, previous, payload
)
VALUES (
  NULL, 'forexfactory', 'weekly-fetch', 'FF-123', '2026-03-01T13:30:00Z',
  'USD', 'high', 3, 'Non-Farm Payrolls',
  NULL, '200K', '175K', '{"sample": true}'::jsonb
)
ON CONFLICT (source, event_id, event_time_utc)
DO UPDATE SET
  actual = EXCLUDED.actual,
  forecast = EXCLUDED.forecast,
  previous = EXCLUDED.previous,
  payload = EXCLUDED.payload,
  updated_at_utc = NOW();
```

## 6) ตัวอย่าง query สำหรับ API/Web

### 6.1 ข่าวล่าสุด (ใช้ view)

```sql
SELECT *
FROM normalized.calendar_events_latest
WHERE event_time_utc >= NOW() - INTERVAL '7 days'
ORDER BY event_time_utc ASC
LIMIT 200;
```

### 6.2 ค่า FRED ล่าสุดของแต่ละ series/frequency

```sql
SELECT *
FROM normalized.fred_series_latest
ORDER BY series_id, frequency;
```

### 6.3 ราคา MT5 ล่าสุดราย symbol/timeframe

```sql
SELECT *
FROM normalized.mt5_latest_bar
WHERE symbol = 'EURUSD'
ORDER BY timeframe;
```

## 7) การติดตามสุขภาพ pipeline

### 7.1 ดู run ล่าสุด

```sql
SELECT run_id, pipeline_name, status, started_at_utc, ended_at_utc, rows_in, rows_out, rows_error
FROM ops.pipeline_runs
ORDER BY started_at_utc DESC
LIMIT 50;
```

### 7.2 ดู run ที่ล้มเหลว

```sql
SELECT run_id, pipeline_name, status, error_message, started_at_utc
FROM ops.pipeline_runs
WHERE status = 'failed'
ORDER BY started_at_utc DESC
LIMIT 20;
```

## 8) คำแนะนำการใช้งานในงานจริง

- ให้ pipeline ทุกตัวเขียน `ops.pipeline_runs` ก่อนเริ่มและปิดสถานะหลังจบ
- ใช้ `normalized.*` สำหรับ API/web เป็นค่าเริ่มต้น ลดความซับซ้อนฝั่ง frontend
- เก็บ payload ดิบใน `jsonb` เพื่อ debug ย้อนหลังเมื่อ source เปลี่ยน format
- ถ้าเป็น scheduler/CI ควรรันโหมดที่บังคับให้มี credentials ครบ (strict mode)

## 9) Troubleshooting เบื้องต้น

- เชื่อมต่อไม่ได้:
  - ตรวจสอบว่ามีการ `source neon.env` แล้ว และ `NEON_URL` map มาจาก `DATABASE_URL` ถูกต้อง
  - ยืนยันว่าใส่ `sslmode=require`
- migration ไม่ผ่าน:
  - ตรวจสอบสิทธิ์ user ว่าสร้าง schema/table ได้
  - รันแบบ transaction (ไฟล์นี้มี `BEGIN/COMMIT` อยู่แล้ว)
- ข้อมูลซ้ำ:
  - ตรวจสอบว่าค่า business key ที่ส่งเข้า upsert ตรงตาม unique index ของตาราง

---

หากมีการเปลี่ยน schema เพิ่มเติม ให้เพิ่ม migration ใหม่ใน `god_project/neon/migrations/` และอัปเดตเอกสารนี้ทุกครั้ง
