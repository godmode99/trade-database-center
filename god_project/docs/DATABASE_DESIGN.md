# Database Design (v1) จาก ref โปรเจคเก่า

อัปเดตล่าสุด: 2026-03-03 (review รอบปรับปรุง)

## 1) ขอบเขตที่ใช้ในการออกแบบ
อ้างอิงจากข้อมูลจริงใน `refจากโปรเจคเก่า/python/Data/raw_data` แยกเป็น 4 แหล่ง:
- `fred` (daily/weekly/monthly JSON + fetch manifest)
- `calendar` (event list JSON/CSV + metadata)
- `mt5` (OHLCV raw CSV + feature CSV + summary JSON + manifest)
- `cme_fedwatch` (มีตัวอย่างไฟล์แล้วทั้ง `fedwatch_quotes` และ `fedwatch_probabilities`)

แนวคิดหลักของ schema:
1. แยก **raw layer** กับ **serving layer** ชัดเจน
2. เก็บ **ingestion run + manifest** สำหรับ trace และ audit
3. ใช้ unique key ตามธรรมชาติของข้อมูลเพื่อรองรับ upsert/idempotent

---

## 2) ER ภาพรวม (เชิง logical)

- `ingestion_runs` 1:N ไปยังทุกตาราง raw/serving (ผ่าน `ingestion_run_id`)
- FRED:
  - `fred_series`
  - `fred_observations`
- Calendar:
  - `calendar_events`
- MT5:
  - `mt5_symbols`
  - `mt5_ohlcv`
  - `mt5_features`
  - `mt5_summaries`
- CME FedWatch:
  - `fedwatch_probabilities`
  - `fedwatch_quotes`

---

## 3) ตารางกลางสำหรับ ingestion/monitoring

### 3.1 `ingestion_runs`
ใช้เก็บสถานะการรันงานแต่ละครั้งของแต่ละ source/timeframe

คอลัมน์หลัก:
- `id` (uuid, pk)
- `source` (enum: `FRED|CALENDAR|MT5|CME_FEDWATCH`)
- `mode` (เช่น `daily|weekly|monthly|intraday`)
- `started_at`, `finished_at`
- `status` (`running|success|partial|failed`)
- `trigger_type` (`schedule|manual|backfill`)
- `notes` (text)
- `manifest_payload` (jsonb) — เก็บไฟล์ fetch_manifest ดิบ
- `created_at`

ดัชนี:
- `(source, mode, started_at desc)`
- `(status, started_at desc)`

---

## 4) ออกแบบรายแหล่งข้อมูล

## 4.1 FRED

### 4.1.1 `fred_series`
- `id` (uuid, pk)
- `series_code` (text, unique) เช่น `DGS2`, `EFFR`
- `title` (text, nullable)
- `frequency` (text, nullable)
- `unit` (text, nullable)
- `is_active` (bool)
- `created_at`, `updated_at`

### 4.1.2 `fred_observations`
รองรับข้อมูลในไฟล์ลักษณะ `series -> [{date, value}]`

- `id` (bigserial, pk)
- `series_id` (fk -> fred_series.id)
- `observation_date` (date)
- `value_num` (numeric(18,6), nullable) — กรณี value missing ให้เป็น null
- `value_raw` (text, nullable) — เก็บค่าเดิมจาก source เพื่อ debug
- `mode` (text: daily/weekly/monthly)
- `ingestion_run_id` (fk -> ingestion_runs.id)
- `source_file` (text, nullable)
- `created_at`

unique key:
- `(series_id, observation_date, mode)`

ดัชนี:
- `(series_id, observation_date desc)`
- `(mode, observation_date desc)`

---

## 4.2 Calendar

### 4.2.1 `calendar_events`
ข้อมูล event จาก `calendar_all_event.json` / `calendar_select_events.json`

- `id` (bigserial, pk)
- `event_id` (text)
- `event_datetime_bkk` (timestamptz)
- `dateline_epoch` (bigint, nullable)
- `day_label` (text, nullable)
- `currency` (text, nullable)
- `country` (text, nullable)
- `impact` (text, nullable)
- `impact_score` (int, nullable)
- `time_label` (text, nullable)
- `name` (text)
- `prefixed_name` (text, nullable)
- `actual` (text, nullable)
- `forecast` (text, nullable)
- `previous` (text, nullable)
- `revision` (text, nullable)
- `url` (text, nullable)
- `solo_url` (text, nullable)
- `is_selected` (bool, default false) — มาจากชุด select_events
- `ingestion_run_id` (fk)
- `updated_at`, `created_at`

unique key:
- `(event_id, event_datetime_bkk)`

ดัชนี:
- `(event_datetime_bkk desc)`
- `(currency, event_datetime_bkk desc)`
- `(impact_score desc, event_datetime_bkk desc)`

หมายเหตุ:
- รองรับการอัปเดตค่า `actual/forecast/previous/revision` เมื่อข่าวออกภายหลัง

---

## 4.3 MT5

### 4.3.1 `mt5_symbols`
- `id` (uuid, pk)
- `symbol` (text, unique) เช่น `EURUSD`
- `description` (text, nullable)
- `created_at`, `updated_at`

### 4.3.2 `mt5_ohlcv`
รองรับ raw CSV (`time_th, open, high, low, close, tick_volume`)

- `id` (bigserial, pk)
- `symbol_id` (fk)
- `timeframe` (text: `M15|H4|D1|W1|MN1`)
- `candle_time_bkk` (timestamptz)
- `open`, `high`, `low`, `close` (numeric(18,6))
- `tick_volume` (bigint)
- `ingestion_run_id` (fk)
- `created_at`

unique key:
- `(symbol_id, timeframe, candle_time_bkk)`

ดัชนี:
- `(symbol_id, timeframe, candle_time_bkk desc)`

### 4.3.3 `mt5_features`
รองรับ feature CSV ที่คำนวณต่อจาก OHLCV

- `id` (bigserial, pk)
- `symbol_id` (fk)
- `timeframe` (text)
- `candle_time_bkk` (timestamptz)
- `tr`, `atr14`, `range`, `body`, `upper_wick`, `lower_wick`, `close_pos` (numeric)
- `swing_high`, `swing_low` (bool)
- `structure_event` (text, nullable)
- `bos_up`, `bos_dn`, `choch_up`, `choch_dn` (int)
- `ema20`, `ema50` (numeric)
- `prev_high`, `prev_low`, `prev_close` (numeric, nullable) — map ได้ทั้ง pdh/pdl/pdc หรือ pwh/pwl/pwc หรือ pmh/pml/pmc
- `sweep_prev_high`, `sweep_prev_low` (int)
- `ingestion_run_id` (fk)
- `created_at`

unique key:
- `(symbol_id, timeframe, candle_time_bkk)`

หมายเหตุ:
- ใช้คอลัมน์กลาง `prev_high/prev_low/prev_close` เพื่อ normalize ความต่างชื่อคอลัมน์จากแต่ละ timeframe

### 4.3.4 `mt5_summaries`
เก็บ summary JSON รายรอบ เช่น `daily_summary_*.json`

- `id` (bigserial, pk)
- `symbol_id` (fk)
- `mode` (text: daily/weekly/monthly)
- `asof` (timestamptz)
- `summary_payload` (jsonb)
- `ingestion_run_id` (fk)
- `created_at`

ดัชนี:
- `(symbol_id, mode, asof desc)`

---

## 4.4 CME FedWatch

> อัปเดตจากการตรวจ ref: มีไฟล์ตัวอย่างทั้ง quotes และ probabilities แล้ว จึง refine schema ให้สอดคล้อง field จริง

### 4.4.1 `fedwatch_probabilities`
- `id` (bigserial, pk)
- `instrument` (text: `SOFR|ZQ`)
- `table_name` (text) — เช่น `SR1`, `SR3` จากไฟล์ `tables`
- `symbol` (text)
- `contract_month` (text)
- `prediction` (numeric(8,4), nullable)
- `current` (numeric(8,4), nullable)
- `diff` (numeric(8,4), nullable)
- `asof_at` (timestamptz)
- `source_file` (text, nullable)
- `ingestion_run_id` (fk)
- `created_at`

unique key:
- `(instrument, table_name, symbol, contract_month, asof_at)`

### 4.4.2 `fedwatch_quotes`
- `id` (bigserial, pk)
- `symbol` (text) — จาก `Code`
- `display_name` (text, nullable) — จาก `Name`
- `mode` (text: daily/weekly/monthly)
- `quote_date` (date, nullable)
- `expiry_label` (text, nullable)
- `last_price`, `change`, `open`, `high`, `low` (numeric, nullable)
- `volume` (bigint, nullable)
- `front_month` (text, nullable)
- `captured_at` (timestamptz)
- `source_file` (text, nullable)
- `ingestion_run_id` (fk)
- `created_at`

unique key:
- `(symbol, mode, captured_at)`

---

## 5) มาตรฐานเพิ่มสำหรับใช้งานจริง

1. ทุกตารางหลักมี `ingestion_run_id` เพื่อ trace กลับไปที่ run ต้นทาง
2. ค่าเวลาใน DB ใช้ `timestamptz` (UTC ใน DB) แต่อนุญาตแปลงจาก `time_th`/`datetime_bkk` ตอน ingest
3. มี job data quality ขั้นต่ำ:
   - duplicate key check ตาม unique key ของแต่ละตาราง
   - null ratio ของคอลัมน์สำคัญ (`value_num`, `close`, `actual`)
   - freshness check ต่อ source/mode
4. เตรียม materialized view สำหรับเว็บ:
   - `vw_fred_latest_by_series`
   - `vw_calendar_upcoming_high_impact`
   - `vw_mt5_latest_feature_by_symbol_tf`
5. เพิ่มคอลัมน์ lineage/quality ที่ใช้ร่วมกันในทุกตาราง fact หลัก:
   - `source_record_hash` (text, nullable) สำหรับ idempotent ingest ที่ payload เปลี่ยน
   - `is_deleted` (bool, default false) สำหรับรองรับ source ที่ยกเลิกรายการในอนาคต
   - `updated_at` (timestamptz) เพื่อรองรับ merge/upsert รอบถัดไป
6. นโยบายเวลา:
   - เก็บ timestamp ใน DB เป็น UTC (`timestamptz`)
   - ถ้ามาจาก timezone เฉพาะ (เช่น BKK) ให้แปลงก่อนเขียน และเก็บค่าเดิมลง `source_time_label` เมื่อต้อง debug

---

## 6) ลำดับการ implement ต่อ (แนะนำ)
1. สร้าง Prisma schema จากตาราง `ingestion_runs`, `fred_*`, `calendar_events`, `mt5_*`
2. ทำ migration v1 + seed ข้อมูลจากไฟล์ ref ชุดเล็กเพื่อทดสอบ unique key/upsert
3. สร้าง data contract JSON schema ให้ตรง field ที่ออกแบบ
4. ทำ migration ของ CME FedWatch ตาม field จริงที่ตรวจจาก ref และทดสอบ parser สำหรับ `tables` (probabilities) + watchlist (quotes)
