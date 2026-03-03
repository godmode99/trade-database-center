# Project Structure (v1)

เอกสารนี้ใช้เป็นโครงสร้างมาตรฐานของโปรเจค `god_project` เพื่อรองรับงาน 3 แกนหลัก:
1) ingestion pipeline
2) database + data contract
3) web application

ออกแบบโดยอิงแนวทาง pipeline จากโปรเจคเก่า (FRED/MT5/Calendar/CME) ที่มี retry, manifest, และ logging.

## โครงสร้างโฟลเดอร์

```text
god_project/
├─ PROJECT_STRUCTURE.md
├─ fetch/
│  ├─ shared/                  # utility กลาง: retry, logger, io, manifest
│  ├─ fred/                    # pipeline แหล่ง FRED
│  ├─ mt5/                     # pipeline แหล่ง MT5
│  ├─ calendar/                # pipeline แหล่ง Calendar
│  └─ cme_fedwatch/            # pipeline แหล่ง CME FedWatch
├─ db/
│  ├─ prisma/                  # prisma schema + model layer
│  ├─ migrations/              # migration history
│  └─ seeds/                   # seed scripts
├─ data_contracts/
│  ├─ raw/                     # schema สัญญาข้อมูล raw ต่อ source
│  ├─ normalized/              # schema สำหรับ normalized table
│  └─ api/                     # schema/read model ที่เว็บใช้
├─ ops/
│  ├─ schedules/               # cron/vercel cron/job spec
│  └─ alerts/                  # alert rule + notify template
├─ docs/                       # เอกสารเชิงระบบ/ADR/ERD เพิ่มเติม
└─ web_trade-data-hub/         # next.js frontend (App Router)
```

## แนวทางวางไฟล์ภายในแต่ละส่วน

### 1) fetch/<source>/
แนะนำ pattern เดียวกันทุก source:
- `main.py` หรือ `main.ts` : entrypoint
- `pipeline.py` : orchestration logic
- `clients/` : external api/scraper
- `transform/` : parse + normalize
- `configs/` : source config (daily/weekly/monthly)
- `tests/` : parser/transform test

### 2) db/
- `prisma/schema.prisma` เป็น single source of truth ของ model
- `migrations/` เก็บ migration แบบ versioned
- `seeds/` เก็บ seed data สำหรับ local/dev

### 3) data_contracts/
- ใช้ไฟล์ YAML/JSON schema แยกตาม source และ timeframe
- กำหนด key สำหรับ dedup/upsert ให้ชัดเจน
- ใช้ร่วมกันระหว่าง fetch layer และ web/API layer

### 4) ops/
- แยกตารางเวลา run ต่อ source (daily/weekly/monthly)
- ระบุ timeout/retry/alert policy ชัดเจนใน job spec

## ลำดับการลงมือหลังออกแบบโครงสร้าง
1. สร้าง `db/prisma/schema.prisma` v1 (raw + normalized + manifest)
2. สร้าง shared utility ใน `fetch/shared/`
3. ทำ vertical slice แรก: `fetch/fred` -> DB -> เว็บ
4. เพิ่ม source อื่นตามลำดับ MT5 -> Calendar -> CME
