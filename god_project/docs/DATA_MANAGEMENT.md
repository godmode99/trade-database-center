# เอกสารการจัดการข้อมูล (Data Management)

อัปเดตล่าสุด: 2026-03-03

เอกสารนี้สรุปแนวทางการจัดการข้อมูลของ `god_project` ตั้งแต่การดึงข้อมูลดิบ (ingestion) ไปจนถึงการแสดงผลบนเว็บ โดยอิง pattern จากโปรเจคเก่า (FRED/MT5/Calendar/CME) ที่เน้น `retry + manifest + logging`.

## 1) วัตถุประสงค์
- ทำให้ข้อมูลจากหลายแหล่งถูกจัดเก็บอย่างเป็นระบบ และตรวจสอบย้อนกลับได้
- รองรับการรันงานซ้ำแบบปลอดภัย (idempotent)
- แยกชั้นข้อมูลให้ชัดเจน: raw → normalized → api/read model
- ทำให้เว็บเรียกข้อมูลได้เสถียร โดยไม่ผูกกับรูปแบบ raw โดยตรง

## 2) ขอบเขตข้อมูล
ครอบคลุม 4 แหล่งข้อมูลหลัก:
1. **FRED** (daily/weekly/monthly)
2. **CME FedWatch** (probabilities + quotes)
3. **Calendar** (weekly snapshot + actual update)
4. **MT5** (OHLCV หลาย timeframe + feature engineering)

## 3) สถาปัตยกรรมชั้นข้อมูล

```text
[Source APIs/Scraping]
        ↓
   (fetch pipelines)
        ↓
 [Raw Layer - เก็บ snapshot + metadata]
        ↓
 [Normalize/Transform Layer]
        ↓
 [Normalized Layer - พร้อม query]
        ↓
 [API/Read Model Layer]
        ↓
 [Web: trade-data-hub]
```

## 4) มาตรฐานการ ingest
แนวทางเดียวกันทุก source:
- มี `timeout` และ `retry` ในการดึงข้อมูล
- บันทึก **raw snapshot** พร้อมเวลา run
- สร้าง **manifest** ทุกครั้งหลัง run เพื่อสรุปผล
- เก็บ **error report** เมื่อเกิดความล้มเหลว
- รองรับการรันซ้ำแบบไม่สร้างข้อมูลซ้ำ (dedup/upsert key)

### 4.1 Manifest ที่ต้องมี
manifest ควรมีข้อมูลอย่างน้อย:
- run timestamp (UTC/TH)
- source + mode/timeframe
- สถานะราย series/endpoint (`ok`, `rows`, `latest`, `error`)
- รายการ stale/failed source
- notes ของ run

## 5) Data contracts
เก็บไว้ใน `god_project/data_contracts/` แยกเป็น:
- `raw/` : สัญญาข้อมูลดิบจากแต่ละ source
- `normalized/` : schema หลังแปลงให้ใช้งานร่วมกันได้
- `api/` : read model สำหรับหน้าเว็บ/API

หลักการ:
- ระบุฟิลด์บังคับ/ฟิลด์ optional ชัดเจน
- ระบุชนิดข้อมูลและหน่วยวัด
- ระบุคีย์ dedup/upsert ต่อ dataset
- version contract เมื่อมี breaking change

## 6) Database strategy (Prisma + Neon)
โครงสร้างเชิงแนวคิด:
- **raw tables**: เก็บ payload/record ต้นทาง + source metadata
- **normalized tables**: เก็บข้อมูลที่พร้อมนำไปใช้ query และทำ visualization
- **ingestion manifest tables**: เก็บสถานะ run, latency, row count, error summary

หลักการสำคัญ:
- ใช้ index ตาม `source + symbol/series + timestamp`
- แยก partition/logical grouping ตาม source/timeframe ที่เหมาะสม
- ออกแบบ migration ให้เพิ่ม source ใหม่ได้โดยไม่กระทบของเดิม

## 7) Data quality และ monitoring
ต้องมี checks ขั้นพื้นฐาน:
1. **Schema validation**: โครงสร้างตรงตาม contract
2. **Freshness check**: ข้อมูลล่าสุดไม่เก่ากว่า SLA
3. **Duplicate check**: ไม่ซ้ำตามคีย์ธุรกิจ
4. **Null/outlier check**: ตรวจความผิดปกติฟิลด์สำคัญ

เมื่อ check ไม่ผ่าน:
- บันทึกเหตุการณ์ใน manifest/error table
- ส่ง alert ตาม policy ใน `ops/alerts`

## 8) การนำข้อมูลขึ้นเว็บ
เว็บ (`web_trade-data-hub`) ควรดึงจาก normalized/api layer เท่านั้น
- ไม่ query raw payload ตรง
- มี endpoint/view ที่ยึด read model กลาง
- รองรับการแสดงสถานะ ingestion ล่าสุด (สุขภาพระบบข้อมูล)

## 9) ลำดับพัฒนาที่แนะนำ (Data-first)
1. ออกแบบ `schema.prisma` v1 (raw + normalized + manifest)
2. นิยาม data contracts ชุดแรก (FRED daily)
3. ทำ pipeline FRED daily ให้ครบ E2E
4. เพิ่ม quality checks + alert ขั้นพื้นฐาน
5. ขยายไป MT5, Calendar, CME ตามลำดับ

## 10) Definition of Done (งานด้าน data)
งาน ingest 1 source ถือว่าเสร็จเมื่อ:
- ดึงข้อมูลสำเร็จและบันทึก raw ได้
- มี manifest ระบุสถานะ run ชัดเจน
- แปลงลง normalized table ได้
- ผ่าน quality checks ขั้นพื้นฐาน
- เว็บสามารถอ่านผ่าน read model ได้


## 11) ประเด็นที่ปรับจากการ review ล่าสุด
1. **CME FedWatch ไม่ควรถูกมองว่าไม่มี sample แล้ว**
   - ใน ref มีทั้ง `fedwatch_quotes` และ `fedwatch_probabilities` แล้ว
   - ให้ใช้ field จริงจากไฟล์ตัวอย่างเป็นฐานของ contract/migration รอบแรก
2. **เพิ่มมาตรฐาน lineage/soft-delete**
   - แนะนำเพิ่ม `source_record_hash`, `updated_at`, `is_deleted` ใน fact tables เพื่อรองรับ incremental merge
3. **บังคับนโยบายเวลาเป็น UTC ใน DB**
   - source ที่เป็นเวลา BKK (เช่น Calendar/MT5) ต้องแปลงเป็น UTC ก่อนเขียนลงตาราง
   - เก็บ source time label เดิมไว้สำหรับ debug ได้ตามความเหมาะสม
4. **Manifest ควรรองรับ quality metrics ขั้นต่ำ**
   - เพิ่ม `row_count`, `inserted_count`, `updated_count`, `error_count`, `duration_ms`
   - เพื่อให้ dashboard ingestion เปรียบเทียบผลราย run ได้ง่าย
