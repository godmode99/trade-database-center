# DB / Prisma setup

โปรเจคนี้ใช้ Prisma 7-style config:

- เก็บ datasource URL ใน `db/prisma.config.ts`
- ไม่กำหนด `url` ใน `db/prisma/schema.prisma`

## Prisma Client constructor

เมื่อเริ่มใช้งาน `PrismaClient` ใน runtime ให้ส่งค่า config ผ่าน constructor แทน datasource url ใน schema
โดยเลือกอย่างใดอย่างหนึ่ง:

1. Direct DB connection: ส่ง `adapter`
2. Prisma Accelerate: ส่ง `accelerateUrl`

อ้างอิง:
- https://pris.ly/d/config-datasource
- https://pris.ly/d/prisma7-client-config
