from playwright.sync_api import sync_playwright

URL = "https://www.forexfactory.com/calendar"
STATE_PATH = "ff_storage.json"

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # ต้องเห็นหน้าจอ
        context = browser.new_context(
            viewport={"width": 1400, "height": 900},
            locale="en-US",
        )
        page = context.new_page()
        page.goto(URL, wait_until="domcontentloaded", timeout=120000)

        print("\n- ทำให้หน้า Calendar โหลดได้ (เห็นตาราง/เนื้อหา) แล้วกลับมากด Enter ใน console\n")
        input("พร้อมแล้วกด Enter เพื่อบันทึก session... ")

        context.storage_state(path=STATE_PATH)
        print(f"✅ Saved session -> {STATE_PATH}")

        browser.close()

if __name__ == "__main__":
    main()
