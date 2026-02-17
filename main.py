import os
import sys

# 1. Import ฟังก์ชันจากลูกน้องแต่ละคน (Modules)
# ดึงความสามารถจาก folder src และ tests
from src.extract import extract_data
from src.transform import transform_products, transform_date, transform_fact
from src.load import load_to_db  # (สมมติว่าคุณมีไฟล์ load.py แล้ว)
from tests.test_etl import test_data_quality

def run_pipeline():
    """
    ฟังก์ชันหลักสำหรับรัน ETL Process ทั้งหมด
    """
    print("🚀 Starting ETL Pipeline...")
    
    # --- STEP 0: Setup Config ---
    # หาที่อยู่ไฟล์ CSV ให้เจอ ไม่ว่าจะรันจากโฟลเดอร์ไหน
    project_root = os.path.dirname(os.path.abspath(__file__))
    data_file_path = os.path.join(project_root, 'data', 'retail_store_sales.csv')

    # --- STEP 1: EXTRACT (ดึงข้อมูล) ---
    print(f"\n--- [1/4] Extracting Data ---")
    df = extract_data(data_file_path)
    
    # ถ้าดึงไม่สำเร็จ (เป็น None) ให้จบการทำงานทันที
    if df is None:
        print("❌ Extraction Failed. Stopping Pipeline.")
        return

    # --- STEP 2: TRANSFORM (แปลงข้อมูล) ---
    print(f"\n--- [2/4] Transforming Data ---")
    try:
        # 2.1 สร้าง Dim Products
        dim_products = transform_products(df)
        print(f"   ✅ Created dim_products: {len(dim_products)} rows")

        # 2.2 สร้าง Dim Date (ต้องทำก่อน Fact เพราะต้องใช้ date_id)
        dim_date = transform_date(df)
        print(f"   ✅ Created dim_date: {len(dim_date)} rows")

        # 2.3 สร้าง Fact Transactions (Merge date_id เข้ามา)
        fact_transactions = transform_fact(df, dim_date)
        print(f"   ✅ Created fact_transactions: {len(fact_transactions)} rows")

    except Exception as e:
        print(f"❌ Transformation Error: {e}")
        return

    # --- STEP 3: VALIDATE / TEST (ตรวจสอบคุณภาพ) ---
    print(f"\n--- [3/4] Validating Data ---")
    try:
        # เรียกใช้ฟังก์ชันเทสที่เราเขียนกันไว้
        test_data_quality(dim_date, dim_products, fact_transactions)
    except AssertionError as e:
        # ถ้า Test ไม่ผ่าน (เจอข้อมูลแย่ๆ) ให้หยุดทันที ห้าม Load
        print(f"❌ Data Quality Failed: {e}")
        return
    except Exception as e:
        print(f"❌ Unexpected Error during testing: {e}")
        return

    # --- STEP 4: LOAD (นำเข้า Database) ---
    print(f"\n--- [4/4] Loading Data to Database ---")
    try:
        load_to_db(dim_date, dim_products, fact_transactions)
        print("🎉 ETL Process Completed Successfully!")
    except Exception as e:
        print(f"❌ Loading Error: {e}")

# --- ENTRY POINT ---
if __name__ == "__main__":
    run_pipeline()