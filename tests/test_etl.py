# สมมติว่ารับค่า df ทั้ง 3 มาจาก transform แล้ว
# dim_date, dim_products, fact_transactions

def test_data_quality(dim_date, dim_products, fact_transactions):
    print("---- เริ่มการตรวจสอบข้อมูล (Data Validation) ----")

    # 1. ตรวจสอบ dim_date
    # เช็คว่า date_id ห้ามซ้ำ
    assert dim_date['date_id'].is_unique, "Error: date_id ใน dim_date มีค่าซ้ำ!"
    # เช็คว่าไม่มีค่า Null ใน date_id
    assert dim_date['date_id'].notnull().all(), "Error: พบค่า Null ใน date_id!"
    print("dim_date: ผ่าน")

    # 2. ตรวจสอบ dim_products
    # เช็คชื่อคอลัมน์ว่าตรงกับ SQL Schema เป๊ะๆ ไหม
    expected_cols = ['item_name', 'category', 'price_per_unit']
    assert list(dim_products.columns) == expected_cols, f"Error: ชื่อคอลัมน์ dim_products ไม่ตรง! พบ: {dim_products.columns}"
    print("dim_products: ผ่าน")

    # 3. ตรวจสอบ fact_transactions (สำคัญที่สุด)
    # เช็คว่า date_id ใน fact ต้องมีตัวตนจริงใน dim_date (Referential Integrity)
    # (เอา date_id ของ fact มาเทียบกับ date_id ของ dim)
    valid_ids = dim_date['date_id'].unique()
    is_subset = fact_transactions['date_id'].isin(valid_ids).all()
    assert is_subset, "Error: พบ date_id ใน Transaction ที่ไม่มีอยู่ในปฏิทิน (Join ไม่เจอ)!"
    
    # เช็คว่ายอดเงินห้ามติดลบ
    assert (fact_transactions['total_spent'] >= 0).all(), "Error: พบยอดขายติดลบ!"
    print("fact_transactions: ผ่าน")
    
    print("ข้อมูลถูกต้องพร้อม Load ลง Database แล้ว!")

# วิธีเรียกใช้ (หลังจาก transform เสร็จ)
# test_data_quality(dim_date, dim_products, fact_transactions)