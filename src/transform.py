import pandas as pd

def transform_products(df):
    # เลือกคอลัมน์ที่ต้องการจากไฟล์ CSV [Item , Category , PPU] 
    # ลบค่าซ้ำ ตรงนี้เราลบเฉพาะมันเป็น Pimary Key 
    dim_products = df[['Item', 'Category', 'Price Per Unit']].drop_duplicates(subset=['Item'])
    # ลบแถวที่ Item เป็น Null เพราะเป็น PK 
    dim_products = dim_products.dropna(subset=['Item'])
    
    # เปลี่ยนชื่อคอลัมน์ให้ตรงกับ SQL
    dim_products.columns = ['item_name', 'category', 'price_per_unit']

    return dim_products

def transform_date(df):
    # ดึงคอลัมน์วันที่และแปลงเป็น Datetime
    dim_date = pd.DataFrame(df['Transaction Date'].unique(), columns=['full_date'])
    dim_date['full_date'] = pd.to_datetime(dim_date['full_date'])
    
    # เรียงวันที่ให้สวยงาม
    dim_date = dim_date.sort_values('full_date').reset_index(drop=True)
    
    # สร้าง date_id (เริ่มจาก 1001)
    dim_date['date_id'] = dim_date.index + 1001 
    
    
    # สกัดเป็นข้อมูลย่อย
    dim_date['year'] = dim_date['full_date'].dt.year
    dim_date['month'] = dim_date['full_date'].dt.month
    dim_date['day'] = dim_date['full_date'].dt.day
    dim_date['weekday'] = dim_date['full_date'].dt.day_name()
    
    # เปลี่ยนชื่อคอลัมน์ให้ตรงกับ SQL schema และจัดลำดับคอลัมน์
    dim_date = dim_date[['date_id', 'full_date', 'year', 'month', 'day', 'weekday']]
    
    return dim_date

def transform_fact(df, dim_date):
    # 1. ทำความสะอาดข้อมูลเบื้องต้น
    # ลบแถวที่ Item หรือ Quantity เป็น Null เพราะเป็นข้อมูลที่จำเป็น
    df = df.dropna(subset=['Item', 'Quantity'])
    
    # แทนที่ค่า Null ใน Discount Applied ด้วย False
    df['Discount Applied'] = df['Discount Applied'].fillna(False)
    
    # คำนวณ Total Spent ใหม่เพื่อให้ข้อมูลแม่นยำ (กันพลาดจาก Source)
    df['Total Spent'] = df['Quantity'] * df['Price Per Unit']
    
    # 2. จัดการเรื่องวันที่สำหรับการ Merge
    # เปลี่ยนคอลัมน์ Transaction Date เป็น datetime เพื่อให้ Type ตรงกันกับ dim_date
    df['Transaction Date'] = pd.to_datetime(df['Transaction Date'])
    
    # ตรวจสอบให้แน่ใจว่า key ที่จะ merge ใน dim_date เป็น datetime เหมือนกัน
    dim_date['full_date'] = pd.to_datetime(dim_date['full_date'])
    
    # 3. Merge df (ตารางยอดขาย) กับ dim_date เพื่อดึง date_id มาใช้แทนวันที่เดิม
    # ตรงนี้เหมือน การ Join ใน SQL
    fact_transactions = df.merge(
        dim_date[['date_id', 'full_date']], # เลือกมาเฉพาะที่ใช้
        left_on='Transaction Date', # df ใช้ Transaction Date
        right_on='full_date', # dim_date ใช้ full_date
        how='left'
    )
    
    # 4. เลือกคอลัมน์ที่ต้องการ 
    fact_transactions = fact_transactions[[
        'date_id',
        'Customer ID',
        'Item',
        'Quantity',
        'Total Spent',
        'Discount Applied',
        'Payment Method',
        'Location'
    ]].copy()
    
    # 5. เปลี่ยนชื่อคอลัมน์ให้ตรงกับ SQL schema (Snake Case)
    fact_transactions.columns = [
        'date_id',
        'customer_id',
        'item_name',
        'quantity',
        'total_spent',
        'discount_applied',
        'payment_method',
        'location'
    ]
    
    return fact_transactions