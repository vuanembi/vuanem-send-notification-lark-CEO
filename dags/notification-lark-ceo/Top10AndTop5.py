import os
import requests, json
import matplotlib.pyplot as plt
from google.cloud import bigquery
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

# ==== CONFIG ====
APP_ID = "cli_a630657c81a15010"
APP_SECRET = "oF4rME28LacN5hnEcoyj8e65UwhyhbLF"
USER_ID = ["VN0039","b96f5475"] # User ID của tao: a8cc1a8f
CHAT_ID = ["oc_82be130d7f567dc8c91828d6c0fb4bd2"]   # chat_id nhóm
PROJECT_ID = "voltaic-country-280607"

# 👉 chỉ định file JSON key service account
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"D:\voltaic-country-280607-3ff5e089e0b0.json"

# ==== 1. Get tenant access token ====
resp = requests.post(
    "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal",
    json={"app_id": APP_ID, "app_secret": APP_SECRET}
).json()
token = resp["tenant_access_token"]
print("🔑 Token response:", resp)

# ==== 2. Query BigQuery ====
client = bigquery.Client(project=PROJECT_ID)

mattress_query = '''
DECLARE today DATE DEFAULT CURRENT_DATE("Asia/Ho_Chi_Minh");
DECLARE yesterday DATE DEFAULT DATE_SUB(today, INTERVAL 1 DAY);
DECLARE start_curr_month DATE DEFAULT DATE_TRUNC(today, MONTH);
DECLARE start_prev_month DATE DEFAULT DATE_SUB(start_curr_month, INTERVAL 1 MONTH);
DECLARE end_prev_month DATE DEFAULT DATE_SUB(DATE_SUB(today, INTERVAL 1 DAY), INTERVAL 1 MONTH);

WITH so AS (
  SELECT 
    s.CLASS_ID,
    DATE(s.TRANDATE) AS dt,
    s.NET_AMOUNT,
    s.ITEM_COUNT
  FROM `voltaic-country-280607.OP_Sales.SA_SalesOrder_` s
),
class AS (
  SELECT 
    CLASS_ID,
    NAME
  FROM `voltaic-country-280607.OP_Dimensions.DI_Classes_`
  WHERE PRODUCT_GROUP = '1-Đệm/ Mattress'
),
agg AS (
  SELECT 
    so.CLASS_ID,
    SUM(CASE WHEN dt BETWEEN start_curr_month AND yesterday 
             THEN NET_AMOUNT ELSE 0 END) AS revenue_curr,
    SUM(CASE WHEN dt BETWEEN start_prev_month AND end_prev_month
             THEN NET_AMOUNT ELSE 0 END) AS revenue_prev,
    SUM(CASE WHEN dt BETWEEN start_curr_month AND yesterday 
             THEN ITEM_COUNT ELSE 0 END) AS qty_curr,
    SUM(CASE WHEN dt BETWEEN start_prev_month AND end_prev_month
             THEN ITEM_COUNT ELSE 0 END) AS qty_prev
  FROM so
  GROUP BY so.CLASS_ID
),
growth AS (
  SELECT 
    c.NAME AS San_pham,
    a.revenue_curr,
    a.revenue_prev,
    a.qty_curr,
    a.qty_prev,
    ROUND(SAFE_DIVIDE(a.revenue_curr - a.revenue_prev, a.revenue_prev) * 100, 2) AS Tang_truong_revenue,
    ROUND(SAFE_DIVIDE(a.qty_curr - a.qty_prev, a.qty_prev) * 100, 2) AS Tang_truong_qty
  FROM agg a
  JOIN class c ON a.CLASS_ID = c.CLASS_ID
  WHERE (a.revenue_curr > 0 OR a.revenue_prev > 0 OR a.qty_curr > 0 OR a.qty_prev > 0)
)

-- 1. Top 10 theo Doanh thu
SELECT * FROM (
  SELECT 'Nệm - Top 10 Doanh thu' AS Loai, 
         San_pham, 
         revenue_curr AS Thang_nay, 
         revenue_prev AS Thang_truoc, 
         Tang_truong_revenue AS Tang_truong
  FROM growth
  ORDER BY Thang_nay DESC
  LIMIT 10
)

UNION ALL

-- 2. Top 10 theo So luong
SELECT * FROM (
  SELECT 'Nệm - Top 10 Số lượng' AS Loai, 
         San_pham, 
         qty_curr AS Thang_nay, 
         qty_prev AS Thang_truoc, 
         Tang_truong_qty AS Tang_truong
  FROM growth
  ORDER BY Thang_nay DESC
  LIMIT 10
)

UNION ALL

-- 3. Top 10 theo Tang truong (theo Doanh thu)
SELECT * FROM (
  SELECT 'Nệm - Top 10 Tăng trưởng' AS Loai, 
         San_pham, 
         revenue_curr AS Thang_nay, 
         revenue_prev AS Thang_truoc, 
         Tang_truong_revenue AS Tang_truong
  FROM growth
  ORDER BY Tang_truong DESC
  LIMIT 10
)
'''

print("📊 Đang query mattress data...")
df = client.query(mattress_query).to_dataframe()
print(f"✅ Query hoàn thành: {len(df)} dòng dữ liệu")

# ==== 3. Tách data theo Loai ====
revenue_data = df[df['Loai'] == 'Nệm - Top 10 Doanh thu'][['San_pham', 'Thang_nay', 'Thang_truoc', 'Tang_truong']]
qty_data = df[df['Loai'] == 'Nệm - Top 10 Số lượng'][['San_pham', 'Thang_nay', 'Thang_truoc', 'Tang_truong']]
growth_data = df[df['Loai'] == 'Nệm - Top 10 Tăng trưởng'][['San_pham', 'Thang_nay', 'Thang_truoc', 'Tang_truong']]

print(f"📋 Data breakdown: Revenue={len(revenue_data)}, Qty={len(qty_data)}, Growth={len(growth_data)}")

# ==== 4. Format data ====
def format_revenue(num):
    if num >= 1e9:
        return f"{num/1e9:.1f} tỷ"
    elif num >= 1e6:
        return f"{num/1e6:.0f} triệu"
    elif num >= 1e3:
        return f"{num/1e3:.0f} nghìn"
    else:
        return f"{num:.0f}"

def format_qty(num):
    return f"{int(num):,}"

def format_percent(num):
    if num is None or str(num).lower() == 'nan':
        return "N/A"
    return f"{num:.1f}%"

# Format bảng 1: Doanh thu với star cho extreme values
revenue_formatted = []
revenue_growth_values = []
for _, row in revenue_data.iterrows():
    revenue_formatted.append([
        row['San_pham'],
        format_revenue(row['Thang_nay']),
        format_revenue(row['Thang_truoc']),
        format_percent(row['Tang_truong'])
    ])
    revenue_growth_values.append(row['Tang_truong'] if not pd.isna(row['Tang_truong']) else 0)

# Add colored symbols to extreme values in revenue data
if len(revenue_growth_values) > 1:
    max_idx = np.argmax(revenue_growth_values)
    min_idx = np.argmin(revenue_growth_values)
    revenue_formatted[max_idx][0] += " ★★"  # Double star for best performer
    revenue_formatted[min_idx][0] += " !!"   # Double exclamation for worst performer

# Format bảng 2: Số lượng với star cho extreme values
qty_formatted = []
qty_growth_values = []
for _, row in qty_data.iterrows():
    qty_formatted.append([
        row['San_pham'],
        format_qty(row['Thang_nay']),
        format_qty(row['Thang_truoc']),
        format_percent(row['Tang_truong'])
    ])
    qty_growth_values.append(row['Tang_truong'] if not pd.isna(row['Tang_truong']) else 0)

# Add colored symbols to extreme values in qty data
if len(qty_growth_values) > 1:
    max_idx = np.argmax(qty_growth_values)
    min_idx = np.argmin(qty_growth_values)
    qty_formatted[max_idx][0] += " ★★"
    qty_formatted[min_idx][0] += " !!"

# Format bảng 3: Tăng trưởng với star cho extreme values
growth_formatted = []
growth_growth_values = []
for _, row in growth_data.iterrows():
    growth_formatted.append([
        row['San_pham'],
        format_revenue(row['Thang_nay']),
        format_revenue(row['Thang_truoc']),
        format_percent(row['Tang_truong'])
    ])
    growth_growth_values.append(row['Tang_truong'] if not pd.isna(row['Tang_truong']) else 0)

# Add colored symbols to extreme values in growth data
if len(growth_growth_values) > 1:
    max_idx = np.argmax(growth_growth_values)
    min_idx = np.argmin(growth_growth_values)
    growth_formatted[max_idx][0] += " ★★"
    growth_formatted[min_idx][0] += " !!"

# ==== 5. Function để vẽ bảng professional ====
def draw_professional_table(ax, data, cols, title, color, growth_values, fontsize=16):
    ax.axis('off')
    if not data:
        data = [["Không có dữ liệu", "-", "-", "-"]]
        growth_values = [0]
    
    # Tìm max và min growth để highlight
    if len(growth_values) > 1:
        max_growth_idx = np.argmax(growth_values)
        min_growth_idx = np.argmin(growth_values)
    else:
        max_growth_idx = min_growth_idx = -1
    
    tbl = ax.table(cellText=data, colLabels=cols, cellLoc="center", loc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(fontsize)
    tbl.scale(1.4, 2.2)
    
    # Title ở đầu bảng
    ax.text(0.5, 0.95, title, transform=ax.transAxes, ha='center', va='top', 
            fontsize=20, fontweight='bold')
    
    # Styling cho từng cell
    for (row, col), cell in tbl.get_celld().items():
        # Border styling
        cell.set_edgecolor('#CCCCCC')
        cell.set_linewidth(0.5)
        
        if row == 0:  # Header row
            cell.set_facecolor(color)
            cell.set_text_props(weight='bold', color='black', fontsize=fontsize+2)
            cell.set_height(0.08)
        else:  # Data rows
            data_row_idx = row - 1
            
            # Alternative row colors
            if data_row_idx % 2 == 0:
                base_color = '#F8F9FA'  # Light gray for even rows
            else:
                base_color = 'white'    # White for odd rows
            
            # Highlight extreme values
            if data_row_idx == max_growth_idx and len(growth_values) > 1:
                base_color = '#D4F4DD'  # Light green for best performer
            elif data_row_idx == min_growth_idx and len(growth_values) > 1:
                base_color = '#FFE8E8'  # Light red for worst performer
            
            cell.set_facecolor(base_color)
            cell.set_height(0.07)
            
            # Color negative growth values in red and style symbols
            if col == 3:  # Growth column
                cell_text = cell.get_text().get_text()
                if cell_text != "N/A" and cell_text != "-":
                    try:
                        val = float(cell_text.replace('%', ''))
                        if val < 0:
                            cell.set_text_props(color='#DC3545', fontweight='bold', fontsize=fontsize)  # Red for negative
                        else:
                            cell.set_text_props(color='black', fontsize=fontsize)
                    except:
                        cell.set_text_props(color='black', fontsize=fontsize)
                else:
                    cell.set_text_props(color='black', fontsize=fontsize)
            elif col == 0:  # Product name column - handle symbols
                cell_text = cell.get_text().get_text()
                if "★★" in cell_text:
                    # Green color for double star (best performer)
                    cell.set_text_props(color='#28A745', fontweight='bold', fontsize=fontsize)
                elif "!!" in cell_text:
                    # Red color for double exclamation (worst performer)
                    cell.set_text_props(color='#DC3545', fontweight='bold', fontsize=fontsize)
                else:
                    cell.set_text_props(color='black', fontsize=fontsize)
            else:
                cell.set_text_props(color='black', fontsize=fontsize)

# ==== 6. Function tạo ảnh riêng biệt ====
yesterday = (datetime.today() - timedelta(days=1)).strftime("%d-%m-%Y")

def create_single_table_image(data, cols, title, color, growth_values, filename):
    fig, ax = plt.subplots(1, 1, figsize=(14, 12))  # Tăng height để có space cho footnote
    
    draw_professional_table(ax, data, cols, title, color, growth_values)
    
    # Add note about colored symbols ở dưới với spacing tốt hơn
    fig.text(0.5, 0.08, "★★ Sản phẩm tăng trưởng cao nhất    !! Sản phẩm giảm mạnh nhất", 
             ha='center', va='bottom', fontsize=12, style='italic', color='#666666')
    
    # Điều chỉnh layout để tránh overlap
    plt.subplots_adjust(top=0.90, bottom=0.15)
    plt.tight_layout()
    plt.savefig(filename, bbox_inches="tight", dpi=300, facecolor='white')
    plt.close()
    print(f"✅ Ảnh đã lưu: {filename}")

def upload_and_send_image(filename, description):
    with open(filename, "rb") as f:
        upload = requests.post(
            "https://open.larksuite.com/open-apis/im/v1/images",
            headers={"Authorization": f"Bearer {token}"},
            files={"image": (filename, f, "image/png")},
            data={"image_type": "message"}
        ).json()
    print(f"📤 Upload {description} response:", upload)
    
    image_key = upload["data"]["image_key"]
    
    # Gửi cho tất cả USER_ID
    for user_id in USER_ID:
        msg = {
            "receive_id": user_id,
            "msg_type": "image", 
            "content": json.dumps({"image_key": image_key})
        }
        resp = requests.post(
            "https://open.larksuite.com/open-apis/im/v1/messages?receive_id_type=user_id",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=msg
        ).json()
        print(f"📩 Send {description} to USER {user_id} response:", resp)
    
    # Gửi cho tất cả CHAT_ID  
    for chat_id in CHAT_ID:
        msg = {
            "receive_id": chat_id,
            "msg_type": "image",
            "content": json.dumps({"image_key": image_key})
        }
        resp = requests.post(
            "https://open.larksuite.com/open-apis/im/v1/messages?receive_id_type=chat_id",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}, 
            json=msg
        ).json()
        print(f"📩 Send {description} to CHAT {chat_id} response:", resp)

# Tạo 3 ảnh riêng biệt cho nệm
create_single_table_image(revenue_formatted, 
          ["Sản phẩm", "Tháng này", "Tháng trước", "Tăng trưởng"], 
          f"Nệm - Top 10 Doanh thu ({yesterday})", "#A9D08E", revenue_growth_values,
          "nem_doanh_thu.png")

create_single_table_image(qty_formatted, 
          ["Sản phẩm", "Tháng này", "Tháng trước", "Tăng trưởng"], 
          f"Nệm - Top 10 Số lượng ({yesterday})", "#9DC3E6", qty_growth_values,
          "nem_so_luong.png")

create_single_table_image(growth_formatted, 
          ["Sản phẩm", "Tháng này", "Tháng trước", "Tăng trưởng"], 
          f"Nệm - Top 10 Tăng trưởng ({yesterday})", "#FFD966", growth_growth_values,
          "nem_tang_truong.png")

# Gửi 3 ảnh nệm về USER_ID
upload_and_send_image("nem_doanh_thu.png", "nệm doanh thu")
upload_and_send_image("nem_so_luong.png", "nệm số lượng") 
upload_and_send_image("nem_tang_truong.png", "nệm tăng trưởng")

print("🎉 Đã gửi 3 ảnh báo cáo nệm về user cá nhân!")

# =================================================================
# PHẦN MỚI: CHĂN GA REPORT
# =================================================================

print("\n🛏️ Bắt đầu tạo báo cáo chăn ga...")

bedding_query = '''
DECLARE today DATE DEFAULT CURRENT_DATE("Asia/Ho_Chi_Minh");
DECLARE yesterday DATE DEFAULT DATE_SUB(today, INTERVAL 1 DAY);
DECLARE start_curr_month DATE DEFAULT DATE_TRUNC(today, MONTH);
DECLARE start_prev_month DATE DEFAULT DATE_SUB(start_curr_month, INTERVAL 1 MONTH);
DECLARE end_prev_month DATE DEFAULT DATE_SUB(DATE_SUB(today, INTERVAL 1 DAY), INTERVAL 1 MONTH);

WITH so AS (
  SELECT 
    s.CLASS_ID,
    DATE(s.TRANDATE) AS dt,
    s.NET_AMOUNT,
    s.ITEM_COUNT
  FROM `voltaic-country-280607.OP_Sales.SA_SalesOrder_` s
),
class AS (
  SELECT 
    CLASS_ID,
    NAME
  FROM `voltaic-country-280607.OP_Dimensions.DI_Classes_`
  WHERE PRODUCT_GROUP = '2-Chăn ga/ Bed Linen'
),
agg AS (
  SELECT 
    so.CLASS_ID,
    SUM(CASE WHEN dt BETWEEN start_curr_month AND yesterday 
             THEN NET_AMOUNT ELSE 0 END) AS revenue_curr,
    SUM(CASE WHEN dt BETWEEN start_prev_month AND end_prev_month
             THEN NET_AMOUNT ELSE 0 END) AS revenue_prev,
    SUM(CASE WHEN dt BETWEEN start_curr_month AND yesterday 
             THEN ITEM_COUNT ELSE 0 END) AS qty_curr,
    SUM(CASE WHEN dt BETWEEN start_prev_month AND end_prev_month
             THEN ITEM_COUNT ELSE 0 END) AS qty_prev
  FROM so
  GROUP BY so.CLASS_ID
),
growth AS (
  SELECT 
    c.NAME AS San_pham,
    a.revenue_curr,
    a.revenue_prev,
    a.qty_curr,
    a.qty_prev,
    ROUND(SAFE_DIVIDE(a.revenue_curr - a.revenue_prev, a.revenue_prev) * 100, 2) AS Tang_truong_revenue,
    ROUND(SAFE_DIVIDE(a.qty_curr - a.qty_prev, a.qty_prev) * 100, 2) AS Tang_truong_qty
  FROM agg a
  JOIN class c ON a.CLASS_ID = c.CLASS_ID
  WHERE (a.revenue_curr > 0 OR a.revenue_prev > 0 OR a.qty_curr > 0 OR a.qty_prev > 0)
)

-- 1. Top 5 theo Doanh thu
SELECT * FROM (
  SELECT 'Chăn ga - Top 5 Doanh thu' AS Loai, 
         San_pham, 
         revenue_curr AS Thang_nay, 
         revenue_prev AS Thang_truoc, 
         Tang_truong_revenue AS Tang_truong
  FROM growth
  ORDER BY Thang_nay DESC
  LIMIT 5
)

UNION ALL

-- 2. Top 5 theo So luong
SELECT * FROM (
  SELECT 'Chăn ga - Top 5 Số lượng' AS Loai, 
         San_pham, 
         qty_curr AS Thang_nay, 
         qty_prev AS Thang_truoc, 
         Tang_truong_qty AS Tang_truong
  FROM growth
  ORDER BY Thang_nay DESC
  LIMIT 5
)

UNION ALL

-- 3. Top 5 theo Tang truong (theo Doanh thu)
SELECT * FROM (
  SELECT 'Chăn ga - Top 5 Tăng trưởng' AS Loai, 
         San_pham, 
         revenue_curr AS Thang_nay, 
         revenue_prev AS Thang_truoc, 
         Tang_truong_revenue AS Tang_truong
  FROM growth
  ORDER BY Tang_truong DESC
  LIMIT 5
)
'''

print("📊 Đang query bedding data...")
df_bedding = client.query(bedding_query).to_dataframe()
print(f"✅ Bedding query hoàn thành: {len(df_bedding)} dòng dữ liệu")

# Tách data theo Loai
bedding_revenue_data = df_bedding[df_bedding['Loai'] == 'Chăn ga - Top 5 Doanh thu'][['San_pham', 'Thang_nay', 'Thang_truoc', 'Tang_truong']]
bedding_qty_data = df_bedding[df_bedding['Loai'] == 'Chăn ga - Top 5 Số lượng'][['San_pham', 'Thang_nay', 'Thang_truoc', 'Tang_truong']]
bedding_growth_data = df_bedding[df_bedding['Loai'] == 'Chăn ga - Top 5 Tăng trưởng'][['San_pham', 'Thang_nay', 'Thang_truoc', 'Tang_truong']]

print(f"📋 Bedding data breakdown: Revenue={len(bedding_revenue_data)}, Qty={len(bedding_qty_data)}, Growth={len(bedding_growth_data)}")

# Format bedding data với star cho extreme values
bedding_revenue_formatted = []
bedding_revenue_growth_values = []
for _, row in bedding_revenue_data.iterrows():
    bedding_revenue_formatted.append([
        row['San_pham'],
        format_revenue(row['Thang_nay']),
        format_revenue(row['Thang_truoc']),
        format_percent(row['Tang_truong'])
    ])
    bedding_revenue_growth_values.append(row['Tang_truong'] if not pd.isna(row['Tang_truong']) else 0)

# Add colored symbols to extreme values
if len(bedding_revenue_growth_values) > 1:
    max_idx = np.argmax(bedding_revenue_growth_values)
    min_idx = np.argmin(bedding_revenue_growth_values)
    bedding_revenue_formatted[max_idx][0] += " ★★"
    bedding_revenue_formatted[min_idx][0] += " !!"

bedding_qty_formatted = []
bedding_qty_growth_values = []
for _, row in bedding_qty_data.iterrows():
    bedding_qty_formatted.append([
        row['San_pham'],
        format_qty(row['Thang_nay']),
        format_qty(row['Thang_truoc']),
        format_percent(row['Tang_truong'])
    ])
    bedding_qty_growth_values.append(row['Tang_truong'] if not pd.isna(row['Tang_truong']) else 0)

# Add colored symbols to extreme values
if len(bedding_qty_growth_values) > 1:
    max_idx = np.argmax(bedding_qty_growth_values)
    min_idx = np.argmin(bedding_qty_growth_values)
    bedding_qty_formatted[max_idx][0] += " ★★"
    bedding_qty_formatted[min_idx][0] += " !!"

bedding_growth_formatted = []
bedding_growth_growth_values = []
for _, row in bedding_growth_data.iterrows():
    bedding_growth_formatted.append([
        row['San_pham'],
        format_revenue(row['Thang_nay']),
        format_revenue(row['Thang_truoc']),
        format_percent(row['Tang_truong'])
    ])
    bedding_growth_growth_values.append(row['Tang_truong'] if not pd.isna(row['Tang_truong']) else 0)

# Add colored symbols to extreme values
if len(bedding_growth_growth_values) > 1:
    max_idx = np.argmax(bedding_growth_growth_values)
    min_idx = np.argmin(bedding_growth_growth_values)
    bedding_growth_formatted[max_idx][0] += " ★★"
    bedding_growth_formatted[min_idx][0] += " !!"

# Tạo 3 ảnh riêng biệt cho chăn ga
create_single_table_image(bedding_revenue_formatted, 
          ["Sản phẩm", "Tháng này", "Tháng trước", "Tăng trưởng"], 
          f"Chăn ga - Top 5 Doanh thu ({yesterday})", "#A9D08E", bedding_revenue_growth_values,
          "changa_doanh_thu.png")

create_single_table_image(bedding_qty_formatted, 
          ["Sản phẩm", "Tháng này", "Tháng trước", "Tăng trưởng"], 
          f"Chăn ga - Top 5 Số lượng ({yesterday})", "#9DC3E6", bedding_qty_growth_values,
          "changa_so_luong.png")

create_single_table_image(bedding_growth_formatted, 
          ["Sản phẩm", "Tháng này", "Tháng trước", "Tăng trưởng"], 
          f"Chăn ga - Top 5 Tăng trưởng ({yesterday})", "#FFD966", bedding_growth_growth_values,
          "changa_tang_truong.png")

# Gửi 3 ảnh chăn ga về USER_ID
upload_and_send_image("changa_doanh_thu.png", "chăn ga doanh thu")
upload_and_send_image("changa_so_luong.png", "chăn ga số lượng")
upload_and_send_image("changa_tang_truong.png", "chăn ga tăng trưởng")

print("🎉 HOÀN THÀNH! Đã gửi 6 ảnh báo cáo về Lark cá nhân của bạn!")
print("📱 3 ảnh nệm: doanh thu, số lượng, tăng trưởng")
print("📱 3 ảnh chăn ga: doanh thu, số lượng, tăng trưởng")
print("🔔 Check Lark để xem kết quả!")

#product metric
