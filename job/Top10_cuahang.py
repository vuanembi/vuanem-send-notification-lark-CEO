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
USER_ID = ["VN0039"] # User ID của tao: a8cc1a8f
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

# ==== 2. Query BigQuery với điều kiện loại bỏ POP stores ====
client = bigquery.Client(project=PROJECT_ID)

store_query = '''
DECLARE today DATE DEFAULT CURRENT_DATE("Asia/Ho_Chi_Minh");
DECLARE yesterday DATE DEFAULT DATE_SUB(today, INTERVAL 1 DAY);
DECLARE start_curr_month DATE DEFAULT DATE_TRUNC(today, MONTH);
DECLARE start_prev_month DATE DEFAULT DATE_SUB(start_curr_month, INTERVAL 1 MONTH);
DECLARE end_prev_month DATE DEFAULT DATE_SUB(DATE_SUB(today, INTERVAL 1 DAY), INTERVAL 1 MONTH);
DECLARE start_yoy_month DATE DEFAULT DATE_SUB(start_curr_month, INTERVAL 1 YEAR);
DECLARE end_yoy_month DATE DEFAULT DATE_SUB(yesterday, INTERVAL 1 YEAR);

WITH so AS (
  SELECT 
    l.STORE_NAME,
    DATE(s.TRANDATE) AS dt,
    s.NET_AMOUNT
  FROM `voltaic-country-280607.OP_Sales.SA_SalesOrder_` s
  LEFT JOIN `voltaic-country-280607.OP_Dimensions.DI_Locations_` l
    ON s.LOCATION_ID = l.LOCATION_ID
  WHERE (l.CLOSE_DATE IS NULL OR DATE(l.CLOSE_DATE) >= CURRENT_DATE())
    AND l.STORE_TYPE = "Kho Chính"
    AND l.CURRENT_ASM IS NOT NULL
    AND l.group_channel <> "Others"
    AND l.STORE_NAME <> "HNIL2-KC/Hà Nội-Lazada2"
    AND NOT STARTS_WITH(l.STORE_NAME, "POP")
),
agg AS (
  SELECT 
    STORE_NAME,
    SUM(CASE WHEN dt BETWEEN start_curr_month AND yesterday THEN NET_AMOUNT ELSE 0 END) AS revenue_curr,
    SUM(CASE WHEN dt BETWEEN start_prev_month AND end_prev_month THEN NET_AMOUNT ELSE 0 END) AS revenue_prev,
    SUM(CASE WHEN dt BETWEEN start_yoy_month AND end_yoy_month THEN NET_AMOUNT ELSE 0 END) AS revenue_yoy
  FROM so
  GROUP BY STORE_NAME
),
growth AS (
  SELECT 
    STORE_NAME,
    revenue_curr AS SO_Thang_nay,
    revenue_prev AS SO_Thang_truoc,
    revenue_yoy AS SO_Nam_truoc,
    ROUND(SAFE_DIVIDE(revenue_curr - revenue_prev, revenue_prev) * 100, 2) AS MoM_pct,
    ROUND(SAFE_DIVIDE(revenue_curr - revenue_yoy, revenue_yoy) * 100, 2) AS YoY_pct
  FROM agg
  WHERE revenue_curr > 0 OR revenue_prev > 0 OR revenue_yoy > 0
)

-- Top 10 SO cao nhất
SELECT * FROM (
  SELECT 'Top_10_SO_Cao_nhat' AS Loai, 
         STORE_NAME, 
         SO_Thang_nay, 
         SO_Thang_truoc, 
         MoM_pct, 
         SO_Nam_truoc, 
         YoY_pct
  FROM growth
  ORDER BY SO_Thang_nay DESC
  LIMIT 10
)

UNION ALL

-- Top 10 SO thấp nhất
SELECT * FROM (
  SELECT 'Top_10_SO_Thap_nhat' AS Loai, 
         STORE_NAME, 
         SO_Thang_nay, 
         SO_Thang_truoc, 
         MoM_pct, 
         SO_Nam_truoc, 
         YoY_pct
  FROM growth
  ORDER BY SO_Thang_nay ASC
  LIMIT 10
)
'''

print("📊 Đang query store performance data...")
df = client.query(store_query).to_dataframe()
print(f"✅ Query hoàn thành: {len(df)} dòng dữ liệu")

# ==== 3. Tách data theo Loai ====
top_stores = df[df['Loai'] == 'Top_10_SO_Cao_nhat'][['STORE_NAME', 'SO_Thang_nay', 'SO_Thang_truoc', 'MoM_pct', 'SO_Nam_truoc', 'YoY_pct']]
bottom_stores = df[df['Loai'] == 'Top_10_SO_Thap_nhat'][['STORE_NAME', 'SO_Thang_nay', 'SO_Thang_truoc', 'MoM_pct', 'SO_Nam_truoc', 'YoY_pct']]

print(f"📋 Data breakdown: Top stores={len(top_stores)}, Bottom stores={len(bottom_stores)}")

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

def format_percent(num):
    if num is None or str(num).lower() == 'nan':
        return "N/A"
    return f"{num:.1f}%"

def shorten_store_name(name, max_length=60):  # Tăng từ 35 lên 60 ký tự
    """Rút gọn tên cửa hàng nếu quá dài"""
    if len(name) <= max_length:
        return name
    return name[:max_length-3] + "..."

# Format bảng 1: Top stores với highlighting theo SO tháng này
top_stores_formatted = []
top_so_values = []  # Đổi từ top_mom_values sang top_so_values

for _, row in top_stores.iterrows():
    top_stores_formatted.append([
        shorten_store_name(row['STORE_NAME']),
        format_revenue(row['SO_Thang_nay']),
        format_revenue(row['SO_Thang_truoc']),
        format_percent(row['MoM_pct']),
        format_revenue(row['SO_Nam_truoc']),
        format_percent(row['YoY_pct'])
    ])
    top_so_values.append(row['SO_Thang_nay'] if not pd.isna(row['SO_Thang_nay']) else 0)

# Add symbols for extreme performers in top stores based on SO tháng này
if len(top_so_values) > 1:
    max_so_idx = np.argmax(top_so_values)  # SO cao nhất
    min_so_idx = np.argmin(top_so_values)  # SO thấp nhất
    top_stores_formatted[max_so_idx][0] += " ★★"  # SO cao nhất
    if min_so_idx != max_so_idx:  # Avoid duplicate if same store
        top_stores_formatted[min_so_idx][0] += " !!"  # SO thấp nhất

# Format bảng 2: Bottom stores với highlighting theo SO tháng này
bottom_stores_formatted = []
bottom_so_values = []  # Đổi từ bottom_mom_values sang bottom_so_values

for _, row in bottom_stores.iterrows():
    bottom_stores_formatted.append([
        shorten_store_name(row['STORE_NAME']),
        format_revenue(row['SO_Thang_nay']),
        format_revenue(row['SO_Thang_truoc']),
        format_percent(row['MoM_pct']),
        format_revenue(row['SO_Nam_truoc']),
        format_percent(row['YoY_pct'])
    ])
    bottom_so_values.append(row['SO_Thang_nay'] if not pd.isna(row['SO_Thang_nay']) else 0)

# Add symbols for extreme performers in bottom stores based on SO tháng này
if len(bottom_so_values) > 1:
    max_so_idx = np.argmax(bottom_so_values)  # SO cao nhất trong nhóm bottom
    min_so_idx = np.argmin(bottom_so_values)  # SO thấp nhất trong nhóm bottom
    bottom_stores_formatted[max_so_idx][0] += " ★★"  # SO cao nhất trong nhóm bottom
    if min_so_idx != max_so_idx:
        bottom_stores_formatted[min_so_idx][0] += " !!"  # SO thấp nhất trong nhóm bottom

# ==== 5. Professional Table Drawing Function ====
yesterday = (datetime.today() - timedelta(days=1)).strftime("%d-%m-%Y")

def draw_professional_store_table(ax, data, cols, title, color, so_values, fontsize=12):  # Đổi từ mom_values sang so_values
    ax.axis('off')
    if not data:
        data = [["Không có dữ liệu", "-", "-", "-", "-", "-"]]
        so_values = [0]
    
    # Find extreme values for highlighting based on SO tháng này
    if len(so_values) > 1:
        max_so_idx = np.argmax(so_values)  # SO cao nhất
        min_so_idx = np.argmin(so_values)  # SO thấp nhất
    else:
        max_so_idx = min_so_idx = -1
    
    tbl = ax.table(cellText=data, colLabels=cols, cellLoc="center", loc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(fontsize)
    tbl.scale(1.5, 2.2)  # Tăng scale để borders rõ ràng hơn
    
    # Title
    ax.text(0.5, 0.95, title, transform=ax.transAxes, ha='center', va='top', 
            fontsize=18, fontweight='bold')
    
    # Column width adjustment - Đảm bảo tổng = 1.0 để không bị overlap
    col_widths = [0.46, 0.12, 0.12, 0.1, 0.12, 0.08]  # Tổng = 1.00, cột tên rộng hơn
    for i, width in enumerate(col_widths):
        for j in range(len(data) + 1):
            tbl[(j, i)].set_width(width)
    
    # Styling cho từng cell
    for (row, col), cell in tbl.get_celld().items():
        # Border styling - Tăng độ rõ ràng của viền
        cell.set_edgecolor('#888888')  # Đậm hơn từ #CCCCCC
        cell.set_linewidth(1.0)        # Dày hơn từ 0.5
        
        if row == 0:  # Header row
            cell.set_facecolor(color)
            cell.set_text_props(weight='bold', color='black', fontsize=fontsize+2)
            cell.set_height(0.08)
        else:  # Data rows
            data_row_idx = row - 1
            
            # Alternative row colors with special highlighting based on SO tháng này
            if data_row_idx % 2 == 0:
                base_color = '#F8F9FA'  # Light gray for even rows
            else:
                base_color = 'white'    # White for odd rows
            
            # Highlight extreme performers based on SO tháng này
            if data_row_idx == max_so_idx and len(so_values) > 1:
                base_color = '#D4F4DD'  # Light green for highest SO
            elif data_row_idx == min_so_idx and len(so_values) > 1:
                base_color = '#FFE8E8'  # Light red for lowest SO
            
            cell.set_facecolor(base_color)
            cell.set_height(0.07)
            
            # Text styling
            if col == 0:  # Store name column
                cell_text = cell.get_text().get_text()
                if "★★" in cell_text:
                    cell.set_text_props(color='#28A745', fontweight='bold', fontsize=fontsize-1)
                elif "!!" in cell_text:
                    cell.set_text_props(color='#DC3545', fontweight='bold', fontsize=fontsize-1)
                else:
                    cell.set_text_props(color='black', fontsize=fontsize-1)
                # Left align for store names
                cell.get_text().set_horizontalalignment('left')
            elif col in [3, 5]:  # MoM and YoY percentage columns
                cell_text = cell.get_text().get_text()
                if cell_text != "N/A" and cell_text != "-":
                    try:
                        val = float(cell_text.replace('%', ''))
                        if val > 0:
                            cell.set_text_props(color='#28A745', fontweight='bold', fontsize=fontsize-1)
                        elif val < 0:
                            cell.set_text_props(color='#DC3545', fontweight='bold', fontsize=fontsize-1)
                        else:
                            cell.set_text_props(color='black', fontsize=fontsize-1)
                    except:
                        cell.set_text_props(color='black', fontsize=fontsize-1)
                else:
                    cell.set_text_props(color='black', fontsize=fontsize-1)
            else:
                cell.set_text_props(color='black', fontsize=fontsize-1)

# ==== 6. Create individual table images ====
def create_store_table_image(data, cols, title, color, so_values, filename):  # Đổi từ mom_values sang so_values
    fig, ax = plt.subplots(1, 1, figsize=(18, 12))  # Tăng width từ 16 lên 18 để chứa tên dài hơn
    
    draw_professional_store_table(ax, data, cols, title, color, so_values)
    
    # Add footnote about symbols - sửa lại mô tả
    fig.text(0.5, 0.08, "★★ Cửa hàng có SO tháng này cao nhất    !! Cửa hàng có SO tháng này thấp nhất", 
             ha='center', va='bottom', fontsize=12, style='italic', color='#666666')
    
    plt.subplots_adjust(top=0.90, bottom=0.15)
    plt.tight_layout()
    plt.savefig(filename, bbox_inches="tight", dpi=400, facecolor='white')  # Tăng DPI từ 300 lên 400
    plt.close()
    print(f"✅ Ảnh đã lưu: {filename}")

def upload_and_send_store_image(filename, description):
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

# ==== 7. Tạo và gửi 2 ảnh riêng biệt ====

# Ảnh 1: Top 10 stores
create_store_table_image(top_stores_formatted,
          ["Tên cửa hàng", "SO tháng này", "SO tháng trước", "%MoM", "SO năm trước", "%YoY"], 
          f"Top 10 cửa hàng doanh thu cao nhất ({yesterday})", "#A9D08E", top_so_values,
          "top_stores_performance.png")

# Ảnh 2: Bottom 10 stores  
create_store_table_image(bottom_stores_formatted,
          ["Tên cửa hàng", "SO tháng này", "SO tháng trước", "%MoM", "SO năm trước", "%YoY"], 
          f"Top 10 cửa hàng doanh thu thấp nhất ({yesterday})", "#FFB3BA", bottom_so_values,
          "bottom_stores_performance.png")

# Gửi 2 ảnh về USER_ID
upload_and_send_store_image("top_stores_performance.png", "top stores performance")
upload_and_send_store_image("bottom_stores_performance.png", "bottom stores performance")

print("🎉 HOÀN THÀNH! Đã gửi 2 ảnh báo cáo hiệu suất cửa hàng về Lark cá nhân của bạn!")
print("📱 1. Top 10 cửa hàng doanh thu cao nhất")
print("📱 2. Top 10 cửa hàng doanh thu thấp nhất") 
print("🔔 Check Lark để xem kết quả!")

#cửa hàng metric