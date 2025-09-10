import os
import requests, json
import matplotlib.pyplot as plt
from google.cloud import bigquery
from datetime import datetime, timedelta
import numpy as np

# ==== CONFIG ====
APP_ID = "cli_a630657c81a15010"
APP_SECRET = "oF4rME28LacN5hnEcoyj8e65UwhyhbLF"
USER_ID = ["VN0039"]  # User ID của tao: a8cc1a8f
CHAT_ID = ["oc_82be130d7f567dc8c91828d6c0fb4bd2"]  # chat_id nhóm
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
query = "SELECT * FROM `voltaic-country-280607.Test.Sale_daily_lark`"
df = client.query(query).to_dataframe()
print("✅ Query thành công, có", len(df), "dòng dữ liệu")

# ==== 3. Hàm format số ====
def format_billion(num): return f"{(num or 0)/1e9:.2f} tỷ"
def format_million(num): return f"{(num or 0)/1e6:.2f} triệu"
def format_integer(num): return f"{int(num or 0):,}"

def format_percent_arrow(num, label, compare, unit_fn):
    n = num or 0
    arrow = "↗" if n > 0 else "↘" if n < 0 else "→"
    return f"{arrow}{n:.1f}% {label} ({unit_fn(compare)})"

# ==== 4. Parse data theo channel ====
data_map = {}
for _, r in df.iterrows():
    channel = r["group_channel"]
    data_map[channel] = {col: r[col] for col in df.columns}

# ==== 5. Chuẩn bị data cho 4 bảng với highlighting ====
yesterday = (datetime.today() - timedelta(days=1)).strftime("%d-%m-%Y")

# Data cho Daily với highlighting cho SO cao nhất
daily_data_raw = [
    ["Total", data_map["Total"]["SO_day"], data_map["Total"]["No_SO_day"], data_map["Total"]["AOV_day"]],
    ["Offline", data_map["Offline"]["SO_day"], data_map["Offline"]["No_SO_day"], data_map["Offline"]["AOV_day"]],
    ["Online", data_map["Online"]["SO_day"], data_map["Online"]["No_SO_day"], data_map["Online"]["AOV_day"]],
    ["Ecommerce", data_map["Ecommerce"]["SO_day"], data_map["Ecommerce"]["No_SO_day"], data_map["Ecommerce"]["AOV_day"]]
]

# Find max SO trong daily (bỏ qua Total - index 0)
daily_so_values = [row[1] for row in daily_data_raw[1:]]  # Skip Total
max_daily_so_idx = daily_so_values.index(max(daily_so_values)) + 1  # +1 vì bỏ qua Total

daily_data = []
for i, (ch, so, no, aov) in enumerate(daily_data_raw):
    channel_name = ch + " ★★" if i == max_daily_so_idx else ch
    daily_data.append([channel_name, format_billion(so), format_integer(no), format_million(aov)])

# Data cho MTD SO với highlighting cho SO cao nhất  
mtd_so_raw = [
    ["Total", data_map["Total"]["SO_MTD"], data_map["Total"]["SO_MoM_percent"], data_map["Total"]["SO_LMTD"], data_map["Total"]["SO_YoY_percent"], data_map["Total"]["SO_LYTD"]],
    ["Offline", data_map["Offline"]["SO_MTD"], data_map["Offline"]["SO_MoM_percent"], data_map["Offline"]["SO_LMTD"], data_map["Offline"]["SO_YoY_percent"], data_map["Offline"]["SO_LYTD"]],
    ["Online", data_map["Online"]["SO_MTD"], data_map["Online"]["SO_MoM_percent"], data_map["Online"]["SO_LMTD"], data_map["Online"]["SO_YoY_percent"], data_map["Online"]["SO_LYTD"]],
    ["Ecommerce", data_map["Ecommerce"]["SO_MTD"], data_map["Ecommerce"]["SO_MoM_percent"], data_map["Ecommerce"]["SO_LMTD"], data_map["Ecommerce"]["SO_YoY_percent"], data_map["Ecommerce"]["SO_LYTD"]]
]

# Find max SO MTD (bỏ qua Total)
mtd_so_values = [row[1] for row in mtd_so_raw[1:]]
max_mtd_so_idx = mtd_so_values.index(max(mtd_so_values)) + 1

mtd_so = []
for i, (ch, so, mom_pct, mom_val, yoy_pct, yoy_val) in enumerate(mtd_so_raw):
    channel_name = ch + " ★★" if i == max_mtd_so_idx else ch
    mtd_so.append([
        channel_name,
        format_billion(so),
        format_percent_arrow(mom_pct, "MoM", mom_val, format_billion),
        format_percent_arrow(yoy_pct, "YoY", yoy_val, format_billion)
    ])

# Data cho MTD Quantity với highlighting cho quantity cao nhất
mtd_no_raw = [
    ["Total", data_map["Total"]["No_SO_MTD"], data_map["Total"]["No_SO_MoM_percent"], data_map["Total"]["No_SO_LMTD"], data_map["Total"]["No_SO_YoY_percent"], data_map["Total"]["No_SO_LYTD"]],
    ["Offline", data_map["Offline"]["No_SO_MTD"], data_map["Offline"]["No_SO_MoM_percent"], data_map["Offline"]["No_SO_LMTD"], data_map["Offline"]["No_SO_YoY_percent"], data_map["Offline"]["No_SO_LYTD"]],
    ["Online", data_map["Online"]["No_SO_MTD"], data_map["Online"]["No_SO_MoM_percent"], data_map["Online"]["No_SO_LMTD"], data_map["Online"]["No_SO_YoY_percent"], data_map["Online"]["No_SO_LYTD"]],
    ["Ecommerce", data_map["Ecommerce"]["No_SO_MTD"], data_map["Ecommerce"]["No_SO_MoM_percent"], data_map["Ecommerce"]["No_SO_LMTD"], data_map["Ecommerce"]["No_SO_YoY_percent"], data_map["Ecommerce"]["No_SO_LYTD"]]
]

# Find max quantity MTD (bỏ qua Total)
mtd_no_values = [row[1] for row in mtd_no_raw[1:]]
max_mtd_no_idx = mtd_no_values.index(max(mtd_no_values)) + 1

mtd_no = []
for i, (ch, no, mom_pct, mom_val, yoy_pct, yoy_val) in enumerate(mtd_no_raw):
    channel_name = ch + " ★★" if i == max_mtd_no_idx else ch
    mtd_no.append([
        channel_name,
        format_integer(no),
        format_percent_arrow(mom_pct, "MoM", mom_val, format_integer),
        format_percent_arrow(yoy_pct, "YoY", yoy_val, format_integer)
    ])

# Data cho MTD AOV với highlighting cho AOV cao nhất
mtd_aov_raw = [
    ["Total", data_map["Total"]["AOV_MTD"], data_map["Total"]["AOV_MoM_percent"], data_map["Total"]["AOV_LMTD"], data_map["Total"]["AOV_YoY_percent"], data_map["Total"]["AOV_LYTD"]],
    ["Offline", data_map["Offline"]["AOV_MTD"], data_map["Offline"]["AOV_MoM_percent"], data_map["Offline"]["AOV_LMTD"], data_map["Offline"]["AOV_YoY_percent"], data_map["Offline"]["AOV_LYTD"]],
    ["Online", data_map["Online"]["AOV_MTD"], data_map["Online"]["AOV_MoM_percent"], data_map["Online"]["AOV_LMTD"], data_map["Online"]["AOV_YoY_percent"], data_map["Online"]["AOV_LYTD"]],
    ["Ecommerce", data_map["Ecommerce"]["AOV_MTD"], data_map["Ecommerce"]["AOV_MoM_percent"], data_map["Ecommerce"]["AOV_LMTD"], data_map["Ecommerce"]["AOV_YoY_percent"], data_map["Ecommerce"]["AOV_LYTD"]]
]

# Find max AOV MTD (bỏ qua Total)
mtd_aov_values = [row[1] for row in mtd_aov_raw[1:]]
max_mtd_aov_idx = mtd_aov_values.index(max(mtd_aov_values)) + 1

mtd_aov = []
for i, (ch, aov, mom_pct, mom_val, yoy_pct, yoy_val) in enumerate(mtd_aov_raw):
    channel_name = ch + " ★★" if i == max_mtd_aov_idx else ch
    mtd_aov.append([
        channel_name,
        format_million(aov),
        format_percent_arrow(mom_pct, "MoM", mom_val, format_million),
        format_percent_arrow(yoy_pct, "YoY", yoy_val, format_million)
    ])

# ==== 6. Professional Table Drawing Function ====
def draw_professional_sales_table(ax, data, cols, title, color, fontsize=16):
    ax.axis('off')
    
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
            cell_text = cell.get_text().get_text() if col == 0 else ""
            
            # Alternative row colors with special highlighting
            if data_row_idx == 0:  # Total row
                base_color = '#E8F4F8'  # Light blue for Total
            elif "★★" in cell_text:  # Top performer row
                base_color = '#D4F4DD'  # Light green for top performer
            elif data_row_idx % 2 == 0:
                base_color = '#F8F9FA'  # Light gray for even rows
            else:
                base_color = 'white'    # White for odd rows
            
            cell.set_facecolor(base_color)
            cell.set_height(0.07)
            
            # Text styling based on column and content
            if col == 0:  # Channel name column
                if "★★" in cell_text:
                    # Green color and bold for top performer
                    cell.set_text_props(color='#28A745', fontweight='bold', fontsize=fontsize)
                elif data_row_idx == 0:  # Total row
                    cell.set_text_props(color='black', fontweight='bold', fontsize=fontsize)
                else:
                    cell.set_text_props(color='black', fontsize=fontsize)
            elif col >= 2:  # Growth/comparison columns
                cell_text = cell.get_text().get_text()
                if "↗" in cell_text:
                    # Green for positive growth
                    cell.set_text_props(color='#28A745', fontweight='bold', fontsize=fontsize)
                elif "↘" in cell_text:
                    # Red for negative growth
                    cell.set_text_props(color='#DC3545', fontweight='bold', fontsize=fontsize)
                else:
                    cell.set_text_props(color='black', fontsize=fontsize)
            else:
                # Main value columns
                if data_row_idx == 0:  # Total row
                    cell.set_text_props(color='black', fontweight='bold', fontsize=fontsize)
                else:
                    cell.set_text_props(color='black', fontsize=fontsize)

# ==== 7. Function to create single table image ====
def create_sales_table_image(data, cols, title, color, filename):
    fig, ax = plt.subplots(1, 1, figsize=(14, 8))  # Smaller height for 4 rows
    
    draw_professional_sales_table(ax, data, cols, title, color)
    
    # Điều chỉnh layout để tránh overlap
    plt.subplots_adjust(top=0.90, bottom=0.10)
    plt.tight_layout()
    plt.savefig(filename, bbox_inches="tight", dpi=300, facecolor='white')
    plt.close()
    print(f"✅ Ảnh đã lưu: {filename}")

def upload_and_send_sales_image(filename, description):
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

# ==== 8. Tạo và gửi 4 ảnh riêng biệt ====

# Ảnh 1: Chi tiết theo ngày
create_sales_table_image(daily_data, 
          ["Kênh", "SO", "Số lượng đơn", "AOV"], 
          f"Chi tiết theo ngày {yesterday}", "#FFD966",
          "sales_daily.png")

# Ảnh 2: Sales Order MTD
create_sales_table_image(mtd_so, 
          ["Kênh", "SO", "MoM", "YoY"], 
          f"Sales Order MTD ({yesterday})", "#A9D08E",
          "sales_so_mtd.png")

# Ảnh 3: Số lượng đơn MTD
create_sales_table_image(mtd_no, 
          ["Kênh", "Số lượng đơn", "MoM", "YoY"], 
          f"Số lượng đơn MTD ({yesterday})", "#9DC3E6",
          "sales_qty_mtd.png")

# Ảnh 4: AOV MTD
create_sales_table_image(mtd_aov, 
          ["Kênh", "AOV", "MoM", "YoY"], 
          f"AOV MTD ({yesterday})", "#FFB3BA",
          "sales_aov_mtd.png")

# Gửi 4 ảnh về USER_ID
upload_and_send_sales_image("sales_daily.png", "chi tiết ngày")
upload_and_send_sales_image("sales_so_mtd.png", "sales order MTD")
upload_and_send_sales_image("sales_qty_mtd.png", "số lượng đơn MTD")
upload_and_send_sales_image("sales_aov_mtd.png", "AOV MTD")

print("🎉 HOÀN THÀNH! Đã gửi 4 ảnh báo cáo sales về Lark cá nhân của bạn!")
print("📱 1. Chi tiết theo ngày")
print("📱 2. Sales Order MTD") 
print("📱 3. Số lượng đơn MTD")
print("📱 4. AOV MTD")
print("🔔 Check Lark để xem kết quả!")

#sales metric