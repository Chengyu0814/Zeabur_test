import pandas as pd
import re
import sys

MONTH_NAMES = {
    "01": "一月", "02": "二月", "03": "三月", "04": "四月",
    "05": "五月", "06": "六月", "07": "七月", "08": "八月",
    "09": "九月", "10": "十月", "11": "十一月", "12": "十二月"
}

filename = sys.argv[1] if len(sys.argv) > 1 else "20260301-20260327_sales_details.xlsx"

match = re.search(r"20\d{2}(\d{2})\d{2}", filename)
if not match:
    raise ValueError(f"無法從檔名 '{filename}' 中偵測到月份，請確認格式為 20YYMMDD")
month_str = match.group(1)
month_name = MONTH_NAMES.get(month_str)
if not month_name:
    raise ValueError(f"無效的月份：{month_str}")

df = pd.read_excel(filename, dtype={"SKU no": str})

df = df[["SKU no","SKU title","Volume","Amount"]]
df['SKU no'] = df['SKU no'].str.zfill(5)
df["SKU no"] = df["SKU no"].astype(str).replace(" ","")
df_grouped = df.groupby("SKU no").agg({
    "Volume": "sum",
    "Amount": "sum"
}).reset_index()

df_grouped = df_grouped.rename(columns={
    "SKU no": "SKU No.",
    "Volume": f"{month_name}銷售量",
    "Amount": f"{month_name}銷售額"
})
df_grouped["SKU No."] = df_grouped["SKU No."].astype(str).str.strip()

df.rename(columns={"SKU no": "SKU No.","SKU title":"品名"}, inplace=True)
df["SKU No."] = df["SKU No."].astype(str).str.strip()
orders_unique = df.drop_duplicates(subset=['SKU No.'], keep='first')
df_res = df_grouped.merge(orders_unique, on='SKU No.', how='left')
df_res = df_res[["SKU No.","品名",f"{month_name}銷售量",f"{month_name}銷售額"]]


## 在途庫存
df_in_org = pd.read_excel("採購未交量-0326.xlsx",header=3, dtype={"品    號": str})
df_in = df_in_org[["品    號","品   名","未交數量","交貨庫"]].rename(columns={
    "品    號": "SKU No.",
    "品   名": "品名",
    "未交數量": "在途庫存"
})
df_in['SKU No.'] = df_in['SKU No.'].str.zfill(5)
df_in = df_in.dropna(subset=["SKU No."])
df_in = df_in[~df_in["SKU No."].astype(str).str.contains(r'[\u4e00-\u9fff]', regex=True)] # 刪除包含中文字的資料列
df_in = df_in[df_in["交貨庫"] == "華膳-IT"]
df_in = df_in[df_in["SKU No."].astype(str).str.endswith('A', na=False)]
df_in["SKU No."] = df_in["SKU No."].str.extract(r'(.{5})A$', expand=False)
df_in["SKU No."] = df_in["SKU No."].astype(str).str.strip() 
df_in_res = df_in.groupby("SKU No.", as_index=False).agg({
    "品名": "first",     # 保存第一筆遇到的品名
    "在途庫存": "sum"    # 加總庫存
})