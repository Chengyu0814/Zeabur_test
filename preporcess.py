import pandas as pd
import re
import sys


## 月銷售量/額
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

## 機上量
def process_excel(filename, multiplier):
    df_org = pd.read_excel(filename, header=1, dtype={"SKU No.": str})
    df_org = df_org.dropna(subset=['SKU No.']).copy()
    df_org["SKU No."] = df_org["SKU No."].astype(str).str.strip()
    df_org['SKU No.'] = df_org['SKU No.'].str.zfill(5)
    
    df_calc = df_org[["SKU No.", "數量"]].copy()
    df_calc["裝載數量"] = df_calc["數量"] * multiplier
    df_result = df_calc.groupby("SKU No.")["裝載數量"].sum().reset_index()
    
    df_unique = df_org[["SKU No.", "DESCRIPTION"]].drop_duplicates(subset=['SKU No.'])
    return df_result, df_unique

df_avg_result, orders_unique = process_excel("DFS一般航線裝載表20260401.xlsx", 41)
df_fly_result, df_fly_unique = process_excel("DFS串飛航線裝載表20260401.xlsx", 10)
df_merged = pd.merge(df_avg_result, df_fly_result, on="SKU No.", how="outer", suffixes=('_avg', '_fly'))
df_merged["機上量"] = df_merged["裝載數量_avg"] + df_merged["裝載數量_fly"]
df_merged = df_merged[["SKU No.", "機上量"]]
df_merged = df_merged.merge(orders_unique, on='SKU No.', how='left')
df_merged = df_merged.merge(df_fly_unique, on='SKU No.', how='left', suffixes=('', '_fly'))
df_merged["品名"] = df_merged["DESCRIPTION"].fillna(df_merged["DESCRIPTION_fly"])
df_merged = df_merged[["SKU No.", "機上量", "品名"]]


## 期末存量
df_stock = pd.read_excel("IT Inventory(飛買家)-DFS-20260228.xls",header=5, dtype={"料號": str})
df_stock = df_stock[["料號","期末存量","品名"]]
df_stock.rename(columns={"料號":"SKU No."},inplace=True)
df_stock['SKU No.'] = df_stock['SKU No.'].str.zfill(5)
df_stock["SKU No."] = df_stock["SKU No."].astype(str).str.strip()

first_empty_index = df_stock[df_stock['SKU No.'].isnull()].index.min()
if pd.notna(first_empty_index): # 清理資料 利用「空白列」截斷後面的所有資料
    df_stock = df_stock.iloc[:first_empty_index]
else:
    df_stock = df_stock

## 本月進貨
df_imp = pd.read_excel("進貨明細表202603_0326.xlsx",header=3, dtype={"品號": str})
df_imp = df_imp[["品號","品名","驗收數量","庫別"]]
df_imp = df_imp.rename(columns={"品號": "SKU No.","驗收數量":"本月進貨"})
df_imp['SKU No.'] = df_imp['SKU No.'].str.zfill(5)
df_imp = df_imp[df_imp["庫別"] == "華膳-IT"]
df_imp = df_imp[df_imp["SKU No."].astype(str).str.endswith('A', na=False)]
df_imp["SKU No."] = df_imp["SKU No."].str.extract(r'(.{5})A$', expand=False)
df_imp = df_imp[["SKU No.","品名","本月進貨"]].reset_index(drop=True)

df_imp_gop = df_imp.groupby("SKU No.")["本月進貨"].sum().reset_index() #如果有一樣的就相加
df_imp_gop["SKU No."] = df_imp_gop["SKU No."].astype(str).str.strip()

df_imp["SKU No."] = df_imp["SKU No."].astype(str).str.strip()
orders_unique = df_imp.drop_duplicates(subset=['SKU No.'], keep='first')
df_imp_res = df_imp_gop.merge(orders_unique, on='SKU No.', how='left')
df_imp_res = df_imp_res[["SKU No.","品名","本月進貨_x"]]
df_imp_res.rename(columns={"本月進貨_x":"本月進貨"},inplace=True)


### 資料整合、計算
import pandas as pd 
import numpy as np
import math

df_org = pd.read_excel("sub_table/TTW 採購大表_20260304 的副本.xlsx",header=2)
df_row = pd.read_excel("TTW sales summary_test.xlsx")
df_row["SKU No."] = df_row["SKU No."].astype(str).str.strip()


df_org_subset = df_org[["SKU No.","DESCRIPTION", "規劃性下架","成箱規定","lead time"]] #把規劃性下架、成箱規定、lead time 拿下來
df_org_subset["SKU No."] = df_org_subset["SKU No."].astype(str).str.strip()

df_row = pd.merge(df_row, df_org_subset, on = "SKU No.", how="left") # 合併後把品名丟掉
df_row["品名"] = df_row["品名"].where(df_row["品名"].notna() & (df_row["品名"].str.strip() != ""), df_row["DESCRIPTION"])
df_row = df_row.drop(columns=["DESCRIPTION"])

df_row["成箱規定"] = df_row["成箱規定"].fillna(5)

## 新增平均月銷量跟本月銷貨(X月)
sales_cols = df_row.filter(like="月銷售量").columns
df_row["平均月銷量"] = df_row[sales_cols].mean(axis=1).round(0)
last_month_col = sales_cols[-1]
month_name = last_month_col.replace("銷售量", "")
df_row[f"本月銷貨({month_name})"] = df_row[last_month_col]

# 重新排列順序
revenue_cols = df_row.filter(like="月銷售額").columns.tolist()
sales_cols_list = sales_cols.tolist()
new_col_order = ["SKU No.","品名","規劃性下架","成箱規定","TWD成本","lead time","機上量"] + sales_cols_list + revenue_cols + ["平均月銷量","期末存量",f"本月銷貨({month_name})","本月進貨","在途庫存"]
new_col_order = [col for col in new_col_order if col in df_row.columns]
df_row = df_row[new_col_order]


# 以下開始延伸欄位
df_row["需求量(1.5)"] = (df_row["機上量"].fillna(0) + np.maximum(df_row["平均月銷量"].fillna(0), df_row[f"本月銷貨({month_name})"].fillna(0)) * 1.5).round(0)
def calculate_replenishment_1_5(row):
    delisting = row.get("規劃性下架")
    if pd.notna(delisting) and str(delisting).strip() not in ["", "0", "False"]:
        return 0

    def get_num(key):
        val = row.get(key, 0)
        return 0 if pd.isna(val) or val == "" else float(val)

    demand = get_num("需求量(1.5)")
    final_inv = get_num("期末存量")
    sales = get_num(f"本月銷貨({month_name})")
    restock_this_month = get_num("本月進貨")
    in_transit = get_num("在途庫存")

    # 公式：x = MAX(需求量1.5 - (期末存量 - 本月銷貨(X月)) - 本月進貨 - 在途庫存, 0)
    x = max(demand - (final_inv - sales) - restock_this_month - in_transit, 0)

    # --- 步驟 3：處理「成箱規定」(對應 Excel 的 CEILING) ---
    box_rule = row.get("成箱規定")
    
    # 如果沒有成箱規定 (null, 空白, 或 0)，直接輸出 x
    if pd.isna(box_rule) or str(box_rule).strip() == "" or float(box_rule) == 0:
        return x
    else:
        # 如果有成箱規定，計算倍數並無條件進位
        # 例如: x = 4, box_rule = 6 -> math.ceil(4 / 6) = 1 -> 1 * 6 = 6
        # 例如: x = 13, box_rule = 6 -> math.ceil(13 / 6) = 3 -> 3 * 6 = 18
        box_rule = float(box_rule)
        return math.ceil(x / box_rule) * box_rule
df_row["補貨量(以1.5個月)"] = (df_row.apply(calculate_replenishment_1_5, axis=1)).round(0)
df_row["採購金額(1.5)"] = (df_row["補貨量(以1.5個月)"] * df_row["TWD成本"]).round(0)



df_row["需求量_lead_time"] = (df_row["機上量"].fillna(0) + np.maximum(df_row["平均月銷量"].fillna(0), df_row[f"本月銷貨({month_name})"].fillna(0)) * df_row["lead time"].fillna(0)).round(0)
def calculate_replenishment_lead_time(row):
    delisting = row.get("規劃性下架")
    if pd.notna(delisting) and str(delisting).strip() not in ["", "0", "False"]:
        return 0

    def get_num(key):
        val = row.get(key, 0)
        return 0 if pd.isna(val) or val == "" else float(val)

    demand = get_num("需求量_lead_time")
    final_inv = get_num("期末存量")
    sales = get_num(f"本月銷貨({month_name})")
    restock_this_month = get_num("本月進貨")
    in_transit = get_num("在途庫存")

    # 公式：x = MAX(需求量_lead_time - (期末存量 - 本月銷貨(X月)) - 本月進貨 - 在途庫存, 0)
    x = max(demand - (final_inv - sales) - restock_this_month - in_transit, 0)

    # --- 步驟 3：處理「成箱規定」(對應 Excel 的 CEILING) ---
    box_rule = row.get("成箱規定")
    
    # 如果沒有成箱規定 (null, 空白, 或 0)，直接輸出 x
    if pd.isna(box_rule) or str(box_rule).strip() == "" or float(box_rule) == 0:
        return x
    else:
        # 如果有成箱規定，計算倍數並無條件進位
        # 例如: x = 4, box_rule = 6 -> math.ceil(4 / 6) = 1 -> 1 * 6 = 6
        # 例如: x = 13, box_rule = 6 -> math.ceil(13 / 6) = 3 -> 3 * 6 = 18
        box_rule = float(box_rule)
        return math.ceil(x / box_rule) * box_rule
df_row["補貨量(以lead time)"] = (df_row.apply(calculate_replenishment_lead_time, axis=1)).round(0)
df_row["採購金額(lead time)"] = (df_row["補貨量(以lead time)"] * df_row["TWD成本"]).round(0)
df_row["追加數量"] = (df_row["補貨量(以lead time)"]-df_row["補貨量(以1.5個月)"]).round(0)

df_row = df_row[~df_row["SKU No."].astype(str).str.contains("B")]
file_name = f"虎航庫存計算表({month_name}).xlsx"

# 建立 Excel 檔案
with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
    df_row.to_excel(writer, index=False, sheet_name='Main')
    workbook  = writer.book
    worksheet = writer.sheets['Main']

    # 定義你的說明文字
    descriptions = {
        "機上量":"DFS一般航線裝載表20260401 跟 DFS串飛航線裝載表20260401",
        "平均月銷量":"所有月份的銷售量平均",
        "本月銷貨(3月)":"最新月份的銷售量",
        "需求量(1.5)":"機上量+MAX(平均月銷量,本月銷貨)*1.5",
        "補貨量(以1.5個月)":"MAX(需求量-(期末存量-本月銷貨)-本月進貨-在途庫存,0) ，且有考慮成箱規定與規劃性下架",
        "採購金額(1.5)":"補貨量(以1.5個月)*TWD成本",
        "需求量_lead_time":"機上量+MAX(平均月銷量,本月銷貨)*lead time",
        "補貨量(以lead time)":"MAX(需求量_lead_time-(期末存量-本月銷貨)-本月進貨-在途庫存,0) ，且有考慮成箱規定與規劃性下架",
        "採購金額(lead time)":"補貨量(以lead time)*TWD成本",
        "追加數量":"補貨量(以lead time)-補貨量(以1.5個月)"
    }

    for col_num, col_name in enumerate(df_row.columns):
        if col_name in descriptions:
            worksheet.write_comment(0, col_num, descriptions[col_name])

