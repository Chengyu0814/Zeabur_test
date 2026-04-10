import io
import json
import math
import zipfile
from urllib.parse import quote
from typing import List, Optional
from functools import reduce

import numpy as np
import pandas as pd
from fastapi import FastAPI, Form, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

app = FastAPI(title="Excel Processor API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def health_check():
    return {"status": "ok", "message": "Excel Processor API is running 🚀"}


MONTH_NAMES = {
    "01": "一月", "02": "二月", "03": "三月", "04": "四月",
    "05": "五月", "06": "六月", "07": "七月", "08": "八月",
    "09": "九月", "10": "十月", "11": "十一月", "12": "十二月"
}

MONTH_ORDER = [
    "一月", "二月", "三月", "四月", "五月", "六月",
    "七月", "八月", "九月", "十月", "十一月", "十二月"
]

MONTH_NUM = {v: k for k, v in MONTH_NAMES.items()}

CAL_SALE_SHEETS      = ['TPEKNCP', 'TSAKNCI', 'KHHKNCI', 'RMQKNCI']
CAL_INVENTORY_SHEETS = ['TPEKSCI', 'TPEKNCP', 'TSAKNCI', 'KHHKNCI', 'RMQKNCI']

CAL_AIRPORT_CONFIGS = [
    {'prefix': 'tpe', 'knci': 'TPEKNCP', 'extra_stock': 'TPEKSCI'},
    {'prefix': 'tsa', 'knci': 'TSAKNCI'},
    {'prefix': 'khh', 'knci': 'KHHKNCI'},
    {'prefix': 'rmq', 'knci': 'RMQKNCI'},
]


def process_cal_sheets(file_bytes: bytes, sheet_list: list, source_col: str, filename: str) -> pd.DataFrame:
    """讀取指定 sheets，各取 PART_NO + source_col，合併並加總"""
    dfs = {}
    for sheet in sheet_list:
        try:
            df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet, engine='openpyxl')
            dfs[sheet] = df[['PART_NO', source_col]].rename(columns={source_col: f'{sheet}_QTY'})
        except KeyError as e:
            raise HTTPException(status_code=400, detail=f"'{filename}' 的 sheet '{sheet}' 缺少欄位：{str(e)}")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"讀取 '{filename}' 的 sheet '{sheet}' 失敗：{str(e)}")

    df_merged = dfs[sheet_list[0]]
    for sheet in sheet_list[1:]:
        df_merged = df_merged.merge(dfs[sheet], on='PART_NO', how='outer')

    qty_cols = [f'{sheet}_QTY' for sheet in sheet_list]
    df_merged[qty_cols] = df_merged[qty_cols].fillna(0).astype(int)
    df_merged['Total'] = df_merged[qty_cols].sum(axis=1)
    return df_merged


async def process_cal_cost(file: UploadFile, exchange_rates: dict) -> pd.DataFrame:
    """處理華航品號價格資料，回傳 PART_NO + 品名 + TWD成本 的 DataFrame"""
    contents = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents), header=2, dtype={"品號": str})
        df = df[["品號", "品名", "幣別名稱", "採購單價", "核價日"]]
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"商品成本檔案缺少必要欄位：{str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"讀取商品成本檔案失敗: {str(e)}")

    df["品號"] = df["品號"].astype(str).str.strip()
    df = df[df["品號"].str.endswith("A", na=False)]
    df["品號"] = df["品號"].str.extract(r'(.{5})A$', expand=False)

    exchange_rates["台幣"] = 1.0
    df["核價日"] = pd.to_datetime(df["核價日"])
    df = df.sort_values(by=["品號", "核價日"], ascending=[True, True])
    df = df.drop_duplicates(subset=["品號"], keep="last")

    df["TWD成本"] = (df["幣別名稱"].map(exchange_rates) * df["採購單價"]).round(0)
    df = df.reset_index(drop=True)
    df.rename(columns={"品號": "PART_NO"}, inplace=True)
    return df[["PART_NO", "品名", "TWD成本"]]


async def process_cal_inventory(file: UploadFile) -> pd.DataFrame:
    """處理華航採購未交量，回傳 PART_NO + 在途庫存 的 DataFrame（篩選 交貨庫=華膳-CI）"""
    contents = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents), header=3, dtype={"品    號": str})
        df = df[["品    號", "未交數量", "交貨庫"]].rename(columns={
            "品    號": "PART_NO",
            "未交數量": "在途庫存"
        })
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"在途庫存檔案缺少必要欄位：{str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"讀取在途庫存檔案失敗: {str(e)}")

    df["PART_NO"] = df["PART_NO"].astype(str).str.zfill(5)
    df = df.dropna(subset=["PART_NO"])
    df = df[~df["PART_NO"].str.contains(r'[\u4e00-\u9fff]', regex=True)]
    df = df[df["交貨庫"] == "華膳-CI"]
    df = df[df["PART_NO"].str.endswith("A", na=False)]
    df["PART_NO"] = df["PART_NO"].str.extract(r'(.{5})A$', expand=False).astype(str).str.strip()

    df_res = df.groupby("PART_NO", as_index=False)["在途庫存"].sum()
    return df_res


async def process_cal_loading(file: UploadFile) -> pd.DataFrame:
    """處理新版裝載表，回傳 CI Code + 裝載數量 + Q2 + SKU 的 DataFrame"""
    contents = await file.read()
    buf = io.BytesIO(contents)
    try:
        df = pd.read_excel(buf, engine='openpyxl')
        buf.seek(0)
        df_new = pd.read_excel(buf, sheet_name="Q2 新品(上機)", engine='openpyxl')["CI Code"]
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"讀取新版裝載表失敗：{str(e)}")

    df = df[df["Q2"].isin(["A", "B", "B7"])].copy()
    df = df[~df["CI Code"].isin(df_new)].copy()
    return df[["CI Code", "裝載數量", "Q2", "SKU"]].reset_index(drop=True)


async def process_cal_procurement(file: UploadFile) -> pd.DataFrame:
    """處理華航採購大表 (CAL sheet)，回傳 CI CODE + TTB CODE + 規劃性下架 + 成箱規定"""
    contents = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents), sheet_name="CAL", header=2, engine='openpyxl')
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"讀取採購大表失敗：{str(e)}")

    df.columns = [str(col).replace('\n', '') for col in df.columns]
    df["成箱規定"] = df["成箱規定"].fillna(5)
    df["TTB CODE"] = df["TTB CODE"].astype(str).str.strip()
    df["CI CODE"] = df["CI CODE"].astype(str).str.strip()
    return df[["CI CODE", "TTB CODE", "規劃性下架", "成箱規定"]].reset_index(drop=True)


def calc_single_airport(month_dfs, present_months_sorted, prefix, knci, df_proc_airport, extra_stock_knci=None):
    """計算單一機場的補貨報表"""
    sales_col = f'{knci}_銷售'
    stock_col = f'{knci}_庫存'
    extra_stock_col = f'{extra_stock_knci}_庫存' if extra_stock_knci else None

    load_qty_col = f'{prefix}_裝載量'
    load_set_col = f'{prefix}_裝載套數'
    load_req_col = f'{prefix}_裝載需求量'
    monthly_req_col = f'{prefix}_月銷需求量'
    extra_req_col = f'{prefix}_追加需求量'
    move_req_col = f'{prefix}_待移倉量'

    n_months = len(month_dfs)

    # 合併各月銷售量
    df_merged = month_dfs[0][['PART_NO', sales_col]].copy()
    df_merged = df_merged.rename(columns={sales_col: f'{sales_col}_{present_months_sorted[0]}'})

    for i in range(1, n_months):
        is_last = (i == n_months - 1)
        if is_last:
            cols = ['PART_NO', sales_col, stock_col]
            if extra_stock_col and extra_stock_col in month_dfs[i].columns:
                cols.append(extra_stock_col)
        else:
            cols = ['PART_NO', sales_col]
        df_m = month_dfs[i][cols].copy()
        df_m = df_m.rename(columns={sales_col: f'{sales_col}_{present_months_sorted[i]}'})
        df_merged = df_merged.merge(df_m, on='PART_NO', how='outer')

    # 單月：需從該月取庫存
    if n_months == 1:
        cols = ['PART_NO', stock_col]
        if extra_stock_col and extra_stock_col in month_dfs[0].columns:
            cols.append(extra_stock_col)
        df_merged = df_merged.merge(month_dfs[0][cols].copy(), on='PART_NO', how='outer')

    # TPE 特殊：TPEKSCI + TPEKNCP 庫存合併
    if extra_stock_col and extra_stock_col in df_merged.columns:
        df_merged[stock_col] = df_merged[stock_col].fillna(0) + df_merged[extra_stock_col].fillna(0)
        df_merged.drop(columns=[extra_stock_col], inplace=True)

    # 合併機場採購/裝載資料
    df_cal = df_merged.merge(
        df_proc_airport, left_on='PART_NO', right_on='CI CODE', how='left'
    ).drop(columns=['CI CODE'], errors='ignore')
    df_cal = df_cal.fillna(0)

    # 計算
    df_cal[load_req_col] = df_cal[load_qty_col] * df_cal[load_set_col]

    sales_month_cols = [f'{sales_col}_{m}' for m in present_months_sorted]
    df_cal[monthly_req_col] = df_cal[sales_month_cols].mean(axis=1).round(0).astype('Int64')
    df_cal[extra_req_col] = (df_cal[monthly_req_col] * 1.5).astype('Int64')

    def calc_move_req(row):
        delisting = row.get('規劃性下架', 0)
        if pd.notna(delisting) and str(delisting).strip() not in ('', '0', '0.0'):
            return 0
        x = max(row[load_req_col] + row[monthly_req_col] + row[extra_req_col] - row[stock_col], 0)
        pack = row.get('成箱規定', 0)
        if pd.notna(pack) and float(pack) != 0:
            return math.ceil(x / float(pack)) * float(pack)
        return x

    df_cal[move_req_col] = df_cal.apply(calc_move_req, axis=1).astype('Int64')
    return df_cal


def calc_full_airport_report(month_dfs, present_months_sorted, df_loading, df_procurement, loading_sets, df_inv_data=None, df_cost=None):
    """計算華航四倉完整補貨報表"""

    # 篩選採購大表：只保留裝載表有的品項
    df_proc = df_procurement[df_procurement["CI CODE"].isin(df_loading["CI Code"])].copy().reset_index(drop=True)

    # 在途庫存 mapping by TTB CODE
    if df_inv_data is not None:
        inv_map = df_inv_data.set_index('PART_NO')['在途庫存']
        df_proc['在途庫存'] = df_proc['TTB CODE'].map(inv_map).fillna(0).astype(int)

    # 成本 mapping by TTB CODE
    if df_cost is not None:
        cost_map = df_cost.set_index('PART_NO')['TWD成本']
        df_proc['成本(TWD)'] = df_proc['TTB CODE'].map(cost_map)

    # 為每個機場建立裝載資料
    qty_map = df_loading.set_index('CI Code')['裝載數量']
    q2_map = df_loading.set_index('CI Code')['Q2']

    airport_proc = {}
    for config in CAL_AIRPORT_CONFIGS:
        prefix = config['prefix']
        set_map = loading_sets.get(prefix, {})
        df_ap = df_proc[['CI CODE', 'TTB CODE', '規劃性下架', '成箱規定']].copy()
        df_ap[f'{prefix}_裝載量'] = df_ap['CI CODE'].map(qty_map).fillna(0)
        df_ap[f'{prefix}_裝載套數'] = df_ap['CI CODE'].map(q2_map).map(set_map).fillna(0)
        if '在途庫存' in df_proc.columns:
            df_ap[f'{prefix}_在途庫存'] = df_proc['在途庫存'].values
        if '成本(TWD)' in df_proc.columns:
            df_ap['成本(TWD)'] = df_proc['成本(TWD)'].values
        airport_proc[prefix] = df_ap

    # 計算各機場
    airport_results = {}
    for config in CAL_AIRPORT_CONFIGS:
        prefix = config['prefix']
        airport_results[prefix] = calc_single_airport(
            month_dfs, present_months_sorted,
            prefix, config['knci'], airport_proc[prefix],
            extra_stock_knci=config.get('extra_stock')
        )

    # TPE 特殊計算
    df_tpe = airport_results['tpe']
    df_tpe.drop(columns=['tpe_追加需求量', 'tpe_待移倉量'], inplace=True)

    # 移出量 = 其他三倉的待移倉量加總（用 PART_NO merge 確保對齊）
    move_data = airport_results['tsa'][['PART_NO', 'tsa_待移倉量']].merge(
        airport_results['khh'][['PART_NO', 'khh_待移倉量']], on='PART_NO', how='outer'
    ).merge(
        airport_results['rmq'][['PART_NO', 'rmq_待移倉量']], on='PART_NO', how='outer'
    )
    move_data['tpe_移出量'] = (
        move_data['tsa_待移倉量'].fillna(0) + move_data['khh_待移倉量'].fillna(0) + move_data['rmq_待移倉量'].fillna(0)
    ).astype('Int64')
    df_tpe = df_tpe.merge(move_data[['PART_NO', 'tpe_移出量']], on='PART_NO', how='left')
    df_tpe['tpe_移出量'] = df_tpe['tpe_移出量'].fillna(0).astype('Int64')

    stock_col_tpe = 'TPEKNCP_庫存'

    def calc_purchase(row):
        delisting = row.get('規劃性下架', 0)
        if pd.notna(delisting) and str(delisting).strip() not in ('', '0', '0.0'):
            return 0
        transit = row.get('tpe_在途庫存', 0)
        if pd.isna(transit):
            transit = 0
        x = max(row['tpe_裝載需求量'] + row['tpe_月銷需求量'] + row['tpe_移出量'] - float(transit) - row[stock_col_tpe], 0)
        pack = row.get('成箱規定', 0)
        if pd.notna(pack) and float(pack) != 0:
            return math.ceil(x / float(pack)) * float(pack)
        return x

    df_tpe['tpe_請購量'] = df_tpe.apply(calc_purchase, axis=1).astype('Int64')
    df_tpe['tpe_請購金額'] = (df_tpe['tpe_請購量'] * df_tpe.get('成本(TWD)', pd.Series(dtype=float)).fillna(0)).round(0)
    airport_results['tpe'] = df_tpe

    # 整理 TPE 欄位順序
    knci_tpe = 'TPEKNCP'
    sales_cols_tpe = [f'{knci_tpe}_銷售_{m}' for m in present_months_sorted]
    tpe_ordered = (['PART_NO', 'TTB CODE', '規劃性下架', '成箱規定', '成本(TWD)']
                   + sales_cols_tpe
                   + [f'{knci_tpe}_庫存', 'tpe_裝載量', 'tpe_裝載套數', 'tpe_裝載需求量',
                      'tpe_月銷需求量', 'tpe_在途庫存', 'tpe_移出量', 'tpe_請購量', 'tpe_請購金額'])
    tpe_ordered = [c for c in tpe_ordered if c in df_tpe.columns]
    df_tpe_final = df_tpe[tpe_ordered]

    # 其他機場：去掉共用欄位
    dfs_other = []
    for prefix in ['tsa', 'khh', 'rmq']:
        config = next(c for c in CAL_AIRPORT_CONFIGS if c['prefix'] == prefix)
        knci = config['knci']
        df_ap = airport_results[prefix].copy()
        drop_cols = [c for c in ['TTB CODE', '規劃性下架', '成箱規定', '成本(TWD)', f'{prefix}_在途庫存'] if c in df_ap.columns]
        df_ap.drop(columns=drop_cols, inplace=True)
        dfs_other.append(df_ap)

    # 合併所有機場
    df_final = df_tpe_final
    for df_ap in dfs_other:
        df_final = df_final.merge(df_ap, on='PART_NO', how='left')

    # 只保留裝載表有的品項
    df_final = df_final[df_final['PART_NO'].isin(df_loading['CI Code'])].copy()

    # 加入 SKU 商品名稱
    sku_map = df_loading.drop_duplicates(subset='CI Code').set_index('CI Code')['SKU']
    df_final.insert(2, 'SKU', df_final['PART_NO'].map(sku_map))

    df_final = df_final.sort_values('PART_NO').reset_index(drop=True)
    return df_final


def build_cal_multi_index(columns, present_months_sorted):
    """將平面欄位轉為 MultiIndex (機場, 指標)"""
    COMMON = {'PART_NO', 'TTB CODE', 'SKU', '規劃性下架', '成箱規定', '成本(TWD)'}
    tuples = []
    for col in columns:
        if col in COMMON:
            tuples.append(('', col))
            continue

        if 'TPE' in col or col.startswith('tpe_'):   top = 'TPE'
        elif 'TSA' in col or col.startswith('tsa_'): top = 'TSA'
        elif 'KHH' in col or col.startswith('khh_'): top = 'KHH'
        elif 'RMQ' in col or col.startswith('rmq_'): top = 'RMQ'
        else:
            tuples.append(('', col))
            continue

        matched = False
        for month in present_months_sorted:
            if col.endswith(f'_{month}'):
                tuples.append((top, f'{month}銷售量'))
                matched = True
                break
        if matched:
            continue

        if '庫存' in col and '在途' not in col:    sub = '庫存數'
        elif '裝載需求量' in col: sub = '裝載需求量'
        elif '裝載套數'  in col: sub = '裝載套數'
        elif '裝載量'   in col: sub = '裝載量'
        elif '月銷需求量' in col: sub = '月銷需求量'
        elif '在途庫存'  in col: sub = '在途庫存'
        elif '追加需求量' in col: sub = '追加需求量'
        elif '待移倉量'  in col: sub = '待移倉量'
        elif '請購金額'  in col: sub = '請購金額'
        elif '請購量'   in col: sub = '請購量'
        elif '移出量'   in col: sub = '移出量'
        else:                    sub = col.split('_')[-1]
        tuples.append((top, sub))

    return pd.MultiIndex.from_tuples(tuples)


async def process_inventory(file: UploadFile) -> pd.DataFrame:
    """處理 採購未交量 檔案，回傳 SKU No. + 品名 + 在途庫存 的 DataFrame"""
    contents = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents), header=3, dtype={"品    號": str})
        df = df[["品    號", "品   名", "未交數量", "交貨庫"]].rename(columns={
            "品    號": "SKU No.",
            "品   名": "品名",
            "未交數量": "在途庫存"
        })
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"在途庫存檔案缺少必要欄位：{str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"讀取在途庫存檔案失敗: {str(e)}")

    df["SKU No."] = df["SKU No."].astype(str).str.zfill(5)
    df = df.dropna(subset=["SKU No."])
    df = df[~df["SKU No."].str.contains(r'[\u4e00-\u9fff]', regex=True)]
    df = df[df["交貨庫"] == "華膳-IT"]
    df = df[df["SKU No."].str.endswith("A", na=False)]
    df["SKU No."] = df["SKU No."].str.extract(r'(.{5})A$', expand=False)
    df["SKU No."] = df["SKU No."].astype(str).str.strip()

    df_res = df.groupby("SKU No.", as_index=False).agg({
        "品名": "first",
        "在途庫存": "sum"
    })
    return df_res[["SKU No.", "品名", "在途庫存"]]


async def process_cost(file: UploadFile, exchange_rates: dict) -> pd.DataFrame:
    """處理 品號價格資料 檔案，回傳 SKU No. + 品名 + TWD成本 的 DataFrame"""
    contents = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents), header=2, dtype={"品號": str})
        df = df[["品號", "品名", "幣別名稱", "採購單價", "核價日"]]
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"商品成本檔案缺少必要欄位：{str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"讀取商品成本檔案失敗: {str(e)}")

    df["品號"] = df["品號"].astype(str).str.strip()
    df = df[df["品號"].str.endswith("A", na=False)]
    df["品號"] = df["品號"].str.extract(r'(.{5})A$', expand=False)

    exchange_rates["台幣"] = 1.0
    df["核價日"] = pd.to_datetime(df["核價日"])
    df = df.sort_values(by=["品號", "核價日"], ascending=[True, True])
    df = df.drop_duplicates(subset=["品號"], keep="last")

    df["TWD成本"] = (df["幣別名稱"].map(exchange_rates) * df["採購單價"]).round(0)
    df = df.reset_index(drop=True)
    df.rename(columns={"品號": "SKU No."}, inplace=True)
    return df[["SKU No.", "品名", "TWD成本"]]


async def process_import(file: UploadFile) -> pd.DataFrame:
    """處理 進貨明細 檔案，回傳 SKU No. + 品名 + 本月進貨 的 DataFrame"""
    contents = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents), header=3, dtype={"品號": str})
        df = df[["品號", "品名", "驗收數量", "庫別"]].rename(columns={"品號": "SKU No.", "驗收數量": "本月進貨"})
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"本月進貨檔案缺少必要欄位：{str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"讀取本月進貨檔案失敗: {str(e)}")

    df["SKU No."] = df["SKU No."].astype(str).str.zfill(5)
    df = df[df["庫別"] == "華膳-IT"]
    df = df[df["SKU No."].str.endswith("A", na=False)]
    df["SKU No."] = df["SKU No."].str.extract(r'(.{5})A$', expand=False)
    df["SKU No."] = df["SKU No."].astype(str).str.strip()

    df_grouped = df.groupby("SKU No.")["本月進貨"].sum().reset_index()
    names = df.drop_duplicates(subset=["SKU No."], keep="first")[["SKU No.", "品名"]]
    df_res = df_grouped.merge(names, on="SKU No.", how="left")
    return df_res[["SKU No.", "品名", "本月進貨"]]


async def process_stock(file: UploadFile) -> pd.DataFrame:
    """處理 期末存量 檔案，回傳 SKU No. + 品名 + 期末存量 的 DataFrame"""
    contents = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents), header=5, dtype={"料號": str})
        df = df[["料號", "品名", "期末存量"]].rename(columns={"料號": "SKU No."})
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"期末存量檔案缺少必要欄位：{str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"讀取期末存量檔案失敗: {str(e)}")

    df["SKU No."] = df["SKU No."].astype(str).str.zfill(5).str.strip()
    first_empty = df[df["SKU No."].isnull()].index.min()
    if pd.notna(first_empty):
        df = df.iloc[:first_empty]
    return df[["SKU No.", "品名", "期末存量"]]


async def process_onboard(normal_file: UploadFile, fly_file: UploadFile) -> pd.DataFrame:
    """處理 機上量 檔案（一般航線 × 41，串飛航線 × 10），回傳 SKU No. + 品名 + 機上量 的 DataFrame"""

    async def read_onboard_file(file: UploadFile, multiplier: int):
        contents = await file.read()
        try:
            df = pd.read_excel(io.BytesIO(contents), header=1, dtype={"SKU No.": str})
            df = df.dropna(subset=["SKU No."]).copy()
            df["SKU No."] = df["SKU No."].astype(str).str.strip().str.zfill(5)
            df_calc = df[["SKU No.", "數量"]].copy()
            df_calc["裝載數量"] = df_calc["數量"] * multiplier
            df_result = df_calc.groupby("SKU No.")["裝載數量"].sum().reset_index()
            df_unique = df[["SKU No.", "DESCRIPTION"]].drop_duplicates(subset=["SKU No."])
        except KeyError as e:
            raise HTTPException(status_code=400, detail=f"機上量檔案 '{file.filename}' 缺少必要欄位：{str(e)}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"讀取機上量檔案 '{file.filename}' 失敗: {str(e)}")
        return df_result, df_unique

    df_normal, normal_unique = await read_onboard_file(normal_file, 41)
    df_fly, fly_unique = await read_onboard_file(fly_file, 10)

    df_merged = pd.merge(df_normal, df_fly, on="SKU No.", how="outer", suffixes=("_normal", "_fly"))
    df_merged["機上量"] = df_merged["裝載數量_normal"].fillna(0) + df_merged["裝載數量_fly"].fillna(0)
    df_merged = df_merged[["SKU No.", "機上量"]]
    df_merged = df_merged.merge(normal_unique, on="SKU No.", how="left")
    df_merged = df_merged.merge(fly_unique, on="SKU No.", how="left", suffixes=("", "_fly"))
    df_merged["品名"] = df_merged["DESCRIPTION"].fillna(df_merged["DESCRIPTION_fly"])
    return df_merged[["SKU No.", "品名", "機上量"]]


async def process_org(file: UploadFile) -> pd.DataFrame:
    """處理 採購大表 檔案，回傳 SKU No. + 規劃性下架 + 成箱規定 + lead time 的 DataFrame"""
    contents = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents), header=2)
        df = df[["SKU No.", "DESCRIPTION", "規劃性下架", "成箱規定", "lead time"]]
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"採購大表缺少必要欄位：{str(e)}（需要：SKU No., DESCRIPTION, 規劃性下架, 成箱規定, lead time）")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"讀取採購大表失敗: {str(e)}")

    df["SKU No."] = df["SKU No."].astype(str).str.strip()
    return df


def calculate_replenishment(row, demand_col, month_col):
    delisting = row.get("規劃性下架")
    if pd.notna(delisting) and str(delisting).strip() not in ["", "0", "False"]:
        return 0

    def get_num(key):
        val = row.get(key, 0)
        return 0 if pd.isna(val) or val == "" else float(val)

    demand = get_num(demand_col)
    final_inv = get_num("期末存量")
    sales = get_num(month_col)
    restock_this_month = get_num("本月進貨")
    in_transit = get_num("在途庫存")

    x = max(demand - (final_inv - sales) - restock_this_month - in_transit, 0)

    box_rule = row.get("成箱規定")
    if pd.isna(box_rule) or str(box_rule).strip() == "" or float(box_rule) == 0:
        return x
    else:
        box_rule = float(box_rule)
        return math.ceil(x / box_rule) * box_rule


def run_replenishment_calculation(merged: pd.DataFrame, df_org: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    """合併採購大表並執行補貨計算，回傳 (結果DataFrame, 月份名稱)"""
    df_org_subset = df_org[["SKU No.", "DESCRIPTION", "規劃性下架", "成箱規定", "lead time"]].copy()
    df_org_subset["SKU No."] = df_org_subset["SKU No."].astype(str).str.strip()

    df_row = pd.merge(merged, df_org_subset, on="SKU No.", how="left")

    # 若 DESCRIPTION 欄存在則補品名（採購大表有時帶 DESCRIPTION）
    if "DESCRIPTION" in df_row.columns:
        df_row["品名"] = df_row["品名"].where(
            df_row["品名"].notna() & (df_row["品名"].str.strip() != ""),
            df_row["DESCRIPTION"]
        )
        df_row = df_row.drop(columns=["DESCRIPTION"])

    df_row["成箱規定"] = df_row["成箱規定"].fillna(5)

    sales_cols = df_row.filter(like="月銷售量").columns
    if len(sales_cols) > 0:
        df_row["平均月銷量"] = df_row[sales_cols].mean(axis=1).round(0)
        last_month_col = sales_cols[-1]
        month_name = last_month_col.replace("銷售量", "")
        df_row[f"本月銷貨({month_name})"] = df_row[last_month_col]
    else:
        df_row["平均月銷量"] = 0
        month_name = ""
        df_row[f"本月銷貨({month_name})"] = 0

    revenue_cols = df_row.filter(like="月銷售額").columns.tolist()
    sales_cols_list = sales_cols.tolist()
    this_month_col = f"本月銷貨({month_name})"
    new_col_order = (
        ["SKU No.", "品名", "規劃性下架", "成箱規定", "TWD成本", "lead time", "機上量"]
        + sales_cols_list + revenue_cols
        + ["平均月銷量", "期末存量", this_month_col, "本月進貨", "在途庫存"]
    )
    new_col_order = [c for c in new_col_order if c in df_row.columns]
    df_row = df_row[new_col_order]

    # 需求量 & 補貨量 (1.5個月)
    df_row["需求量(1.5)"] = (
        df_row["機上量"].fillna(0)
        + np.maximum(df_row["平均月銷量"].fillna(0), df_row[this_month_col].fillna(0)) * 1.5
    ).round(0)
    df_row["補貨量(以1.5個月)"] = df_row.apply(
        lambda r: calculate_replenishment(r, "需求量(1.5)", this_month_col), axis=1
    ).round(0)
    df_row["採購金額(1.5)"] = (df_row["補貨量(以1.5個月)"] * df_row["TWD成本"].fillna(0)).round(0)

    # 需求量 & 補貨量 (lead time)
    df_row["需求量_lead_time"] = (
        df_row["機上量"].fillna(0)
        + np.maximum(df_row["平均月銷量"].fillna(0), df_row[this_month_col].fillna(0))
        * df_row["lead time"].fillna(0)
    ).round(0)
    df_row["補貨量(以lead time)"] = df_row.apply(
        lambda r: calculate_replenishment(r, "需求量_lead_time", this_month_col), axis=1
    ).round(0)
    df_row["採購金額(lead time)"] = (df_row["補貨量(以lead time)"] * df_row["TWD成本"].fillna(0)).round(0)
    df_row["追加數量"] = (df_row["補貨量(以lead time)"] - df_row["補貨量(以1.5個月)"]).round(0)

    df_row = df_row[~df_row["SKU No."].astype(str).str.contains("B")]

    return df_row, month_name


@app.post("/scan-cost-currencies") #處理幣別，匯率轉換
async def scan_cost_currencies(cost_file: UploadFile = File(...)):
    contents = await cost_file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents), header=2, dtype={"品號": str})
        df = df[["品號", "幣別名稱"]]
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"商品成本檔案缺少必要欄位：{str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"讀取商品成本檔案失敗: {str(e)}")

    df["品號"] = df["品號"].astype(str).str.strip()
    df = df[df["品號"].str.endswith("A", na=False)]
    currencies = [c for c in df["幣別名稱"].dropna().unique().tolist() if c != "台幣"]
    return {"currencies": currencies}


@app.post("/process-excel")
async def process_excel(
    files: Optional[List[UploadFile]] = File(default=None),
    months: Optional[List[str]] = Form(default=None),
    inventory_file: Optional[UploadFile] = File(None),
    onboard_normal_file: Optional[UploadFile] = File(None),
    onboard_fly_file: Optional[UploadFile] = File(None),
    stock_file: Optional[UploadFile] = File(None),
    import_file: Optional[UploadFile] = File(None),
    cost_file: Optional[UploadFile] = File(None),
    exchange_rates_json: Optional[str] = Form(None),
    org_file: Optional[UploadFile] = File(None),
):
    has_sales = bool(files and any(f.filename for f in files))
    has_inventory = bool(inventory_file and inventory_file.filename)
    has_onboard_normal = bool(onboard_normal_file and onboard_normal_file.filename)
    has_onboard_fly = bool(onboard_fly_file and onboard_fly_file.filename)
    has_stock = bool(stock_file and stock_file.filename)
    has_import = bool(import_file and import_file.filename)
    has_cost = bool(cost_file and cost_file.filename)

    if has_onboard_normal != has_onboard_fly:
        raise HTTPException(status_code=400, detail="一般航線與串飛航線檔案須同時上傳")

    has_onboard = has_onboard_normal and has_onboard_fly

    if not has_sales and not has_inventory and not has_onboard and not has_stock and not has_import and not has_cost:
        raise HTTPException(status_code=400, detail="請至少上傳一個檔案")

    result = None
    present_months = []

    # ── 處理銷售明細 ──────────────────────────────────────────
    if has_sales:
        if len(files) != len(months or []):
            raise HTTPException(status_code=400, detail="files 與 months 數量不符")

        all_sales = []
        all_names = []

        for file, month_str in zip(files, months):
            if not file.filename.endswith(('.xls', '.xlsx')):
                raise HTTPException(
                    status_code=400,
                    detail=f"檔案 '{file.filename}' 不是有效的 Excel 格式 (.xls 或 .xlsx)"
                )

            month_name = MONTH_NAMES.get(month_str)
            if not month_name:
                raise HTTPException(status_code=400, detail=f"無效的月份：{month_str}")

            contents = await file.read()
            file_stream = io.BytesIO(contents)

            try:
                excel_file = pd.ExcelFile(file_stream)
                sheet_names = excel_file.sheet_names

                if len(sheet_names) == 1:
                    target_sheet = sheet_names[0]
                elif "details" in sheet_names:
                    target_sheet = "details"
                else:
                    raise HTTPException(
                        status_code=400,
                        detail=f"檔案 '{file.filename}' 包含多個工作表，但找不到名為 'details' 的工作表"
                    )

                df = pd.read_excel(excel_file, sheet_name=target_sheet, dtype={"SKU no": str})
                df = df[["SKU no", "SKU title", "Volume", "Amount"]]
            except KeyError as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"檔案 '{file.filename}' 缺少必要欄位：{str(e)}"
                )
            except HTTPException:
                raise
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"讀取 '{file.filename}' 失敗: {str(e)}")

            df['SKU no'] = df['SKU no'].astype(str).str.zfill(5).str.replace(" ", "", regex=False)

            df_grouped = df.groupby("SKU no").agg({"Volume": "sum", "Amount": "sum"}).reset_index()
            df_grouped = df_grouped.rename(columns={
                "SKU no": "SKU No.",
                "Volume": f"{month_name}銷售量",
                "Amount": f"{month_name}銷售額"
            })
            df_grouped["SKU No."] = df_grouped["SKU No."].astype(str).str.strip()

            df.rename(columns={"SKU no": "SKU No.", "SKU title": "品名"}, inplace=True)
            df["SKU No."] = df["SKU No."].astype(str).str.strip()
            names = df.drop_duplicates(subset=["SKU No."], keep="first")[["SKU No.", "品名"]]
            names = names[names["品名"].notna()]

            all_sales.append(df_grouped)
            all_names.append(names)

        result = reduce(lambda l, r: l.merge(r, on="SKU No.", how="outer"), all_sales)
        name_df = pd.concat(all_names).drop_duplicates(subset=["SKU No."], keep="first")
        result = result.merge(name_df, on="SKU No.", how="left")

        present_months = [m for m in MONTH_ORDER if f"{m}銷售量" in result.columns]
        vol_cols = [f"{m}銷售量" for m in present_months]
        amt_cols = [f"{m}銷售額" for m in present_months]
        result = result[["SKU No.", "品名"] + vol_cols + amt_cols]

    # ── 處理在途庫存 ──────────────────────────────────────────
    df_inv = None
    if has_inventory:
        df_inv = await process_inventory(inventory_file)
        if result is None:
            result = df_inv[["SKU No.", "品名", "在途庫存"]]

    # ── 處理機上量 ────────────────────────────────────────────
    df_onboard = None
    if has_onboard:
        df_onboard = await process_onboard(onboard_normal_file, onboard_fly_file)
        if result is None:
            result = df_onboard

    # ── 處理期末存量 ──────────────────────────────────────────
    df_stock = None
    if has_stock:
        df_stock = await process_stock(stock_file)
        if result is None:
            result = df_stock

    # ── 處理本月進貨 ──────────────────────────────────────────
    df_import = None
    if has_import:
        df_import = await process_import(import_file)
        if result is None:
            result = df_import

    # ── 處理商品成本 ──────────────────────────────────────────
    df_cost = None
    if has_cost:
        if not exchange_rates_json:
            raise HTTPException(status_code=400, detail="請提供匯率資料")
        try:
            exchange_rates = json.loads(exchange_rates_json)
        except Exception:
            raise HTTPException(status_code=400, detail="匯率格式錯誤")
        df_cost = await process_cost(cost_file, exchange_rates)
        if result is None:
            result = df_cost

    # ── 合併為單一工作表 ──────────────────────────────────────────
    # 收集各來源品名，優先順序：銷售明細 > 在途庫存 > 機上量 > 期末存量 > 本月進貨 > 商品成本
    name_sources = []
    for df_n in (([result] if has_sales and result is not None else []) +
                 [df_inv, df_onboard, df_stock, df_import, df_cost]):
        if df_n is not None and "品名" in df_n.columns:
            name_sources.append(df_n[["SKU No.", "品名"]].dropna(subset=["品名"]))
    all_names = (pd.concat(name_sources).drop_duplicates(subset=["SKU No."], keep="first")
                 if name_sources else pd.DataFrame(columns=["SKU No.", "品名"]))

    def drop_name(df):
        return df.drop(columns=["品名"], errors="ignore")

    # Outer join: 銷售明細、在途庫存、機上量、期末存量、本月進貨
    base_dfs = []
    if has_sales and result is not None:
        base_dfs.append(drop_name(result))
    if df_inv is not None:
        base_dfs.append(drop_name(df_inv[["SKU No.", "品名", "在途庫存"]]))
    if df_onboard is not None:
        base_dfs.append(drop_name(df_onboard))
    if df_stock is not None:
        base_dfs.append(drop_name(df_stock))
    if df_import is not None:
        base_dfs.append(drop_name(df_import))

    if base_dfs:
        merged = reduce(lambda l, r: l.merge(r, on="SKU No.", how="outer"), base_dfs)
        # Left join: 商品成本
        if df_cost is not None:
            merged = merged.merge(drop_name(df_cost), on="SKU No.", how="left")
    else:
        # 只有商品成本
        merged = drop_name(df_cost) if df_cost is not None else pd.DataFrame(columns=["SKU No."])

    # 加回品名
    merged = merged.merge(all_names, on="SKU No.", how="left")

    # 欄位排序：SKU No., 品名, TWD成本, 機上量, {月}銷售量/額, 期末存量, 本月進貨, 在途庫存
    sales_cols = [c for c in merged.columns if "銷售量" in c or "銷售額" in c]
    ordered = ["SKU No.", "品名", "TWD成本", "機上量"] + sales_cols + ["期末存量", "本月進貨", "在途庫存"]
    final_cols = [c for c in ordered if c in merged.columns]
    merged = merged[final_cols]

    # ── 採購大表：補貨計算 ─────────────────────────────────────
    has_org = bool(org_file and org_file.filename)
    if has_org:
        df_org = await process_org(org_file)
        merged, calc_month = run_replenishment_calculation(merged, df_org)
        if not present_months and calc_month:
            out_month = calc_month
        elif present_months:
            out_month = present_months[-1]
        else:
            out_month = ""
    else:
        out_month = present_months[-1] if present_months else ""

    # ── 輸出 ──────────────────────────────────────────────────
    if has_org:
        out_filename = f"虎航庫存計算表({out_month}).xlsx" if out_month else "虎航庫存計算表.xlsx"
    elif present_months:
        month_nums = [MONTH_NUM[m] for m in present_months]
        out_filename = f"TTW sales summary {month_nums[0]}-{month_nums[-1]}.xlsx"
    else:
        out_filename = "TTW 庫存表.xlsx"

    output_stream = io.BytesIO()
    if has_org:
        with pd.ExcelWriter(output_stream, engine="xlsxwriter") as writer:
            merged.to_excel(writer, sheet_name="Main", index=False)
            worksheet = writer.sheets["Main"]
            descriptions = {
                "機上量": "DFS一般航線裝載表 × 41 + 串飛航線裝載表 × 10",
                "平均月銷量": "所有月份的銷售量平均",
                f"本月銷貨({out_month})": "最新月份的銷售量",
                "需求量(1.5)": "機上量+MAX(平均月銷量,本月銷貨)*1.5",
                "補貨量(以1.5個月)": "MAX(需求量-(期末存量-本月銷貨)-本月進貨-在途庫存,0) ，且有考慮成箱規定與規劃性下架",
                "採購金額(1.5)": "補貨量(以1.5個月)*TWD成本",
                "需求量_lead_time": "機上量+MAX(平均月銷量,本月銷貨)*lead time",
                "補貨量(以lead time)": "MAX(需求量_lead_time-(期末存量-本月銷貨)-本月進貨-在途庫存,0) ，且有考慮成箱規定與規劃性下架",
                "採購金額(lead time)": "補貨量(以lead time)*TWD成本",
                "追加數量": "補貨量(以lead time)-補貨量(以1.5個月)",
            }
            for col_num, col_name in enumerate(merged.columns):
                if col_name in descriptions:
                    worksheet.write_comment(0, col_num, descriptions[col_name])
    else:
        with pd.ExcelWriter(output_stream, engine="openpyxl") as writer:
            merged.to_excel(writer, sheet_name="報表", index=False)

    output_stream.seek(0)

    return StreamingResponse(
        output_stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename*=UTF-8''{quote(out_filename)}",
            "Access-Control-Expose-Headers": "Content-Disposition"
        }
    )


@app.post("/cal/process-excel")
async def cal_process_excel(
    files: Optional[List[UploadFile]] = File(default=None),
    months: Optional[List[str]] = Form(default=None),
    inventory_file: Optional[UploadFile] = File(None),
    cost_file: Optional[UploadFile] = File(None),
    exchange_rates_json: Optional[str] = Form(None),
    loading_file: Optional[UploadFile] = File(None),
    loading_sets_json: Optional[str] = Form(None),
    procurement_file: Optional[UploadFile] = File(None),
):
    files = files or []
    months = months or []

    if len(files) != len(months):
        raise HTTPException(status_code=400, detail="files 與 months 數量不符")

    has_months = len(files) > 0
    has_inventory = inventory_file and inventory_file.filename
    has_cost = bool(cost_file and cost_file.filename)
    has_loading = bool(loading_file and loading_file.filename)
    has_procurement = bool(procurement_file and procurement_file.filename)

    if not has_months and not has_inventory and not has_cost and not has_loading and not has_procurement:
        raise HTTPException(status_code=400, detail="請至少上傳一種檔案")

    if has_procurement and not has_loading:
        raise HTTPException(status_code=400, detail="上傳採購大表時需同時上傳新版裝載表")
    if has_procurement and not has_months:
        raise HTTPException(status_code=400, detail="上傳採購大表時需同時上傳月份資料")

    # === 處理月份資料 ===
    month_dfs = []
    present_months = []

    for file, month_str in zip(files, months):
        month_name = MONTH_NAMES.get(month_str)
        if not month_name:
            raise HTTPException(status_code=400, detail=f"無效的月份：{month_str}")

        contents = await file.read()

        df_inv  = process_cal_sheets(contents, CAL_INVENTORY_SHEETS, 'END_TTL_QTY', file.filename)
        df_sale = process_cal_sheets(contents, CAL_SALE_SHEETS,      'CS_QTY',      file.filename)

        df_month = df_inv.merge(df_sale, on='PART_NO', how='outer', suffixes=('_庫存', '_銷售'))
        df_month = df_month.fillna(0)

        col_rename = {}
        for col in df_month.columns:
            if col == 'PART_NO':
                continue
            new = col
            new = new.replace('_QTY_庫存', '_庫存')
            new = new.replace('_QTY_銷售', '_銷售')
            new = new.replace('_QTY',     '_庫存')
            new = new.replace('Total_庫存', '庫存合計')
            new = new.replace('Total_銷售', '銷售合計')
            col_rename[col] = new
        df_month = df_month.rename(columns=col_rename)

        num_cols = [c for c in df_month.columns if c != 'PART_NO']
        df_month[num_cols] = df_month[num_cols].astype(int)

        month_dfs.append(df_month)
        present_months.append(month_name)

    # 月份按時間排序
    present_months_sorted = [m for m in MONTH_ORDER if m in present_months]
    month_dfs_sorted = [month_dfs[present_months.index(m)] for m in present_months_sorted]

    # === 處理商品成本 ===
    df_cost = None
    if has_cost:
        if not exchange_rates_json:
            raise HTTPException(status_code=400, detail="請提供匯率資料")
        try:
            exchange_rates = json.loads(exchange_rates_json)
        except Exception:
            raise HTTPException(status_code=400, detail="匯率格式錯誤")
        df_cost = await process_cal_cost(cost_file, exchange_rates)

    # === 處理在途庫存 ===
    df_inv_data = None
    if has_inventory:
        df_inv_data = await process_cal_inventory(inventory_file)

    # === 有採購大表：完整補貨計算 ===
    if has_procurement:
        df_loading = await process_cal_loading(loading_file)
        df_procurement_data = await process_cal_procurement(procurement_file)

        if loading_sets_json:
            try:
                loading_sets = json.loads(loading_sets_json)
            except Exception:
                raise HTTPException(status_code=400, detail="裝載套數格式錯誤")
        else:
            loading_sets = {
                'rmq': {'A': 2, 'B': 2, 'B7': 0},
                'khh': {'A': 15, 'B': 6, 'B7': 0},
                'tsa': {'A': 12, 'B': 5, 'B7': 0},
                'tpe': {'A': 247, 'B': 62, 'B7': 27},
            }

        df_final = calc_full_airport_report(
            month_dfs_sorted, present_months_sorted,
            df_loading, df_procurement_data, loading_sets,
            df_inv_data=df_inv_data, df_cost=df_cost
        )

        # MultiIndex 輸出
        df_final.columns = build_cal_multi_index(df_final.columns, present_months_sorted)

        if len(present_months_sorted) == 1:
            month_range = present_months_sorted[0]
        else:
            month_range = f'{present_months_sorted[0]}到{present_months_sorted[-1]}'
        out_filename = f'華航補貨計算表_{month_range}.xlsx'

        output_stream = io.BytesIO()
        with pd.ExcelWriter(output_stream, engine='openpyxl') as writer:
            df_final.to_excel(writer, sheet_name='報表', index=False)
        output_stream.seek(0)

        return StreamingResponse(
            output_stream,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename*=UTF-8''{quote(out_filename)}",
                "Access-Control-Expose-Headers": "Content-Disposition"
            }
        )

    # === 沒有月份檔案：只有在途庫存 / 商品成本（獨立下載） ===
    if not has_months:
        merged = None
        if df_inv_data is not None:
            merged = df_inv_data.sort_values(by='PART_NO').reset_index(drop=True)
        if df_cost is not None:
            if merged is not None:
                merged = merged.merge(df_cost[['PART_NO', 'TWD成本']], on='PART_NO', how='left')
            else:
                merged = df_cost.sort_values(by='PART_NO').reset_index(drop=True)

        out_filename = '華航庫存表.xlsx'
        output_stream = io.BytesIO()
        with pd.ExcelWriter(output_stream, engine='openpyxl') as writer:
            merged.to_excel(writer, sheet_name='報表', index=False)
        output_stream.seek(0)

        return StreamingResponse(
            output_stream,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename*=UTF-8''{quote(out_filename)}",
                "Access-Control-Expose-Headers": "Content-Disposition"
            }
        )

    # === 有月份但無採購大表：每月各自一個 xlsx ===
    def build_month_excel(df_month):
        df = df_month.copy()
        df = df.dropna(subset=['PART_NO'])
        df = df[df['PART_NO'].astype(str).str.strip() != '']
        for col in df.columns:
            if col != 'PART_NO':
                df[col] = df[col].astype(int)
        df = df.sort_values(by='PART_NO').reset_index(drop=True)

        if df_inv_data is not None:
            df = df.merge(df_inv_data, on='PART_NO', how='left')
            df['在途庫存'] = df['在途庫存'].fillna(0).astype(int)

        if df_cost is not None:
            df = df.merge(df_cost[['PART_NO', 'TWD成本']], on='PART_NO', how='left')

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='報表', index=False)
        buf.seek(0)
        return buf

    if len(month_dfs_sorted) == 1:
        month_name = present_months_sorted[0]
        output_stream = build_month_excel(month_dfs_sorted[0])
        out_filename = f'華航庫存計算表_{month_name}.xlsx'

        return StreamingResponse(
            output_stream,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename*=UTF-8''{quote(out_filename)}",
                "Access-Control-Expose-Headers": "Content-Disposition"
            }
        )

    zip_stream = io.BytesIO()
    with zipfile.ZipFile(zip_stream, 'w', zipfile.ZIP_DEFLATED) as zf:
        for df_month, month_name in zip(month_dfs_sorted, present_months_sorted):
            xlsx_buf = build_month_excel(df_month)
            zf.writestr(f'華航庫存計算表_{month_name}.xlsx', xlsx_buf.read())
    zip_stream.seek(0)

    zip_filename = f'華航庫存計算表_{present_months_sorted[0]}到{present_months_sorted[-1]}.zip'

    return StreamingResponse(
        zip_stream,
        media_type="application/zip",
        headers={
            "Content-Disposition": f"attachment; filename*=UTF-8''{quote(zip_filename)}",
            "Access-Control-Expose-Headers": "Content-Disposition"
        }
    )
