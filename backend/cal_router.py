"""華航：API endpoint

對應前端 china-airlines.html / cal-app.js。
"""

import io
import json
import zipfile
from typing import List, Optional

import pandas as pd
from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from cal_calculations import calc_full_airport_report
from cal_formatting import build_cal_multi_index
from cal_processors import (
    CAL_INVENTORY_SHEETS,
    CAL_SALE_SHEETS,
    process_cal_cost,
    process_cal_inventory,
    process_cal_loading,
    process_cal_procurement,
    process_cal_sheets,
)
from constants import MONTH_NAMES, MONTH_ORDER
from responses import make_xlsx_response, make_zip_response


router = APIRouter()


@router.post("/cal/process-excel")
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
        return await _handle_full_procurement(
            month_dfs_sorted, present_months_sorted,
            loading_file, procurement_file, loading_sets_json,
            df_inv_data, df_cost
        )

    # === 沒有月份檔案：只有在途庫存 / 商品成本（獨立下載） ===
    if not has_months:
        return _handle_inventory_or_cost_only(df_inv_data, df_cost)

    # === 有月份但無採購大表：每月各自一個 xlsx ===
    return _handle_monthly_only(month_dfs_sorted, present_months_sorted, df_inv_data, df_cost)


# ─────────────────────────────────────────────────────────────
# 各分支的處理 helper
# ─────────────────────────────────────────────────────────────

async def _handle_full_procurement(
    month_dfs_sorted, present_months_sorted,
    loading_file, procurement_file, loading_sets_json,
    df_inv_data, df_cost,
):
    """有採購大表時的完整四倉補貨計算流程"""
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

    return make_xlsx_response(output_stream, out_filename)


def _handle_inventory_or_cost_only(df_inv_data, df_cost):
    """沒有月份檔案：只有在途庫存 / 商品成本，輸出單一檔案"""
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

    return make_xlsx_response(output_stream, out_filename)


def _handle_monthly_only(month_dfs_sorted, present_months_sorted, df_inv_data, df_cost):
    """有月份但無採購大表：單月一個 xlsx，多月打包成 zip"""

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
        return make_xlsx_response(output_stream, out_filename)

    # 多月打包成 zip
    zip_stream = io.BytesIO()
    with zipfile.ZipFile(zip_stream, 'w', zipfile.ZIP_DEFLATED) as zf:
        for df_month, month_name in zip(month_dfs_sorted, present_months_sorted):
            xlsx_buf = build_month_excel(df_month)
            zf.writestr(f'華航庫存計算表_{month_name}.xlsx', xlsx_buf.read())
    zip_stream.seek(0)

    zip_filename = f'華航庫存計算表_{present_months_sorted[0]}到{present_months_sorted[-1]}.zip'
    return make_zip_response(zip_stream, zip_filename)
