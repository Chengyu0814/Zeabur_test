"""華航：各上傳檔案的讀取與處理函式"""

import io

import pandas as pd
from fastapi import HTTPException, UploadFile


# === CAL sheet 名稱 ===
CAL_SALE_SHEETS = ['TPEKNCP', 'TSAKNCI', 'KHHKNCI', 'RMQKNCI']
CAL_INVENTORY_SHEETS = ['TPEKSCI', 'TPEKNCP', 'TSAKNCI', 'KHHKNCI', 'RMQKNCI']


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
