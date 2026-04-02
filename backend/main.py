import io
from typing import List
from functools import reduce

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



@app.post("/process-excel")
async def process_excel(
    files: List[UploadFile] = File(...),
    months: List[str] = Form(...)
):
    """
    接收一或多個月份的 Excel 銷售明細，各自加總後 outer join，
    回傳 TTW sales summary MM-MM.xlsx。
    months: 與 files 對應的月份編號清單，如 ["01", "03"]
    """
    if not files:
        raise HTTPException(status_code=400, detail="請至少上傳一個檔案")
    if len(files) != len(months):
        raise HTTPException(status_code=400, detail="files 與 months 數量不符")

    all_sales = []  # 每月: SKU No., month銷售量, month銷售額
    all_names = []  # 每月: SKU No., 品名

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

    # Outer join 所有月份資料
    result = reduce(lambda l, r: l.merge(r, on="SKU No.", how="outer"), all_sales)

    # 品名：取所有月份中第一個非 null 的值
    name_df = pd.concat(all_names).drop_duplicates(subset=["SKU No."], keep="first")
    result = result.merge(name_df, on="SKU No.", how="left")

    # 整理欄位順序：SKU No., 品名, 各月銷售量..., 各月銷售額...
    present_months = [m for m in MONTH_ORDER if f"{m}銷售量" in result.columns]
    vol_cols = [f"{m}銷售量" for m in present_months]
    amt_cols = [f"{m}銷售額" for m in present_months]
    result = result[["SKU No.", "品名"] + vol_cols + amt_cols]

    # 輸出檔名
    month_nums = [MONTH_NUM[m] for m in present_months]
    out_filename = f"TTW sales summary {month_nums[0]}-{month_nums[-1]}.xlsx"

    output_stream = io.BytesIO()
    result.to_excel(output_stream, index=False)
    output_stream.seek(0)

    return StreamingResponse(
        output_stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{out_filename}"',
            "Access-Control-Expose-Headers": "Content-Disposition"
        }
    )
