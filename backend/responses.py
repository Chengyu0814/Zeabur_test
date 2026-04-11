"""共用輸出工具：把 BytesIO 包成下載用的 StreamingResponse"""

from urllib.parse import quote

from fastapi.responses import StreamingResponse


XLSX_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
ZIP_MEDIA_TYPE = "application/zip"


def make_xlsx_response(buffer, filename: str) -> StreamingResponse:
    """把 xlsx BytesIO 包成檔案下載用的 StreamingResponse"""
    return StreamingResponse(
        buffer,
        media_type=XLSX_MEDIA_TYPE,
        headers={
            "Content-Disposition": f"attachment; filename*=UTF-8''{quote(filename)}",
            "Access-Control-Expose-Headers": "Content-Disposition",
        },
    )


def make_zip_response(buffer, filename: str) -> StreamingResponse:
    """把 zip BytesIO 包成檔案下載用的 StreamingResponse"""
    return StreamingResponse(
        buffer,
        media_type=ZIP_MEDIA_TYPE,
        headers={
            "Content-Disposition": f"attachment; filename*=UTF-8''{quote(filename)}",
            "Access-Control-Expose-Headers": "Content-Disposition",
        },
    )
