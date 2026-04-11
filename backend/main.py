"""FastAPI 入口檔

實際業務邏輯都拆到了：
  - 共用：constants.py / responses.py
  - 虎航：tigerair_processors.py / tigerair_replenishment.py / tigerair_router.py
  - 華航：cal_processors.py / cal_calculations.py / cal_formatting.py / cal_router.py
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from cal_router import router as cal_router
from tigerair_router import router as tigerair_router


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


app.include_router(tigerair_router)
app.include_router(cal_router)
