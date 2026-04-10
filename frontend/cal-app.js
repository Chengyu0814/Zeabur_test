// ====================================================
// 設定
// ====================================================
const API_URL = window.CALCULATOR_API_URL || "http://localhost:8000";

// ====================================================
// 狀態
// ====================================================
const assignedFiles = {}; // { "01": File, "03": File, ... }
let inventoryFile = null;
let costFile = null;
let detectedCurrencies = [];

const statusDot  = document.getElementById("status-dot");
const statusText = document.getElementById("status-text");
const btnProcess = document.getElementById("btn-process");

// ====================================================
// 初始化 12 個月份格子
// ====================================================
document.querySelectorAll(".month-slot").forEach((slot) => {
  const month = slot.dataset.month;
  const input = slot.querySelector(".slot-input");

  slot.addEventListener("click", (e) => {
    if (!e.target.classList.contains("remove-btn")) input.click();
  });

  input.addEventListener("change", function () {
    if (this.files[0]) assignFile(month, this.files[0]);
    this.value = "";
  });

  slot.addEventListener("dragover", (e) => {
    e.preventDefault();
    slot.classList.add("dragover");
  });
  slot.addEventListener("dragleave", () => slot.classList.remove("dragover"));
  slot.addEventListener("drop", (e) => {
    e.preventDefault();
    slot.classList.remove("dragover");
    const file = e.dataTransfer.files[0];
    if (file) {
      if (!file.name.match(/\.(xlsx|xls)$/i)) {
        alert("請上傳 Excel 檔案 (.xlsx 或 .xls)");
        return;
      }
      assignFile(month, file);
    }
  });
});

// ====================================================
// 月份格子操作
// ====================================================
function assignFile(month, file) {
  assignedFiles[month] = file;
  renderSlot(month);
  updateButton();
}

function removeFile(month) {
  delete assignedFiles[month];
  renderSlot(month);
  updateButton();
}

function renderSlot(month) {
  const slot     = document.querySelector(`.month-slot[data-month="${month}"]`);
  const plus     = slot.querySelector(".slot-plus");
  let fileEl     = slot.querySelector(".slot-filename");
  let removeBtn  = slot.querySelector(".remove-btn");

  if (assignedFiles[month]) {
    slot.classList.add("assigned");
    plus.style.display = "none";

    if (!fileEl) {
      fileEl = document.createElement("span");
      fileEl.className = "slot-filename";
      slot.appendChild(fileEl);
    }
    const name = assignedFiles[month].name;
    fileEl.textContent = name.length > 14 ? name.slice(0, 12) + "…" : name;
    fileEl.title = name;

    if (!removeBtn) {
      removeBtn = document.createElement("span");
      removeBtn.className = "remove-btn";
      removeBtn.textContent = "×";
      removeBtn.addEventListener("click", (e) => {
        e.stopPropagation();
        removeFile(month);
      });
      slot.appendChild(removeBtn);
    }
  } else {
    slot.classList.remove("assigned");
    plus.style.display = "";
    if (fileEl)    fileEl.remove();
    if (removeBtn) removeBtn.remove();
  }
}

function updateButton() {
  const count = Object.keys(assignedFiles).length;
  btnProcess.disabled = count === 0 && !inventoryFile && !costFile;

  const parts = [];
  if (count > 0) {
    const months = Object.keys(assignedFiles).sort();
    parts.push(months.map(m => parseInt(m) + "月").join("、"));
  }
  if (inventoryFile) parts.push("在途庫存");
  if (costFile) parts.push("商品成本");

  if (parts.length > 0) {
    setStatus("online", `已選取：${parts.join("、")}`);
  } else {
    setStatus("online", "請將 Excel 檔案放入月份格子");
  }
}

// ====================================================
// API 呼叫
// ====================================================
async function processFile() {
  const entries = Object.entries(assignedFiles).sort(([a], [b]) => a.localeCompare(b));
  if (entries.length === 0 && !inventoryFile && !costFile) return;

  setLoading(true);
  setStatus("loading", "處理中，請稍候…");

  const formData = new FormData();
  entries.forEach(([month, file]) => {
    formData.append("files", file);
    formData.append("months", month);
  });

  if (inventoryFile) {
    formData.append("inventory_file", inventoryFile);
  }

  if (costFile) {
    const missing = detectedCurrencies.filter(c => {
      const input = document.getElementById(`rate-${c}`);
      return !input || !input.value;
    });
    if (missing.length > 0) {
      alert(`請填寫以下幣別的匯率：${missing.join("、")}`);
      setLoading(false);
      return;
    }
    formData.append("cost_file", costFile);
    formData.append("exchange_rates_json", JSON.stringify(getExchangeRates()));
  }

  try {
    const response = await fetch(`${API_URL}/cal/process-excel`, {
      method: "POST",
      body: formData,
    });

    if (!response.ok) {
      let errorDetail = "檔案處理失敗";
      try {
        const errJson = await response.json();
        errorDetail = errJson.detail || errorDetail;
      } catch (e) {
        console.error(e);
      }
      alert(`錯誤：${errorDetail}`);
      setStatus("error", "處理失敗");
      return;
    }

    const contentDisposition = response.headers.get("Content-Disposition");
    let filename = "華航庫存計算表.xlsx";
    if (contentDisposition) {
      const m = contentDisposition.match(/filename\*=UTF-8''(.+)/);
      if (m) filename = decodeURIComponent(m[1]);
    }

    const blob = await response.blob();
    const url  = window.URL.createObjectURL(blob);
    const a    = document.createElement("a");
    a.href     = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    window.URL.revokeObjectURL(url);

    setStatus("online", "處理成功，開始下載");
  } catch (err) {
    alert("無法連接到伺服器或發生網路錯誤");
    setStatus("error", "連線失敗");
    console.error(err);
  } finally {
    setLoading(false);
  }
}

// ====================================================
// 輔助函式
// ====================================================
function setLoading(isLoading) {
  btnProcess.classList.toggle("loading", isLoading);
}

function setStatus(state, text) {
  statusDot.className = "status-dot " + state;
  statusText.textContent = text;
}

async function checkHealth() {
  setStatus("loading", "連線中…");
  try {
    const res = await fetch(`${API_URL}/`, { signal: AbortSignal.timeout(5000) });
    if (res.ok) {
      setStatus("online", "API 已連線，請將檔案拖入月份格子");
    } else {
      setStatus("error", "API 伺服器異常");
    }
  } catch {
    setStatus("error", "無法連接後端");
  }
}

// ====================================================
// 在途庫存上傳
// ====================================================
const inventoryDrop   = document.getElementById("inventory-drop");
const inventoryInput  = document.getElementById("inventory-input");
const invText         = document.getElementById("inv-text");
const invRemove       = document.getElementById("inv-remove");

inventoryDrop.addEventListener("click", (e) => {
  if (!e.target.classList.contains("inv-remove")) inventoryInput.click();
});
inventoryInput.addEventListener("change", function () {
  if (this.files[0]) setInventoryFile(this.files[0]);
  this.value = "";
});
inventoryDrop.addEventListener("dragover",  (e) => { e.preventDefault(); inventoryDrop.classList.add("dragover"); });
inventoryDrop.addEventListener("dragleave", ()  => inventoryDrop.classList.remove("dragover"));
inventoryDrop.addEventListener("drop", (e) => {
  e.preventDefault();
  inventoryDrop.classList.remove("dragover");
  const file = e.dataTransfer.files[0];
  if (file) {
    if (!file.name.match(/\.(xlsx|xls)$/i)) { alert("請上傳 Excel 檔案 (.xlsx 或 .xls)"); return; }
    setInventoryFile(file);
  }
});

function setInventoryFile(file) {
  inventoryFile = file;
  inventoryDrop.classList.add("assigned");
  const name = file.name.length > 30 ? file.name.slice(0, 28) + "…" : file.name;
  invText.textContent = name;
  invText.title = file.name;
  invRemove.style.display = "";
  updateButton();
}

function removeInventoryFile() {
  inventoryFile = null;
  inventoryDrop.classList.remove("assigned");
  invText.textContent = "選擇採購未交量 Excel 檔案（選填）";
  invText.title = "";
  invRemove.style.display = "none";
  updateButton();
}

// ====================================================
// 商品成本上傳
// ====================================================
const costDrop = document.getElementById("cost-drop");
const costInput = document.getElementById("cost-input");
const costText = document.getElementById("cost-text");
const costRemove = document.getElementById("cost-remove");
const exchangeRatesPanel = document.getElementById("exchange-rates-panel");
const exchangeRateInputsEl = document.getElementById("exchange-rate-inputs");

costDrop.addEventListener("click", (e) => {
  if (!e.target.classList.contains("inv-remove")) costInput.click();
});

costInput.addEventListener("change", function () {
  if (this.files[0]) setCostFile(this.files[0]);
  this.value = "";
});

costDrop.addEventListener("dragover", (e) => { e.preventDefault(); costDrop.classList.add("dragover"); });
costDrop.addEventListener("dragleave", () => { costDrop.classList.remove("dragover"); });
costDrop.addEventListener("drop", (e) => {
  e.preventDefault();
  costDrop.classList.remove("dragover");
  const file = e.dataTransfer.files[0];
  if (file) {
    if (!file.name.match(/\.(xlsx|xls)$/i)) { alert("請上傳 Excel 檔案 (.xlsx 或 .xls)"); return; }
    setCostFile(file);
  }
});

async function setCostFile(file) {
  costFile = file;
  costDrop.classList.add("assigned");
  const name = file.name.length > 30 ? file.name.slice(0, 28) + "…" : file.name;
  costText.textContent = name;
  costText.title = file.name;
  costRemove.style.display = "";
  updateButton();

  exchangeRateInputsEl.innerHTML = '<div style="font-size:0.72rem;color:var(--text-secondary)">掃描幣別中…</div>';
  exchangeRatesPanel.style.display = "";

  try {
    const fd = new FormData();
    fd.append("cost_file", file);
    const res = await fetch(`${API_URL}/scan-cost-currencies`, { method: "POST", body: fd });
    if (!res.ok) throw new Error("scan failed");
    const data = await res.json();
    detectedCurrencies = data.currencies;
    renderExchangeRateInputs(detectedCurrencies);
  } catch {
    exchangeRateInputsEl.innerHTML = '<div style="font-size:0.72rem;color:#ef4444">掃描幣別失敗，請確認檔案格式</div>';
  }
}

function renderExchangeRateInputs(currencies) {
  if (currencies.length === 0) {
    exchangeRateInputsEl.innerHTML = '<div style="font-size:0.72rem;color:var(--text-secondary)">全部為台幣，無需填寫匯率</div>';
    return;
  }
  exchangeRateInputsEl.innerHTML = currencies.map(c => `
    <div class="rate-input-row">
      <span class="rate-label">${c}</span>
      <input class="rate-input" type="number" step="0.0001" min="0" placeholder="請輸入匯率" id="rate-${c}" data-currency="${c}">
    </div>
  `).join("");
}

function removeCostFile() {
  costFile = null;
  detectedCurrencies = [];
  costDrop.classList.remove("assigned");
  costText.textContent = "選擇品號價格資料 Excel 檔案（選填）";
  costText.title = "";
  costRemove.style.display = "none";
  exchangeRatesPanel.style.display = "none";
  exchangeRateInputsEl.innerHTML = "";
  updateButton();
}

function getExchangeRates() {
  const rates = { "台幣": 1.0 };
  detectedCurrencies.forEach(c => {
    const input = document.getElementById(`rate-${c}`);
    if (input && input.value) rates[c] = parseFloat(input.value);
  });
  return rates;
}

checkHealth();
