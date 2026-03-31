// ====================================================
// ⚙️  設定：部署到 Zeabur 後，改成你的後端 Domain
// ====================================================
const API_URL = window.CALCULATOR_API_URL || "http://localhost:8000";

// ====================================================
// DOM 元素
// ====================================================
const dropZone = document.getElementById("drop-zone");
const fileInput = document.getElementById("file-input");
const btnProcess = document.getElementById("btn-process");
const fileNameDisplay = document.getElementById("file-name");
const fileNameSpan = fileNameDisplay.querySelector("span");

const statusDot = document.getElementById("status-dot");
const statusText = document.getElementById("status-text");

let selectedFile = null;

// ====================================================
// 上傳介面互動 (Drag & Drop)
// ====================================================
dropZone.addEventListener("click", () => fileInput.click());

dropZone.addEventListener("dragover", (e) => {
  e.preventDefault();
  dropZone.classList.add("dragover");
});

dropZone.addEventListener("dragleave", (e) => {
  e.preventDefault();
  dropZone.classList.remove("dragover");
});

dropZone.addEventListener("drop", (e) => {
  e.preventDefault();
  dropZone.classList.remove("dragover");

  if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
    handleFileSelect(e.dataTransfer.files[0]);
  }
});

fileInput.addEventListener("change", function () {
  if (this.files && this.files.length > 0) {
    handleFileSelect(this.files[0]);
  }
});

function handleFileSelect(file) {
  // 檢查附檔名
  if (!file.name.match(/\.(xlsx|xls)$/i)) {
    alert("請上傳 Excel 檔案 (.xlsx 或 .xls)");
    return;
  }
  selectedFile = file;
  fileNameSpan.textContent = file.name;
  fileNameDisplay.style.display = "block";
  btnProcess.disabled = false;
  setStatus("online", "檔案準備就緒");
}

// ====================================================
// API 呼叫：上傳與下載處理
// ====================================================
async function processFile() {
  if (!selectedFile) return;

  setLoading(true);
  setStatus("loading", "處理中，請稍候…");

  const formData = new FormData();
  formData.append("file", selectedFile);

  try {
    const response = await fetch(`${API_URL}/process-excel`, {
      method: "POST",
      body: formData,
    });

    if (!response.ok) {
      // 嘗試解析錯誤訊息
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

    // 取得檔名（如果有 header 的話）或是預設一個檔名
    const contentDisposition = response.headers.get("Content-Disposition");
    let filename = `processed_${selectedFile.name}`;
    if (contentDisposition && contentDisposition.includes("filename=")) {
      // 提取 filename="..." 裡面的內容
      const matches = contentDisposition.match(/filename="([^"]+)"/);
      if (matches && matches[1]) {
        filename = matches[1];
      }
    }

    // 處理二進位檔案下載
    const blob = await response.blob();
    const downloadUrl = window.URL.createObjectURL(blob);
    
    // 建立臨時 <a> 標籤觸發下載
    const a = document.createElement("a");
    a.href = downloadUrl;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    window.URL.revokeObjectURL(downloadUrl);

    setStatus("online", "處理成功並開始下載 🎉");

  } catch (err) {
    alert("無法連接到伺服器或發生網路錯誤");
    setStatus("error", "連線失敗");
    console.error("API 錯誤：", err);
  } finally {
    setLoading(false);
  }
}

// ====================================================
// 輔助函式
// ====================================================
function setLoading(isLoading) {
  if (isLoading) {
    btnProcess.classList.add("loading");
  } else {
    btnProcess.classList.remove("loading");
  }
}

function setStatus(state, text) {
  statusDot.className = "status-dot " + state;
  statusText.textContent = text;
}

// 初始化：檢查 API 健康狀態
async function checkHealth() {
  setStatus("loading", "連線中…");
  try {
    const res = await fetch(`${API_URL}/`, { signal: AbortSignal.timeout(5000) });
    if (res.ok) {
      setStatus("online", "API 已連線，請上傳檔案");
    } else {
      setStatus("error", "API 伺服器異常");
    }
  } catch {
    setStatus("error", "無法連接後端");
  }
}

checkHealth();
