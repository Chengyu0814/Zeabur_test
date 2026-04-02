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
const fileList = document.getElementById("file-list");

const statusDot = document.getElementById("status-dot");
const statusText = document.getElementById("status-text");

let selectedFiles = [];

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
    handleFileSelect(e.dataTransfer.files);
  }
});

fileInput.addEventListener("change", function () {
  if (this.files && this.files.length > 0) {
    handleFileSelect(this.files);
  }
});

function handleFileSelect(files) {
  const valid = Array.from(files).filter(f => f.name.match(/\.(xlsx|xls)$/i));

  if (valid.length === 0) {
    alert("請上傳 Excel 檔案 (.xlsx 或 .xls)");
    return;
  }

  if (valid.length < files.length) {
    alert(`已過濾掉 ${files.length - valid.length} 個非 Excel 檔案`);
  }

  selectedFiles = valid;
  renderFileList();
  btnProcess.disabled = false;
  setStatus("online", `已選取 ${selectedFiles.length} 個檔案，準備就緒`);
}

function renderFileList() {
  if (selectedFiles.length === 0) {
    fileList.style.display = "none";
    return;
  }

  fileList.innerHTML = selectedFiles
    .map((f, i) => `<div class="file-item"><span class="file-index">${i + 1}</span><span class="file-name-text">${f.name}</span></div>`)
    .join("");
  fileList.style.display = "block";
}

// ====================================================
// API 呼叫：上傳與下載處理
// ====================================================
async function processFile() {
  if (selectedFiles.length === 0) return;

  setLoading(true);
  setStatus("loading", "處理中，請稍候…");

  const formData = new FormData();
  selectedFiles.forEach(f => formData.append("files", f));

  try {
    const response = await fetch(`${API_URL}/process-excel`, {
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
    let filename = "TTW sales summary.xlsx";
    if (contentDisposition && contentDisposition.includes("filename=")) {
      const matches = contentDisposition.match(/filename="([^"]+)"/);
      if (matches && matches[1]) {
        filename = matches[1];
      }
    }

    const blob = await response.blob();
    const downloadUrl = window.URL.createObjectURL(blob);

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
