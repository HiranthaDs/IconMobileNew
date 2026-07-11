$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot

Write-Host "Setting up ICON MOBILE LAN ERP..." -ForegroundColor Cyan

if (-not (Test-Path -LiteralPath ".venv\Scripts\python.exe")) {
    py -3 -m venv .venv
}

& ".venv\Scripts\python.exe" -m pip install --upgrade pip
& ".venv\Scripts\python.exe" -m pip install -r requirements.txt

if (-not (Get-Command npm -ErrorAction SilentlyContinue)) {
    throw "Node.js/npm is required once to build the offline browser assets. Install Node.js 18 or newer."
}

if (Test-Path -LiteralPath "package-lock.json") {
    npm ci
} else {
    npm install
}
npm run build

Write-Host ""
Write-Host "Setup complete. Run .\start.ps1" -ForegroundColor Green
