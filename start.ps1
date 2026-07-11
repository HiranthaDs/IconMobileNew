$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot

$python = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $python)) {
    throw "Python environment is missing. Run .\setup.ps1 first."
}
if (-not (Test-Path -LiteralPath (Join-Path $PSScriptRoot "assets\app.css"))) {
    throw "Offline browser assets are missing. Run .\setup.ps1 first."
}

& $python main.py
