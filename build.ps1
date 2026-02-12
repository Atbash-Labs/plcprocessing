<#
.SYNOPSIS
    Build the PLC Ontology Assistant into a distributable package.

.DESCRIPTION
    This script:
      1. Creates a Python virtual-env and installs dependencies + PyInstaller
      2. Runs PyInstaller to produce  build/python-backend/
      3. Runs electron-builder to produce the final installer in  dist/

    Prerequisites:
      - Python 3.10+ on PATH  (python --version)
      - Node.js 18+ on PATH   (node --version)
      - npm on PATH

.PARAMETER SkipPython
    Skip the PyInstaller step (reuse an existing build/python-backend/).

.PARAMETER SkipElectron
    Skip the electron-builder step.

.EXAMPLE
    .\build.ps1                 # Full build
    .\build.ps1 -SkipPython     # Rebuild Electron only
#>
param(
    [switch]$SkipPython,
    [switch]$SkipElectron
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Definition

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  PLC Ontology Assistant - Build" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

# ---------------------------------------------------------------
# Step 1: Python backend  (PyInstaller)
# ---------------------------------------------------------------
if (-not $SkipPython) {
    Write-Host "[1/3] Setting up Python virtual environment ..." -ForegroundColor Yellow
    $venvDir = Join-Path $root ".build-venv"

    if (Test-Path $venvDir) {
        Write-Host "  Removing old venv ..."
        Remove-Item -Recurse -Force $venvDir
    }

    # Create venv – use --without-pip as a fallback if ensurepip fails
    try {
        python -m venv $venvDir 2>$null
    } catch {}
    $pythonExe = Join-Path $venvDir "Scripts\python.exe"

    if (-not (Test-Path $pythonExe)) {
        Write-Host "ERROR: Failed to create Python virtual environment." -ForegroundColor Red
        exit 1
    }

    # Ensure pip is available inside the venv
    $pip = Join-Path $venvDir "Scripts\pip.exe"
    if (-not (Test-Path $pip)) {
        Write-Host "  Bootstrapping pip in venv (ensurepip) ..."
        python -m venv --without-pip $venvDir
        & $pythonExe -m ensurepip --default-pip 2>$null
        if (-not (Test-Path $pip)) {
            # Last resort: use get-pip.py
            Write-Host "  Downloading get-pip.py ..."
            $getPip = Join-Path $venvDir "get-pip.py"
            Invoke-WebRequest -Uri "https://bootstrap.pypa.io/get-pip.py" -OutFile $getPip
            & $pythonExe $getPip 2>&1 | Out-Null
        }
    }

    Write-Host "  Installing Python dependencies ..."
    & $pythonExe -m pip install --upgrade pip 2>&1 | Out-Null
    & $pythonExe -m pip install -r (Join-Path $root "requirements.txt") 2>&1 | Out-Null
    & $pythonExe -m pip install pyinstaller 2>&1 | Out-Null

    Write-Host "`n[2/3] Running PyInstaller ..." -ForegroundColor Yellow

    # Clean previous build artefacts
    $pyiBuildDir = Join-Path $root "build"
    $pyiDistDir  = Join-Path $root "build\python-backend"
    if (Test-Path $pyiDistDir) {
        Remove-Item -Recurse -Force $pyiDistDir
    }

    $specFile = Join-Path $root "pyinstaller.spec"

    # PyInstaller outputs to  dist/python-backend  by default; we override to
    # build/python-backend so electron-builder can pick it up via extraResources.
    & $pythonExe -m PyInstaller `
        --distpath (Join-Path $root "build") `
        --workpath (Join-Path $root "build\pyinstaller-work") `
        --noconfirm `
        $specFile

    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: PyInstaller failed." -ForegroundColor Red
        exit 1
    }

    # Verify the dispatcher exe was created
    $dispatcherExe = Join-Path $pyiDistDir "dispatcher.exe"
    if (-not (Test-Path $dispatcherExe)) {
        Write-Host "ERROR: dispatcher.exe not found at $dispatcherExe" -ForegroundColor Red
        exit 1
    }
    Write-Host "  Python backend built at: $pyiDistDir" -ForegroundColor Green
} else {
    Write-Host "[1-2/3] Skipping Python build (--SkipPython)" -ForegroundColor DarkGray
}

# ---------------------------------------------------------------
# Step 2: Electron app  (electron-builder)
# ---------------------------------------------------------------
if (-not $SkipElectron) {
    Write-Host "`n[3/3] Building Electron app ..." -ForegroundColor Yellow

    $electronDir = Join-Path $root "electron-ui"
    Push-Location $electronDir

    # Verify python-backend exists
    $backendCheck = Join-Path $root "build\python-backend\dispatcher.exe"
    if (-not (Test-Path $backendCheck)) {
        Write-Host "ERROR: build/python-backend/dispatcher.exe not found." -ForegroundColor Red
        Write-Host "       Run without -SkipPython first." -ForegroundColor Red
        Pop-Location
        exit 1
    }

    Write-Host "  Installing npm dependencies ..."
    # npm writes deprecation warnings to stderr which PowerShell treats as
    # terminating errors under $ErrorActionPreference=Stop. Temporarily relax.
    $prevPref = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    npm install 2>&1 | Out-Null
    $ErrorActionPreference = $prevPref

    Write-Host "  Running electron-builder ..."
    $prevPref = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    npm run dist 2>&1 | ForEach-Object { Write-Host $_ }
    $buildExit = $LASTEXITCODE
    $ErrorActionPreference = $prevPref

    if ($buildExit -ne 0) {
        Write-Host "ERROR: electron-builder failed (exit code $buildExit)." -ForegroundColor Red
        Pop-Location
        exit 1
    }

    Pop-Location
    Write-Host "  Electron app built. Check the dist/ folder." -ForegroundColor Green
} else {
    Write-Host "[3/3] Skipping Electron build (--SkipElectron)" -ForegroundColor DarkGray
}

# ---------------------------------------------------------------
# Done
# ---------------------------------------------------------------
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  Build complete!" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Outputs:" -ForegroundColor White
Write-Host "  Python backend:  build\python-backend\" -ForegroundColor White
Write-Host "  Electron app:    dist\" -ForegroundColor White
Write-Host ""
Write-Host "To run the packaged app:" -ForegroundColor White
Write-Host "  dist\win-unpacked\PLC Ontology Assistant.exe" -ForegroundColor White
Write-Host ""
Write-Host "NOTE: Place your .env file (with NEO4J and ANTHROPIC keys)" -ForegroundColor Yellow
Write-Host "      next to the .exe for the app to find it." -ForegroundColor Yellow
