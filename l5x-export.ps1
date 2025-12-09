#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Export Rockwell/Allen-Bradley L5X files to structured code (.sc) format

.DESCRIPTION
    Exports L5X files (Rockwell Logix 5000) to text-based .sc files for version control.
    Can process a single L5X file or an entire directory.

.PARAMETER Input
    Path to L5X file or directory containing L5X files

.PARAMETER OutputDir
    Directory where .sc files will be exported

.EXAMPLE
    .\l5x-export.ps1 -Input "Motor_Control.L5X" -OutputDir ".\export"
    Export a single L5X file

.EXAMPLE
    .\l5x-export.ps1 -Input ".\PLC" -OutputDir ".\export"
    Export all L5X files from PLC directory
#>

param(
    [Parameter(Mandatory=$true)]
    [string]$Input,

    [Parameter(Mandatory=$true)]
    [string]$OutputDir
)

$ErrorActionPreference = "Stop"

# Get script directory
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$pythonScript = Join-Path $scriptDir "scripts\l5x_export.py"

# Validate inputs
if (-not (Test-Path $Input)) {
    Write-Error "Input path not found: $Input"
    exit 1
}

if (-not (Test-Path $pythonScript)) {
    Write-Error "Python script not found: $pythonScript"
    exit 1
}

# Check for Python
try {
    $pythonVersion = python --version 2>&1
    Write-Host "[INFO] Using $pythonVersion" -ForegroundColor Cyan
} catch {
    Write-Error "Python not found. Please install Python 3.x"
    exit 1
}

# Run export
Write-Host "`n[INFO] Starting L5X export..." -ForegroundColor Cyan
Write-Host "[INFO] Input: $Input" -ForegroundColor Cyan
Write-Host "[INFO] Output: $OutputDir" -ForegroundColor Cyan
Write-Host ""

try {
    python $pythonScript $Input $OutputDir

    if ($LASTEXITCODE -eq 0) {
        Write-Host "`n[SUCCESS] Export completed!" -ForegroundColor Green
    } else {
        Write-Error "Export failed with exit code $LASTEXITCODE"
        exit $LASTEXITCODE
    }
} catch {
    Write-Error "Export failed: $_"
    exit 1
}
