<#
.SYNOPSIS
    Generate diff between two CODESYS text exports

.DESCRIPTION
    Compares two directories of exported CODESYS text files (.st) and generates
    unified diffs showing additions, modifications, and removals.

.PARAMETER BaseDir
    Directory containing the base/original export (before changes)

.PARAMETER TargetDir
    Directory containing the target export (after changes)

.PARAMETER OutputDir
    Directory to write diff files to

.EXAMPLE
    .\codesys-diff.ps1 -BaseDir ".\export_v1" -TargetDir ".\export_v2" -OutputDir ".\diffs"
#>

param(
    [Parameter(Mandatory=$true)]
    [string]$BaseDir,
    
    [Parameter(Mandatory=$true)]
    [string]$TargetDir,
    
    [Parameter(Mandatory=$true)]
    [string]$OutputDir
)

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

# Validate inputs
if (-not (Test-Path $BaseDir)) {
    Write-Host "Error: Base directory not found: $BaseDir" -ForegroundColor Red
    exit 1
}

if (-not (Test-Path $TargetDir)) {
    Write-Host "Error: Target directory not found: $TargetDir" -ForegroundColor Red
    exit 1
}

Write-Host "=== CODESYS Diff ===" -ForegroundColor Cyan
Write-Host "  Base:   $BaseDir" -ForegroundColor Gray
Write-Host "  Target: $TargetDir" -ForegroundColor Gray
Write-Host "  Output: $OutputDir" -ForegroundColor Gray

$pythonScript = Join-Path $scriptDir "scripts\codesys_diff.py"
if (-not (Test-Path $pythonScript)) {
    Write-Host "Error: Diff script not found: $pythonScript" -ForegroundColor Red
    exit 1
}

& python $pythonScript $BaseDir $TargetDir $OutputDir

if ($LASTEXITCODE -eq 0) {
    Write-Host "`nDiff completed successfully!" -ForegroundColor Green
    
    # Display summary if it exists
    $summaryFile = Join-Path $OutputDir "diff_summary.txt"
    if (Test-Path $summaryFile) {
        Write-Host "`n--- Diff Summary ---" -ForegroundColor Yellow
        Get-Content $summaryFile
    }
    
    # List generated diff files
    $diffFiles = Get-ChildItem -Path $OutputDir -Filter "*.diff" -ErrorAction SilentlyContinue
    $addedFiles = Get-ChildItem -Path $OutputDir -Filter "*.added" -ErrorAction SilentlyContinue
    $removedFiles = Get-ChildItem -Path $OutputDir -Filter "*.removed" -ErrorAction SilentlyContinue
    
    if ($diffFiles -or $addedFiles -or $removedFiles) {
        Write-Host "`n--- Generated Files ---" -ForegroundColor Yellow
        if ($diffFiles) {
            $diffFiles | ForEach-Object { Write-Host "  [DIFF] $($_.Name)" -ForegroundColor Cyan }
        }
        if ($addedFiles) {
            $addedFiles | ForEach-Object { Write-Host "  [ADD]  $($_.Name)" -ForegroundColor Green }
        }
        if ($removedFiles) {
            $removedFiles | ForEach-Object { Write-Host "  [DEL]  $($_.Name)" -ForegroundColor Red }
        }
    }
} else {
    Write-Host "`nDiff failed with exit code: $LASTEXITCODE" -ForegroundColor Red
    exit $LASTEXITCODE
}

