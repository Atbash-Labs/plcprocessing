<#
.SYNOPSIS
    Apply diffs to CODESYS text exports

.DESCRIPTION
    Takes a diff directory and applies changes to a target export directory,
    producing a merged result that can be imported back to CODESYS.

.PARAMETER DiffDir
    Directory containing diff files (.diff, .added, .removed)

.PARAMETER TargetDir
    Directory containing the target export to apply diffs to

.PARAMETER OutputDir
    Directory to write the merged/modified files to

.PARAMETER ProjectPath
    Path to the CODESYS project (used for reference in output messages)

.EXAMPLE
    .\codesys-apply.ps1 -DiffDir ".\diffs" -TargetDir ".\export" -OutputDir ".\merged" -ProjectPath "MyProject.project"
#>

param(
    [Parameter(Mandatory=$true)]
    [string]$DiffDir,
    
    [Parameter(Mandatory=$true)]
    [string]$TargetDir,
    
    [Parameter(Mandatory=$true)]
    [string]$OutputDir,
    
    [Parameter(Mandatory=$false)]
    [string]$ProjectPath = "project.project"
)

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

# Validate inputs
if (-not (Test-Path $DiffDir)) {
    Write-Host "Error: Diff directory not found: $DiffDir" -ForegroundColor Red
    exit 1
}

if (-not (Test-Path $TargetDir)) {
    Write-Host "Error: Target directory not found: $TargetDir" -ForegroundColor Red
    exit 1
}

Write-Host "=== CODESYS Apply Diffs ===" -ForegroundColor Cyan
Write-Host "  Diff Dir:   $DiffDir" -ForegroundColor Gray
Write-Host "  Target Dir: $TargetDir" -ForegroundColor Gray
Write-Host "  Output Dir: $OutputDir" -ForegroundColor Gray

# Count diff files
$diffCount = (Get-ChildItem -Path $DiffDir -Filter "*.diff" -ErrorAction SilentlyContinue).Count
$addedCount = (Get-ChildItem -Path $DiffDir -Filter "*.added" -ErrorAction SilentlyContinue).Count
$removedCount = (Get-ChildItem -Path $DiffDir -Filter "*.removed" -ErrorAction SilentlyContinue).Count

Write-Host "`n  Diffs to apply:" -ForegroundColor Gray
Write-Host "    Modified: $diffCount" -ForegroundColor Cyan
Write-Host "    Added:    $addedCount" -ForegroundColor Green
Write-Host "    Removed:  $removedCount" -ForegroundColor Red

$pythonScript = Join-Path $scriptDir "scripts\codesys_apply.py"
if (-not (Test-Path $pythonScript)) {
    Write-Host "Error: Apply script not found: $pythonScript" -ForegroundColor Red
    exit 1
}

& python $pythonScript $DiffDir $TargetDir $ProjectPath $OutputDir

if ($LASTEXITCODE -eq 0) {
    Write-Host "`nDiffs applied successfully!" -ForegroundColor Green
    Write-Host "`nMerged files written to: $OutputDir" -ForegroundColor Gray
    
    # List output files
    $outputFiles = Get-ChildItem -Path $OutputDir -Filter "*.st" -ErrorAction SilentlyContinue
    if ($outputFiles) {
        Write-Host "`n--- Output Files ---" -ForegroundColor Yellow
        $outputFiles | ForEach-Object { Write-Host "  $($_.Name)" -ForegroundColor Gray }
    }
    
    Write-Host "`nNext step: Import to CODESYS with:" -ForegroundColor Yellow
    Write-Host "  .\codesys-import.ps1 -ProjectPath `"$ProjectPath`" -ImportDir `"$OutputDir`"" -ForegroundColor White
} else {
    Write-Host "`nApply failed with exit code: $LASTEXITCODE" -ForegroundColor Red
    exit $LASTEXITCODE
}















