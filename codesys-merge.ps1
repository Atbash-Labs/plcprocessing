<#
.SYNOPSIS
    Complete merge workflow: Apply diffs to a CODESYS export and import back to project

.DESCRIPTION
    Combines the apply and import steps into a single workflow:
    1. Apply diffs to a target export directory
    2. Import the merged result back to the CODESYS project
    
    This is the typical workflow for merging changes from one version into another.

.PARAMETER DiffDir
    Directory containing diff files (.diff, .added, .removed)

.PARAMETER TargetDir
    Directory containing the target export to apply diffs to

.PARAMETER ProjectPath
    Path to the CODESYS .project file to import into

.PARAMETER WorkDir
    Optional: Working directory for intermediate files (default: temp directory)

.PARAMETER DryRun
    Preview changes without modifying the project

.PARAMETER KeepWorkDir
    Don't delete the working directory after completion

.PARAMETER CodesysPath
    Optional: Path to CODESYS.exe (auto-detected if not specified)

.PARAMETER Profile
    Optional: CODESYS profile name (default: CODESYS V3.5 SP21 Patch 3)

.EXAMPLE
    .\codesys-merge.ps1 -DiffDir ".\diffs" -TargetDir ".\export" -ProjectPath "MyProject.project" -DryRun
    
.EXAMPLE
    .\codesys-merge.ps1 -DiffDir ".\diffs" -TargetDir ".\export" -ProjectPath "MyProject.project"
#>

param(
    [Parameter(Mandatory=$true)]
    [string]$DiffDir,
    
    [Parameter(Mandatory=$true)]
    [string]$TargetDir,
    
    [Parameter(Mandatory=$true)]
    [string]$ProjectPath,
    
    [Parameter(Mandatory=$false)]
    [string]$WorkDir = "",
    
    [Parameter(Mandatory=$false)]
    [switch]$DryRun,
    
    [Parameter(Mandatory=$false)]
    [switch]$KeepWorkDir,
    
    [Parameter(Mandatory=$false)]
    [string]$CodesysPath = "",
    
    [Parameter(Mandatory=$false)]
    [string]$Profile = "CODESYS V3.5 SP21 Patch 3"
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

if (-not (Test-Path $ProjectPath)) {
    Write-Host "Error: Project file not found: $ProjectPath" -ForegroundColor Red
    exit 1
}

# Create working directory
$createdWorkDir = $false
if ([string]::IsNullOrEmpty($WorkDir)) {
    $WorkDir = Join-Path ([System.IO.Path]::GetTempPath()) "codesys_merge_$(Get-Date -Format 'yyyyMMdd_HHmmss')"
    $createdWorkDir = $true
}

if (-not (Test-Path $WorkDir)) {
    New-Item -ItemType Directory -Path $WorkDir -Force | Out-Null
    $createdWorkDir = $true
}

$mergedDir = Join-Path $WorkDir "merged"

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  CODESYS Merge Workflow" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Diff Dir:    $DiffDir" -ForegroundColor Gray
Write-Host "  Target Dir:  $TargetDir" -ForegroundColor Gray
Write-Host "  Project:     $ProjectPath" -ForegroundColor Gray
Write-Host "  Work Dir:    $WorkDir" -ForegroundColor Gray

if ($DryRun) {
    Write-Host "  Mode:        DRY RUN" -ForegroundColor Yellow
}

Write-Host ""

# Step 1: Apply diffs
Write-Host "Step 1/2: Applying diffs..." -ForegroundColor Cyan
Write-Host "--------------------------------------------" -ForegroundColor Gray

$applyScript = Join-Path $scriptDir "scripts\codesys_apply.py"
if (-not (Test-Path $applyScript)) {
    Write-Host "Error: Apply script not found: $applyScript" -ForegroundColor Red
    exit 1
}

& python $applyScript $DiffDir $TargetDir $ProjectPath $mergedDir

if ($LASTEXITCODE -ne 0) {
    Write-Host "`nError: Failed to apply diffs (exit code: $LASTEXITCODE)" -ForegroundColor Red
    exit $LASTEXITCODE
}

Write-Host ""

# Step 2: Import to project
Write-Host "Step 2/2: Importing to project..." -ForegroundColor Cyan
Write-Host "--------------------------------------------" -ForegroundColor Gray

$importScript = Join-Path $scriptDir "scripts\codesys_import_external.py"
if (-not (Test-Path $importScript)) {
    Write-Host "Error: Import script not found: $importScript" -ForegroundColor Red
    exit 1
}

# Build import arguments
$importArgs = @($ProjectPath, $mergedDir)
if (-not [string]::IsNullOrEmpty($CodesysPath)) {
    $importArgs += "--codesys-path"
    $importArgs += $CodesysPath
}
$importArgs += "--profile"
$importArgs += $Profile
if ($DryRun) {
    $importArgs += "--dry-run"
}

& python $importScript @importArgs

if ($LASTEXITCODE -ne 0) {
    Write-Host "`nError: Failed to import to project (exit code: $LASTEXITCODE)" -ForegroundColor Red
    exit $LASTEXITCODE
}

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan

if ($DryRun) {
    Write-Host "  DRY RUN COMPLETE - No changes made" -ForegroundColor Yellow
    Write-Host "  Run without -DryRun to apply changes" -ForegroundColor Gray
} else {
    Write-Host "  MERGE COMPLETE!" -ForegroundColor Green
}

Write-Host "============================================" -ForegroundColor Cyan

# Cleanup
if ($createdWorkDir -and -not $KeepWorkDir -and -not $DryRun) {
    Write-Host "`nCleaning up working directory..." -ForegroundColor Gray
    Remove-Item -Path $WorkDir -Recurse -Force -ErrorAction SilentlyContinue
} elseif ($KeepWorkDir -or $DryRun) {
    Write-Host "`nMerged files available at: $mergedDir" -ForegroundColor Gray
}






