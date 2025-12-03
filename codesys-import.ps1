<#
.SYNOPSIS
    Import text files back into a CODESYS project

.DESCRIPTION
    Imports .st text files into a CODESYS project using headless CODESYS execution.
    Performs authoritative import - items not in the import directory will be deleted
    from the project.

.PARAMETER ProjectPath
    Path to the CODESYS .project file

.PARAMETER ImportDir
    Directory containing .st files to import

.PARAMETER DryRun
    Preview changes without modifying the project

.PARAMETER CodesysPath
    Optional: Path to CODESYS.exe (auto-detected if not specified)

.PARAMETER Profile
    Optional: CODESYS profile name (default: CODESYS V3.5 SP21 Patch 3)

.EXAMPLE
    .\codesys-import.ps1 -ProjectPath "MyProject.project" -ImportDir ".\import" -DryRun
    
.EXAMPLE
    .\codesys-import.ps1 -ProjectPath "MyProject.project" -ImportDir ".\import"
#>

param(
    [Parameter(Mandatory=$true)]
    [string]$ProjectPath,
    
    [Parameter(Mandatory=$true)]
    [string]$ImportDir,
    
    [Parameter(Mandatory=$false)]
    [switch]$DryRun,
    
    [Parameter(Mandatory=$false)]
    [string]$CodesysPath = "",
    
    [Parameter(Mandatory=$false)]
    [string]$Profile = "CODESYS V3.5 SP21 Patch 3"
)

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

# Validate inputs
if (-not (Test-Path $ProjectPath)) {
    Write-Host "Error: Project file not found: $ProjectPath" -ForegroundColor Red
    exit 1
}

if (-not (Test-Path $ImportDir)) {
    Write-Host "Error: Import directory not found: $ImportDir" -ForegroundColor Red
    exit 1
}

Write-Host "=== CODESYS Import ===" -ForegroundColor Cyan
Write-Host "  Project:    $ProjectPath" -ForegroundColor Gray
Write-Host "  Import Dir: $ImportDir" -ForegroundColor Gray
Write-Host "  Profile:    $Profile" -ForegroundColor Gray

if ($DryRun) {
    Write-Host "  Mode:       DRY RUN (no changes will be made)" -ForegroundColor Yellow
}

# List files to import
$stFiles = Get-ChildItem -Path $ImportDir -Filter "*.st" -ErrorAction SilentlyContinue
if ($stFiles) {
    Write-Host "`n  Files to import:" -ForegroundColor Gray
    $stFiles | ForEach-Object {
        $type = switch -Wildcard ($_.Name) {
            "*.gvl.st" { "[GVL] " }
            "*.prg.st" { "[PRG] " }
            "*.fb.st"  { "[FB]  " }
            "*.fun.st" { "[FUN] " }
            "*.meth.st" { "[METH]" }
            default { "[???] " }
        }
        Write-Host "    $type $($_.Name)" -ForegroundColor Gray
    }
} else {
    Write-Host "`nWarning: No .st files found in import directory" -ForegroundColor Yellow
}

Write-Host "" # blank line

$pythonScript = Join-Path $scriptDir "scripts\codesys_import_external.py"
if (-not (Test-Path $pythonScript)) {
    Write-Host "Error: Import script not found: $pythonScript" -ForegroundColor Red
    exit 1
}

# Build arguments
$args = @($ProjectPath, $ImportDir)
if (-not [string]::IsNullOrEmpty($CodesysPath)) {
    $args += "--codesys-path"
    $args += $CodesysPath
}
$args += "--profile"
$args += $Profile
if ($DryRun) {
    $args += "--dry-run"
}

& python $pythonScript @args

if ($LASTEXITCODE -eq 0) {
    if ($DryRun) {
        Write-Host "`nDry run completed. No changes were made." -ForegroundColor Yellow
        Write-Host "Run without -DryRun to apply changes." -ForegroundColor Gray
    } else {
        Write-Host "`nImport completed successfully!" -ForegroundColor Green
    }
} else {
    Write-Host "`nImport failed with exit code: $LASTEXITCODE" -ForegroundColor Red
    exit $LASTEXITCODE
}

