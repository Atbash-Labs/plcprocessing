<#
.SYNOPSIS
    Export CODESYS project to text files (.st format)

.DESCRIPTION
    Exports POUs, GVLs, and methods from a CODESYS project to text files for version control.
    Can export directly from project (requires CODESYS) or from PLCOpenXML file.

.PARAMETER ProjectPath
    Path to the CODESYS .project file

.PARAMETER OutputDir
    Directory to export text files to

.PARAMETER FromXml
    Optional: Export from PLCOpenXML file instead of project (doesn't require CODESYS running)

.PARAMETER CodesysPath
    Optional: Path to CODESYS.exe (auto-detected if not specified)

.PARAMETER Profile
    Optional: CODESYS profile name (default: CODESYS V3.5 SP21 Patch 3)

.EXAMPLE
    .\codesys-export.ps1 -ProjectPath "MyProject.project" -OutputDir ".\export"
    
.EXAMPLE
    .\codesys-export.ps1 -FromXml "MyProject.xml" -OutputDir ".\export"
#>

param(
    [Parameter(Mandatory=$false)]
    [string]$ProjectPath,
    
    [Parameter(Mandatory=$true)]
    [string]$OutputDir,
    
    [Parameter(Mandatory=$false)]
    [string]$FromXml,
    
    [Parameter(Mandatory=$false)]
    [string]$CodesysPath = "",
    
    [Parameter(Mandatory=$false)]
    [string]$Profile = "CODESYS V3.5 SP21 Patch 3"
)

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

# Validate parameters
if ([string]::IsNullOrEmpty($ProjectPath) -and [string]::IsNullOrEmpty($FromXml)) {
    Write-Host "Error: Either -ProjectPath or -FromXml must be specified" -ForegroundColor Red
    exit 1
}

if (-not [string]::IsNullOrEmpty($FromXml)) {
    # Export from XML file
    if (-not (Test-Path $FromXml)) {
        Write-Host "Error: XML file not found: $FromXml" -ForegroundColor Red
        exit 1
    }
    
    Write-Host "=== CODESYS Export from XML ===" -ForegroundColor Cyan
    Write-Host "  Source XML: $FromXml" -ForegroundColor Gray
    Write-Host "  Output Dir: $OutputDir" -ForegroundColor Gray
    
    $pythonScript = Join-Path $scriptDir "scripts\codesys_export_from_xml.py"
    if (-not (Test-Path $pythonScript)) {
        Write-Host "Error: Export script not found: $pythonScript" -ForegroundColor Red
        exit 1
    }
    
    & python $pythonScript $FromXml $OutputDir
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "`nExport completed successfully!" -ForegroundColor Green
    } else {
        Write-Host "`nExport failed with exit code: $LASTEXITCODE" -ForegroundColor Red
        exit $LASTEXITCODE
    }
} else {
    # Export from project using CODESYS
    if (-not (Test-Path $ProjectPath)) {
        Write-Host "Error: Project file not found: $ProjectPath" -ForegroundColor Red
        exit 1
    }
    
    Write-Host "=== CODESYS Export from Project ===" -ForegroundColor Cyan
    Write-Host "  Project: $ProjectPath" -ForegroundColor Gray
    Write-Host "  Output Dir: $OutputDir" -ForegroundColor Gray
    Write-Host "  Profile: $Profile" -ForegroundColor Gray
    
    Write-Host "`nNote: Direct project export requires CODESYS to be installed." -ForegroundColor Yellow
    Write-Host "For offline export, first export project to PLCOpenXML from CODESYS," -ForegroundColor Yellow
    Write-Host "then use -FromXml parameter." -ForegroundColor Yellow
    
    # TODO: Implement direct project export via codesys_export.py
    Write-Host "`nDirect project export not yet implemented in driver script." -ForegroundColor Yellow
    Write-Host "Use: codesys_export.py inside CODESYS, or export to XML first." -ForegroundColor Yellow
}











