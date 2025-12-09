#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Generate semantic ontologies from L5X/SC files using Claude AI

.DESCRIPTION
    Analyzes PLC structured code (.sc) files and generates semantic ontologies
    that explain tag meanings, relationships, control patterns, and data flows.

.PARAMETER Input
    Path to .sc file or directory containing .sc files

.PARAMETER OutputDir
    Directory where ontology JSON files will be saved

.PARAMETER Pattern
    File pattern for directory mode (default: *.aoi.sc)

.PARAMETER Verbose
    Enable verbose output

.EXAMPLE
    .\l5x-ontology.ps1 -Input "export_l5x\IO_DigitalInput\IO_DigitalInput.aoi.sc" -OutputDir ".\ontologies"
    Analyze a single SC file

.EXAMPLE
    .\l5x-ontology.ps1 -Input ".\export_l5x" -OutputDir ".\ontologies"
    Analyze all AOI files in export directory
#>

param(
    [Parameter(Mandatory=$true)]
    [string]$Input,

    [Parameter(Mandatory=$true)]
    [string]$OutputDir,

    [string]$Pattern = "*.aoi.sc",

    [switch]$Verbose
)

$ErrorActionPreference = "Stop"

# Get script directory
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$pythonScript = Join-Path $scriptDir "scripts\ontology_analyzer.py"

# Validate inputs
if (-not (Test-Path $Input)) {
    Write-Error "Input path not found: $Input"
    exit 1
}

if (-not (Test-Path $pythonScript)) {
    Write-Error "Python script not found: $pythonScript"
    exit 1
}

# Check for .env file with API key
$envFile = Join-Path $scriptDir ".env"
if (-not (Test-Path $envFile)) {
    Write-Warning ".env file not found. Make sure ANTHROPIC_API_KEY is set in environment."
}

# Check for Python
try {
    $pythonVersion = python3 --version 2>&1
    Write-Host "[INFO] Using $pythonVersion" -ForegroundColor Cyan
} catch {
    try {
        $pythonVersion = python --version 2>&1
        Write-Host "[INFO] Using $pythonVersion" -ForegroundColor Cyan
    } catch {
        Write-Error "Python not found. Please install Python 3.x"
        exit 1
    }
}

# Create output directory
if (-not (Test-Path $OutputDir)) {
    New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
    Write-Host "[INFO] Created output directory: $OutputDir" -ForegroundColor Cyan
}

# Build command arguments
$args = @($pythonScript, $Input, "-o", (Join-Path $OutputDir "ontology.json"), "-p", $Pattern)
if ($Verbose) {
    $args += "-v"
}

# Run analysis
Write-Host "`n[INFO] Starting ontology analysis..." -ForegroundColor Cyan
Write-Host "[INFO] Input: $Input" -ForegroundColor Cyan
Write-Host "[INFO] Output: $OutputDir" -ForegroundColor Cyan
Write-Host "[INFO] Pattern: $Pattern" -ForegroundColor Cyan
Write-Host ""

try {
    python3 @args

    if ($LASTEXITCODE -eq 0) {
        Write-Host "`n[SUCCESS] Ontology analysis completed!" -ForegroundColor Green
        Write-Host "[INFO] Results saved to: $OutputDir" -ForegroundColor Cyan
    } else {
        Write-Error "Analysis failed with exit code $LASTEXITCODE"
        exit $LASTEXITCODE
    }
} catch {
    Write-Error "Analysis failed: $_"
    exit 1
}
