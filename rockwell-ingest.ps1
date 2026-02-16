#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Process Rockwell/Allen-Bradley PLC files into the ontology database

.DESCRIPTION
    Unified handler for all Rockwell PLC file formats:
    - .L5X  (Studio 5000 XML export — component or full project)
    - .L5K  (Studio 5000 / RSLogix 5000 ASCII text project)
    - .ACD  (Studio 5000 native binary project — requires acd-tools)
    - .sc   (Pre-exported structured code files)

    Auto-detects the file format and routes to the appropriate parser.
    Can process single files or entire directories containing mixed formats.

.PARAMETER Input
    Path to a Rockwell PLC file (.L5X, .L5K, .ACD, .sc) or directory

.PARAMETER OutputDir
    Directory where exported .sc files and ontology results will be saved

.PARAMETER ExportOnly
    Only export to .sc files without running ontology analysis

.PARAMETER SkipAI
    Skip AI analysis (import structure only, mark as pending)

.PARAMETER Verbose
    Enable verbose output

.EXAMPLE
    .\rockwell-ingest.ps1 -Input "Motor_Control.L5X" -OutputDir ".\export"
    Process a single L5X file

.EXAMPLE
    .\rockwell-ingest.ps1 -Input "project.L5K" -OutputDir ".\export"
    Process an L5K project file

.EXAMPLE
    .\rockwell-ingest.ps1 -Input "project.ACD" -OutputDir ".\export"
    Process an ACD binary project file

.EXAMPLE
    .\rockwell-ingest.ps1 -Input ".\PLC_Files" -OutputDir ".\export"
    Process all Rockwell files in a directory (L5X, L5K, ACD)

.EXAMPLE
    .\rockwell-ingest.ps1 -Input "project.L5K" -ExportOnly -OutputDir ".\export"
    Export L5K to .sc files without ontology analysis

.EXAMPLE
    .\rockwell-ingest.ps1 -Input "project.L5X" -OutputDir ".\export" -SkipAI
    Import structure into Neo4j without AI analysis (for incremental mode)
#>

param(
    [Parameter(Mandatory=$true)]
    [string]$Input,

    [Parameter(Mandatory=$true)]
    [string]$OutputDir,

    [switch]$ExportOnly,
    [switch]$SkipAI,
    [switch]$Verbose
)

$ErrorActionPreference = "Stop"

# Get script directory
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$scriptsDir = Join-Path $scriptDir "scripts"

# Determine which Python scripts to use
$rockwellExport = Join-Path $scriptsDir "rockwell_export.py"
$ontologyAnalyzer = Join-Path $scriptsDir "ontology_analyzer.py"

# Validate inputs
if (-not (Test-Path $Input)) {
    Write-Error "Input path not found: $Input"
    exit 1
}

if (-not (Test-Path $rockwellExport)) {
    Write-Error "Python script not found: $rockwellExport"
    exit 1
}

# Check for Python
$pythonCmd = $null
try {
    $pythonVersion = python3 --version 2>&1
    $pythonCmd = "python3"
    Write-Host "[INFO] Using $pythonVersion" -ForegroundColor Cyan
} catch {
    try {
        $pythonVersion = python --version 2>&1
        $pythonCmd = "python"
        Write-Host "[INFO] Using $pythonVersion" -ForegroundColor Cyan
    } catch {
        Write-Error "Python not found. Please install Python 3.x"
        exit 1
    }
}

# Check for .env file
$envFile = Join-Path $scriptDir ".env"
if (-not $ExportOnly -and -not (Test-Path $envFile)) {
    Write-Warning ".env file not found. Make sure ANTHROPIC_API_KEY is set in environment for AI analysis."
}

# Detect file format
$inputItem = Get-Item $Input
$extension = $inputItem.Extension.ToLower()
$isDirectory = $inputItem.PSIsContainer

if ($isDirectory) {
    $formatDesc = "Directory (all Rockwell formats)"
    $fileCount = @(
        (Get-ChildItem -Path $Input -Recurse -Include "*.L5X","*.l5x","*.L5K","*.l5k","*.ACD","*.acd" -ErrorAction SilentlyContinue)
    ).Count
    Write-Host "[INFO] Found $fileCount Rockwell PLC file(s) in directory" -ForegroundColor Cyan
} else {
    switch ($extension) {
        ".l5x" { $formatDesc = "L5X (Studio 5000 XML)" }
        ".l5k" { $formatDesc = "L5K (Legacy ASCII)" }
        ".acd" { $formatDesc = "ACD (Native Binary)" }
        ".sc"  { $formatDesc = "SC (Pre-exported)" }
        default { $formatDesc = "Unknown ($extension)" }
    }
}

# Create output directory
if (-not (Test-Path $OutputDir)) {
    New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
    Write-Host "[INFO] Created output directory: $OutputDir" -ForegroundColor Cyan
}

# Display banner
Write-Host ""
Write-Host "========================================" -ForegroundColor Blue
Write-Host "  Rockwell PLC Ontology Processor" -ForegroundColor Blue
Write-Host "========================================" -ForegroundColor Blue
Write-Host "[INFO] Input:   $Input" -ForegroundColor Cyan
Write-Host "[INFO] Format:  $formatDesc" -ForegroundColor Cyan
Write-Host "[INFO] Output:  $OutputDir" -ForegroundColor Cyan
if ($ExportOnly) {
    Write-Host "[INFO] Mode:    Export Only (.sc files)" -ForegroundColor Yellow
} elseif ($SkipAI) {
    Write-Host "[INFO] Mode:    Structure Import (no AI)" -ForegroundColor Yellow
} else {
    Write-Host "[INFO] Mode:    Full Analysis (with AI)" -ForegroundColor Yellow
}
Write-Host ""

if ($ExportOnly) {
    # Export-only mode: convert to .sc files
    Write-Host "[INFO] Exporting to .sc format..." -ForegroundColor Cyan
    try {
        & $pythonCmd $rockwellExport $Input $OutputDir

        if ($LASTEXITCODE -eq 0) {
            Write-Host "`n[SUCCESS] Export completed!" -ForegroundColor Green
            Write-Host "[INFO] .sc files saved to: $OutputDir" -ForegroundColor Cyan
            Write-Host "[INFO] Run without -ExportOnly to analyze with AI" -ForegroundColor Cyan
        } else {
            Write-Error "Export failed with exit code $LASTEXITCODE"
            exit $LASTEXITCODE
        }
    } catch {
        Write-Error "Export failed: $_"
        exit 1
    }
} else {
    # Full ontology analysis mode
    Write-Host "[INFO] Starting ontology analysis..." -ForegroundColor Cyan

    $analyzerArgs = @($ontologyAnalyzer, $Input, "--rockwell")
    if ($SkipAI) {
        $analyzerArgs += "--skip-ai"
    }
    if ($Verbose) {
        $analyzerArgs += "-v"
    }

    try {
        & $pythonCmd @analyzerArgs

        if ($LASTEXITCODE -eq 0) {
            Write-Host "`n[SUCCESS] Ontology analysis completed!" -ForegroundColor Green
            if ($SkipAI) {
                Write-Host "[INFO] Structure imported. Use incremental analyzer for AI enrichment." -ForegroundColor Cyan
            } else {
                Write-Host "[INFO] Results stored in Neo4j ontology database." -ForegroundColor Cyan
            }
        } else {
            Write-Error "Analysis failed with exit code $LASTEXITCODE"
            exit $LASTEXITCODE
        }
    } catch {
        Write-Error "Analysis failed: $_"
        exit 1
    }
}
