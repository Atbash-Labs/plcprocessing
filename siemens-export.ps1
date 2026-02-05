#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Parse Siemens TIA Portal / AX Structured Text (.st) files and display summaries

.DESCRIPTION
    Parses Siemens .st source files and extracts classes (FBs), types (UDTs),
    programs, and configurations into a structured format compatible with the
    ontology analysis pipeline.

    Can process a single .st file or an entire directory.

.PARAMETER Input
    Path to .st file or directory containing .st files

.PARAMETER Analyze
    If set, also run ontology analysis (requires ANTHROPIC_API_KEY)

.PARAMETER Pattern
    File pattern for directory mode (default: *.st)

.PARAMETER Verbose
    Enable verbose output

.EXAMPLE
    .\siemens-export.ps1 -Input "conveyorControl.st"
    Parse a single Siemens ST file

.EXAMPLE
    .\siemens-export.ps1 -Input ".\siemens_project"
    Parse all .st files in a directory

.EXAMPLE
    .\siemens-export.ps1 -Input ".\siemens_project" -Analyze
    Parse and run ontology analysis on all .st files
#>

param(
    [Parameter(Mandatory=$true)]
    [string]$Input,

    [switch]$Analyze,

    [string]$Pattern = "*.st",

    [switch]$Verbose
)

$ErrorActionPreference = "Stop"

# Get script directory
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$parserScript = Join-Path $scriptDir "scripts\siemens_parser.py"
$analyzerScript = Join-Path $scriptDir "scripts\ontology_analyzer.py"

# Validate inputs
if (-not (Test-Path $Input)) {
    Write-Error "Input path not found: $Input"
    exit 1
}

if (-not (Test-Path $parserScript)) {
    Write-Error "Python script not found: $parserScript"
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

# Run parser
Write-Host "`n[INFO] Starting Siemens ST parsing..." -ForegroundColor Cyan
Write-Host "[INFO] Input: $Input" -ForegroundColor Cyan
Write-Host ""

try {
    python $parserScript $Input

    if ($LASTEXITCODE -eq 0) {
        Write-Host "`n[SUCCESS] Parsing completed!" -ForegroundColor Green
    } else {
        Write-Error "Parsing failed with exit code $LASTEXITCODE"
        exit $LASTEXITCODE
    }
} catch {
    Write-Error "Parsing failed: $_"
    exit 1
}

# Optionally run ontology analysis
if ($Analyze) {
    if (-not (Test-Path $analyzerScript)) {
        Write-Error "Analyzer script not found: $analyzerScript"
        exit 1
    }

    # Check for .env file with API key
    $envFile = Join-Path $scriptDir ".env"
    if (-not (Test-Path $envFile)) {
        Write-Warning ".env file not found. Make sure ANTHROPIC_API_KEY is set in environment."
    }

    Write-Host "`n[INFO] Starting ontology analysis..." -ForegroundColor Cyan

    $analyzerArgs = @($analyzerScript, $Input, "-p", $Pattern, "--siemens")
    if ($Verbose) {
        $analyzerArgs += "-v"
    }

    try {
        python @analyzerArgs

        if ($LASTEXITCODE -eq 0) {
            Write-Host "`n[SUCCESS] Ontology analysis completed!" -ForegroundColor Green
        } else {
            Write-Error "Analysis failed with exit code $LASTEXITCODE"
            exit $LASTEXITCODE
        }
    } catch {
        Write-Error "Analysis failed: $_"
        exit 1
    }
}
