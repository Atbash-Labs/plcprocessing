# PLC DevOps Tools

A complete DevOps toolchain for PLC projects supporting both **CODESYS** and **Rockwell/Allen-Bradley** platforms. Export, diff, merge, and import with full support for additions, modifications, and deletions.

## Why These Tools?

**Problem**: CODESYS V3.x GUI PLCOpenXML export/import is broken - exported XML files cannot be re-imported reliably.

**Solution**: Use the CODESYS Scripting API which:
- ✅ Direct project manipulation (no broken export/import cycle)
- ✅ Full text-based workflow for version control
- ✅ Supports additions, modifications, AND deletions
- ✅ Can run headless for CI/CD
- ✅ Authoritative imports (import directory is source of truth)

## Quick Start (PowerShell Driver Scripts)

### CODESYS Projects

#### Export Project
```powershell
# From PLCOpenXML file (no CODESYS needed)
.\codesys-export.ps1 -FromXml "MyProject.xml" -OutputDir ".\export"
```

### Rockwell/Allen-Bradley Projects

#### Export L5X Files
```powershell
# Export single L5X file
.\l5x-export.ps1 -Input "Motor_Control.L5X" -OutputDir ".\export"

# Export all L5X files in directory
.\l5x-export.ps1 -Input ".\PLC" -OutputDir ".\export"
```

### Generate Diff
```powershell
.\codesys-diff.ps1 -BaseDir ".\export_v1" -TargetDir ".\export_v2" -OutputDir ".\diffs"
```

### Apply Diff (Merge)
```powershell
.\codesys-apply.ps1 -DiffDir ".\diffs" -TargetDir ".\export" -OutputDir ".\merged"
```

### Import to Project
```powershell
# Preview changes first
.\codesys-import.ps1 -ProjectPath "MyProject.project" -ImportDir ".\merged" -DryRun

# Apply changes
.\codesys-import.ps1 -ProjectPath "MyProject.project" -ImportDir ".\merged"
```

### Complete Merge Workflow
```powershell
# One command to apply diffs and import
.\codesys-merge.ps1 -DiffDir ".\diffs" -TargetDir ".\export" -ProjectPath "MyProject.project" -DryRun
```

## Project Structure

```
.
├── codesys-export.ps1    # Driver: Export CODESYS project to text files
├── codesys-diff.ps1      # Driver: Generate diffs between exports
├── codesys-apply.ps1     # Driver: Apply diffs to exports
├── codesys-import.ps1    # Driver: Import text files to CODESYS project
├── codesys-merge.ps1     # Driver: Complete merge workflow
├── l5x-export.ps1        # Driver: Export Rockwell L5X files to text
├── scripts/              # Python implementation
│   ├── codesys_export.py           # Export (runs in CODESYS)
│   ├── codesys_export_from_xml.py  # Export from PLCOpenXML
│   ├── codesys_import.py           # Import (runs in CODESYS)
│   ├── codesys_import_external.py  # Import wrapper (headless)
│   ├── codesys_diff.py             # Generate unified diffs
│   ├── codesys_apply.py            # Apply diffs to text files
│   └── l5x_export.py               # Export Rockwell L5X files
├── PLC/                  # Sample Rockwell L5X files
├── legacy/               # Deprecated XML-based tools
├── tests/                # Test files and outputs
└── docs/                 # Documentation
```

## File Formats

### CODESYS Exported Text Files (.st)

| Extension | Type |
|-----------|------|
| `NAME.prg.st` | Program |
| `NAME.fb.st` | Function Block |
| `NAME.fun.st` | Function |
| `NAME.gvl.st` | Global Variable List |
| `POU_METHOD.meth.st` | Method (in POU) |

### Rockwell/Allen-Bradley Exported Files (.sc)

| Extension | Type |
|-----------|------|
| `NAME.aoi.sc` | Add-On Instruction |
| `NAME.udt.sc` | User Defined Type |

### Diff Files

| Extension | Meaning |
|-----------|---------|
| `*.diff` | Unified diff (modifications) |
| `*.added` | New file content |
| `*.removed` | Marker for deletion |
| `diff_summary.txt` | Summary statistics |

## Workflow Examples

### 1. Version Control Workflow
```powershell
# Export current state
.\codesys-export.ps1 -FromXml "Project.xml" -OutputDir ".\v1"

# Make changes in CODESYS, export again
.\codesys-export.ps1 -FromXml "Project_modified.xml" -OutputDir ".\v2"

# Generate diff for review/commit
.\codesys-diff.ps1 -BaseDir ".\v1" -TargetDir ".\v2" -OutputDir ".\changes"
```

### 2. Merge Changes from Branch
```powershell
# You have: diffs from feature branch, current project export
.\codesys-merge.ps1 `
    -DiffDir ".\feature_diffs" `
    -TargetDir ".\current_export" `
    -ProjectPath "MyProject.project" `
    -DryRun  # Preview first!
```

### 3. Add New Variables to GVL
```powershell
# Edit GVL.gvl.st in your text editor, then import
.\codesys-import.ps1 -ProjectPath "MyProject.project" -ImportDir ".\modified"
```

### 4. Export Rockwell L5X Files for Version Control
```powershell
# Export all L5X files from PLC directory
.\l5x-export.ps1 -Input ".\PLC" -OutputDir ".\export"

# Commit the .sc files to version control
git add export/
git commit -m "Add PLC logic from Rockwell project"
```

## Rockwell/Allen-Bradley L5X Export

The L5X export tool converts Rockwell Logix 5000 L5X files to structured code (.sc) format for version control and code review.

### What Gets Exported

- **Add-On Instructions (AOIs)**: Complete instruction definitions including parameters, local tags, and ladder logic
- **User Defined Types (UDTs)**: Custom data structures with member definitions
- **Ladder Logic**: RLL rungs with comments preserved
- **Structured Text**: ST code blocks (when present)

### Output Format

Each L5X file is exported to a subdirectory containing:

```
export/
├── Motor_Reversing/
│   ├── Motor_Reversing.aoi.sc      # Main AOI
│   ├── HMI_MotorControl.udt.sc     # Custom data type
│   ├── Error_Motor.udt.sc          # Error structure
│   └── HMIBitEnable.aoi.sc         # Dependency AOI
└── IO_DigitalInput/
    ├── IO_DigitalInput.aoi.sc
    └── HMI_DigitalInput.udt.sc
```

### .sc File Structure

```
(* AOI: Motor_Reversing *)
(* Type: AddOnInstruction *)
(* Revision: 1.0 *)
(* Vendor: De Clerck Arnaud *)
(* Description: Controls a reversing motor contactor *)

(* PARAMETERS *)
VAR_INPUT
    tInTimeout: INT;  // Timeout time
    bInEstop: BOOL;   // Estop
END_VAR

VAR_OUTPUT
    bOutCommandForward: BOOL;  // Output for motor relay forward
    bOutError: BOOL;           // Motor Error Exists
END_VAR

(* LOCAL TAGS *)
VAR
    bTemp: BOOL;
    TON_TimeOut: TIMER;
END_VAR

(* IMPLEMENTATION *)
(* ROUTINE: Logic [RLL] *)

// Rung 0: Inputs
OTU(bTemp);

// Rung 1: Read HMI input buttons
[EQU(mode,2) OTE(bTemp) ,HMIBitEnable(...) ];
```

### Benefits

- **Version Control**: Track changes to PLC logic over time
- **Code Review**: Review ladder logic in pull requests
- **Documentation**: Self-documenting with comments preserved
- **Diffing**: Use standard diff tools to compare versions
- **Search**: Grep through PLC logic to find specific tags/instructions

## Authoritative Import Behavior

The import is **authoritative** - the import directory is the source of truth:

- **POUs/GVLs not in import directory → DELETED from project**
- **Methods not in import directory → DELETED from their POU**
- **GVL content is fully replaced** (variables not in file are removed)

Use `--dry-run` to preview deletions before applying!

## Requirements

- CODESYS V3.5 SP21 or later
- Python 3.x
- PowerShell 5.1+ (for driver scripts)
- Windows (CODESYS is Windows-only)

## Python Scripts (Direct Usage)

### CODESYS
```bash
# Export from XML
python scripts/codesys_export_from_xml.py "Project.xml" "output_dir"

# Generate diff
python scripts/codesys_diff.py "dir1" "dir2" "diff_output"

# Apply diff
python scripts/codesys_apply.py "diff_dir" "target_dir" "project.project" "output_dir"

# Import (runs CODESYS headless)
python scripts/codesys_import_external.py "Project.project" "import_dir" --dry-run
```

### Rockwell/Allen-Bradley
```bash
# Export L5X file
python scripts/l5x_export.py "Motor_Control.L5X" "output_dir"

# Export all L5X files in directory
python scripts/l5x_export.py "PLC" "output_dir"
```

## See Also

- [docs/import_instructions.md](docs/import_instructions.md) - Detailed import instructions
