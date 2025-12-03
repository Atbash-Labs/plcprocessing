# CODESYS DevOps Tools

A complete DevOps toolchain for CODESYS PLC projects: export, diff, merge, and import with full support for additions, modifications, and deletions.

## Why These Tools?

**Problem**: CODESYS V3.x GUI PLCOpenXML export/import is broken - exported XML files cannot be re-imported reliably.

**Solution**: Use the CODESYS Scripting API which:
- ✅ Direct project manipulation (no broken export/import cycle)
- ✅ Full text-based workflow for version control
- ✅ Supports additions, modifications, AND deletions
- ✅ Can run headless for CI/CD
- ✅ Authoritative imports (import directory is source of truth)

## Quick Start (PowerShell Driver Scripts)

### Export Project
```powershell
# From PLCOpenXML file (no CODESYS needed)
.\codesys-export.ps1 -FromXml "MyProject.xml" -OutputDir ".\export"
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
├── codesys-export.ps1    # Driver: Export project to text files
├── codesys-diff.ps1      # Driver: Generate diffs between exports
├── codesys-apply.ps1     # Driver: Apply diffs to exports
├── codesys-import.ps1    # Driver: Import text files to project
├── codesys-merge.ps1     # Driver: Complete merge workflow
├── scripts/              # Python implementation
│   ├── codesys_export.py           # Export (runs in CODESYS)
│   ├── codesys_export_from_xml.py  # Export from PLCOpenXML
│   ├── codesys_import.py           # Import (runs in CODESYS)
│   ├── codesys_import_external.py  # Import wrapper (headless)
│   ├── codesys_diff.py             # Generate unified diffs
│   └── codesys_apply.py            # Apply diffs to text files
├── legacy/               # Deprecated XML-based tools
├── tests/                # Test files and outputs
└── docs/                 # Documentation
```

## File Formats

### Exported Text Files (.st)

| Extension | Type |
|-----------|------|
| `NAME.prg.st` | Program |
| `NAME.fb.st` | Function Block |
| `NAME.fun.st` | Function |
| `NAME.gvl.st` | Global Variable List |
| `POU_METHOD.meth.st` | Method (in POU) |

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

## See Also

- [docs/import_instructions.md](docs/import_instructions.md) - Detailed import instructions
