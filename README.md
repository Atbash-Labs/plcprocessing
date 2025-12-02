# CODESYS Scripting API Tools

A collection of Python tools for exporting, diffing, and applying changes to CODESYS projects using the **CODESYS Scripting API**. This approach solves the broken PLCOpenXML export/import cycle by using CODESYS's native API, which exports/imports correctly.

## Why Scripting API Instead of XML?

**Problem**: CODESYS V3.x GUI PLCOpenXML export/import is broken - exported XML files cannot be re-imported.

**Solution**: Use the CODESYS Scripting API which:
- ✅ Exports XML that actually works
- ✅ Direct project manipulation (no export/import cycle)
- ✅ Can run headless for CI/CD
- ✅ Full access to project structure
- ✅ Well-documented with active community

## Tools

### `codesys_export.py`
Exports CODESYS project to text format using Scripting API. Exports POUs and GVLs as `.st` files for source control and diffing.

**Usage (inside CODESYS):**
```
Tools → Execute Script → codesys_export.py
```

**Usage (headless):**
```bash
CODESYS.exe --noUI --runscript="codesys_export.py" "C:\Projects\MyProject.project" "C:\Export"
```

**Output Format:**
- POUs: `POU_NAME.prg.st`, `POU_NAME.fb.st`, `POU_NAME.fun.st`
- GVLs: `GVL_NAME.gvl.st`
- Each file contains declaration and implementation sections

### `codesys_import.py`
Imports text files back into CODESYS project using Scripting API. Reads `.st` files and applies them to the project.

**Usage (inside CODESYS):**
```
Tools → Execute Script → codesys_import.py
```

**Usage (headless):**
```bash
CODESYS.exe --noUI --runscript="codesys_import.py" "C:\Projects\MyProject.project" "C:\Import"
```

### `codesys_diff.py`
Compares two directories of CODESYS text exports and generates unified diffs for source control.

**Usage:**
```bash
python codesys_diff.py <export_dir1> <export_dir2> <diff_output_dir>
```

**Output:**
- `.diff` files for modified files
- `.added` files for new files
- `.removed` files for deleted files
- `diff_summary.txt` with summary

### `codesys_apply.py`
Applies diffs to CODESYS text exports and prepares them for import.

**Usage:**
```bash
python codesys_apply.py <diff_dir> <target_dir> <project_path> [output_dir]
```

**Workflow:**
1. Applies diffs to text files
2. Creates modified directory
3. Use `codesys_import.py` to import modified files back to project

## Complete Workflow Example

### 1. Export Project
```bash
# Inside CODESYS or headless
codesys_export.py "C:\Projects\MyProject.project" "C:\Repo\export1"
```

### 2. Make Changes (manually or via AI)
Edit the `.st` files or generate new code.

### 3. Export Modified Version
```bash
codesys_export.py "C:\Projects\MyProject.project" "C:\Repo\export2"
```

### 4. Generate Diff
```bash
python codesys_diff.py "C:\Repo\export1" "C:\Repo\export2" "C:\Repo\diffs"
```

### 5. Apply Diff to Another Project
```bash
# Apply diffs to text files
python codesys_apply.py "C:\Repo\diffs" "C:\Repo\export1" "C:\Projects\OtherProject.project" "C:\Repo\modified"

# Import modified files
codesys_import.py "C:\Projects\OtherProject.project" "C:\Repo\modified"
```

## Text File Format

### POU File (`PLC_PRG.prg.st`)
```
(* POU: PLC_PRG *)
(* Type: Program *)

(* DECLARATION *)
PROGRAM PLC_PRG
VAR
    i : INT;
END_VAR

(* IMPLEMENTATION *)
i := i + 1;
```

### GVL File (`GVL.gvl.st`)
```
(* GVL: GVL *)

VAR_GLOBAL
    counter : INT;
    status : BOOL;
END_VAR
```

## Requirements

- **CODESYS V3.5+** (for Scripting API)
- **Python 3.7+** (for diff/apply tools)
- Scripts run inside CODESYS using IronPython (export/import)
- Standard Python for diff/apply tools (can run anywhere)

## Installation

1. **CODESYS Scripting API**: Built into CODESYS V3.5+
2. **Python tools**: No installation needed, just run the scripts

## Headless Execution

Run scripts without opening CODESYS GUI:

```bash
"C:\Program Files\CODESYS\CODESYS.exe" ^
  --noUI ^
  --profile="CODESYS V3.5 SP21" ^
  --runscript="codesys_export.py" ^
  "C:\Projects\MyProject.project" ^
  "C:\Export"
```

## Integration with AI Tools

The text-based format is perfect for AI code generation:

1. **Export** project to `.st` files
2. **Send to AI** for modification/generation
3. **Receive modified** `.st` files
4. **Import** back to project using Scripting API

Example AI integration:
```python
# Export
codesys_export.py project.project export/

# AI modifies export/*.st files

# Import
codesys_import.py project.project export/
```

## Advantages Over XML Approach

✅ **Reliable**: Scripting API exports/imports actually work  
✅ **Simple**: Text files are easier to diff and merge  
✅ **Direct**: No XML parsing/restructuring needed  
✅ **Fast**: Direct project manipulation  
✅ **CI/CD Ready**: Can run headless  
✅ **AI-Friendly**: Text format is perfect for LLMs  

## Limitations

⚠️ **Requires CODESYS**: Scripts must run inside CODESYS (or headless)  
⚠️ **Structured Text Only**: Graphical languages (FBD, LD, SFC) not directly editable  
⚠️ **Windows Only**: CODESYS constraint  

## Legacy XML Tools

The following tools are deprecated but still available for reference:
- `plcopenxmlprocessor.py` - XML extraction
- `plcopenxmldiff.py` - XML diffing
- `plcopenxmlapply.py` - XML apply
- `plcopenxmlrestructure.py` - XML restructuring

**Recommendation**: Use the Scripting API tools (`codesys_*.py`) instead.

## License

MIT
