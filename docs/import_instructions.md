# Import Instructions for CODESYS

## Method 1: Using CODESYS Scripting API (Recommended)

### Step 1: Open CODESYS
1. Open CODESYS IDE
2. Create a new project or open an existing one

### Step 2: Run Import Script
1. Go to **Tools → Execute Script**
2. Select `codesys_import.py`
3. Enter arguments:
   ```
   "C:\path\to\your\project.project" "C:\path\to\test_cross_applied_export"
   ```
4. Click **Execute**

### Step 3: Verify
- Check that `PLC_PRG` program appears
- Check that `GVL` with `SEVEN` variable appears
- Verify the code matches the exported files

## Method 2: Headless Execution

```bash
"C:\Program Files\CODESYS\CODESYS.exe" ^
  --noUI ^
  --profile="CODESYS V3.5 SP21" ^
  --runscript="codesys_import.py" ^
  "C:\Projects\MyProject.project" ^
  "C:\path\to\test_cross_applied_export"
```

## What Will Be Imported

From `test_cross_applied_export/`:
- **PLC_PRG.prg.st** → Creates/updates Program `PLC_PRG`
  - Implementation: `test test4; begin; i = i+1;`
  
- **GVL.gvl.st** → Creates/updates GVL `GVL`
  - Variables: `SEVEN: INT;`

## Notes

- If POUs/GVLs already exist, they will be **updated**
- If they don't exist, they will be **created**
- The project will be **saved automatically** after import

