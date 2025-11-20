# PLCopen XML Diff and Apply Tools

This toolkit allows you to compare PLCopen XML files and apply diffs to other XML files.

## Tools

1. **plcopenxmldiff.py** - Compare two XML files and generate diffs
2. **plcopenxmlapply.py** - Apply diffs to a target XML file
3. **plcopenxmlmerge.py** - Merge modified .sc files back into XML (used by apply)

## Usage

### Step 1: Generate Diffs

Compare two XML files to see what changed:

```bash
python plcopenxmldiff.py file1.xml file2.xml [output_dir]
```

This will:
- Extract all POUs, Methods, and GVLs from both files
- Generate unified diffs for files that changed
- Create `.diff` files for modified files
- Create `.added` files for new files
- Create `.removed` files for deleted files
- Generate a summary in `diff_summary.txt`

Example:
```bash
python plcopenxmldiff.py old_version.xml new_version.xml diffs/
```

### Step 2: Apply Diffs

Apply the diffs to a third XML file:

```bash
python plcopenxmlapply.py diff_dir/ target.xml output.xml
```

This will:
- Extract code from the target XML
- Apply all diffs from the diff directory
- Merge the changes back into a new XML file

Example:
```bash
python plcopenxmlapply.py diffs/ production.xml updated_production.xml
```

## Workflow Example

```bash
# 1. Compare development version with release version
python plcopenxmldiff.py dev.xml release.xml release_diffs/

# 2. Apply those changes to production
python plcopenxmlapply.py release_diffs/ production.xml production_updated.xml

# 3. Review the updated XML file
# production_updated.xml now contains the changes from release applied to production
```

## File Structure

The diff tool creates:
- `*.diff` - Unified diff format for modified files
- `*.added` - New files that were added
- `*.removed` - Files that were removed
- `diff_summary.txt` - Summary of all changes

## Notes

- The tools extract code to `.sc` files for comparison
- POUs are extracted as `POU_NAME.sc`
- Methods are extracted as `POU_NAME_METHOD_NAME.sc`
- GVLs are extracted as `GVL_NAME.sc`
- The apply tool automatically merges changes back into XML format

