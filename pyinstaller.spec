# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller spec file for the PLC Ontology Assistant Python backend.

Produces a one-folder distribution at  build/python-backend/
containing dispatcher.exe and all required scripts + dependencies.

Usage:
    pyinstaller pyinstaller.spec
"""

import os
from pathlib import Path

block_cipher = None

# Collect every .py file in scripts/ as data (they are executed at runtime
# via runpy.run_path inside the dispatcher).
scripts_dir = os.path.join(SPECPATH, 'scripts')
script_files = [
    (os.path.join(scripts_dir, f), 'scripts')
    for f in os.listdir(scripts_dir)
    if f.endswith('.py') and f != 'dispatcher.py'
]

a = Analysis(
    [os.path.join('scripts', 'dispatcher.py')],
    pathex=[scripts_dir],
    binaries=[],
    datas=script_files,
    # Hidden imports: packages that PyInstaller can't discover because they
    # are only referenced inside the data-file scripts (loaded via runpy).
    hiddenimports=[
        # --- Third-party (from requirements.txt) ---
        'neo4j',
        'neo4j.api',
        'neo4j._sync.driver',
        'neo4j._sync.work',
        'neo4j._async.driver',
        'neo4j._codec.hydration',
        'neo4j._codec.packstream',
        'anthropic',
        'anthropic._client',
        'anthropic.resources',
        'anthropic.types',
        'dotenv',
        'lxml',
        'lxml.etree',
        'lxml._elementpath',
        'pyodbc',
        # --- Stdlib modules used by scripts ---
        'json',
        'argparse',
        'pathlib',
        'dataclasses',
        'enum',
        'contextlib',
        'typing',
        'datetime',
        're',
        'html',
        'difflib',
        'shutil',
        'tempfile',
        'xml.etree.ElementTree',
        # --- httpx / httpcore (transitive dep of anthropic) ---
        'httpx',
        'httpcore',
        'httpcore._async',
        'httpcore._sync',
        'h11',
        'certifi',
        'idna',
        'sniffio',
        'anyio',
        'anyio._backends',
        'anyio._backends._asyncio',
        'socksio',
        'hpack',
        'hyperframe',
        # --- pydantic (transitive dep of anthropic) ---
        'pydantic',
        'pydantic.deprecated',
        'pydantic_core',
        'annotated_types',
        'typing_extensions',
        # --- other transitive ---
        'distro',
        'jiter',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        # CODESYS-only modules (won't be available outside CODESYS IDE)
        'scriptengine',
    ],
    noarchive=False,
    optimize=0,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='dispatcher',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,  # must be True – scripts write to stdout/stderr
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='python-backend',
)
