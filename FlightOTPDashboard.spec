# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller spec for Flight OTP Dashboard.

Builds a single-folder distribution that includes:
  - launcher.py  (the entry-point)
  - dashboard.py (the marimo notebook)
  - src/         (all pipeline source modules)

The exe launches a local marimo server and opens the browser.

Build command:
    pyinstaller FlightOTPDashboard.spec --noconfirm
"""

import os
from pathlib import Path

block_cipher = None
ROOT = Path(SPECPATH)

a = Analysis(
    [str(ROOT / "launcher.py")],
    pathex=[str(ROOT)],
    binaries=[],
    datas=[
        # Bundle the marimo dashboard notebook
        (str(ROOT / "dashboard.py"), "."),
        # Bundle the entire src package tree
        (str(ROOT / "src"), "src"),
    ],
    hiddenimports=[
        # --- Core pipeline deps ---
        "duckdb",
        "pyarrow",
        "pyarrow.json",
        "pyarrow.lib",
        "pydantic",
        "faker",
        # --- marimo + its deps ---
        "marimo",
        "plotly",
        "plotly.express",
        "plotly.graph_objects",
        # --- src modules (ensure they're found) ---
        "src",
        "src.generators",
        "src.generators.models",
        "src.generators.flight_generator",
        "src.streaming",
        "src.streaming.json_stream",
        "src.ingestion",
        "src.ingestion.arrow_loader",
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        # Trim unnecessary weight
        "tkinter",
        "matplotlib",
        "scipy",
        "IPython",
        "notebook",
        "jupyter",
        "pyiceberg",
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="FlightOTPDashboard",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,          # Keep console so user sees status + can Ctrl+C
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="FlightOTPDashboard",
)
