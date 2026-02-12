"""
Desktop launcher for the Flight OTP Dashboard.

Bundles the marimo dashboard and all source modules, starts the marimo
server on a free port, opens the default browser, and waits for the user
to close the console window (Ctrl+C or close button).

The launcher locates the system Python (with marimo installed) to run
the dashboard server.  The exe itself is a lightweight wrapper (~80 MB)
that carries the dashboard + source code as bundled data files.

Build:
    pyinstaller FlightOTPDashboard.spec --noconfirm

Run:
    dist\\FlightOTPDashboard\\FlightOTPDashboard.exe
"""
from __future__ import annotations

import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import webbrowser
from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find_free_port() -> int:
    """Find an available TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _resource_dir() -> Path:
    """
    Return the directory containing bundled resources.

    When running from a PyInstaller bundle the data files are extracted
    to ``sys._MEIPASS`` (--onefile) or sit next to the exe in
    ``_internal/`` (--onedir via COLLECT).
    When running from source, use the project root.
    """
    if getattr(sys, "frozen", False):
        # --onefile: sys._MEIPASS is the temp extraction dir
        # --onedir : sys._MEIPASS is <dist>/<name>/_internal
        return Path(sys._MEIPASS)
    return Path(__file__).resolve().parent


def _find_system_python() -> str:
    """
    Locate a Python interpreter that has marimo installed.

    Search order:
      1. ``py -3`` (Windows Launcher)
      2. ``python`` on PATH
      3. ``python3`` on PATH
      4. The Python that built this exe (stored in env at build time)
    """
    candidates: list[list[str]] = [
        ["py", "-3"],
        ["python"],
        ["python3"],
    ]

    # Also try the exact Python path that was used to build the exe
    build_python = os.environ.get("FLIGHT_OTP_PYTHON")
    if build_python:
        candidates.insert(0, [build_python])

    for cmd in candidates:
        try:
            result = subprocess.run(
                cmd + ["-c", "import marimo; print(marimo.__version__)"],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0:
                version = result.stdout.strip()
                python_path = cmd[0] if len(cmd) == 1 else " ".join(cmd)
                print(f"  Python    : {python_path}  (marimo {version})")
                return " ".join(cmd)  # return as single string for shell
        except (FileNotFoundError, subprocess.TimeoutExpired):
            continue

    print()
    print("  ERROR: Could not find a Python installation with marimo.")
    print()
    print("  Please install the required packages:")
    print("    pip install marimo plotly duckdb pyarrow faker pydantic")
    print()
    input("  Press Enter to exit ...")
    sys.exit(1)


def _prepare_workspace(resource_dir: Path) -> Path:
    """
    Copy the dashboard + src tree into a temporary workspace so that
    marimo can resolve ``src.*`` imports via ``sys.path.insert(0, ".")``.
    """
    workspace = Path(tempfile.mkdtemp(prefix="flight_otp_"))

    # Copy dashboard.py
    shutil.copy2(resource_dir / "dashboard.py", workspace / "dashboard.py")

    # Copy the src package tree
    src_src = resource_dir / "src"
    src_dst = workspace / "src"
    if src_src.is_dir():
        shutil.copytree(src_src, src_dst)

    return workspace


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

BANNER = r"""
  _____ _ _       _     _      ___  _____ ____
 |  ___| (_) __ _| |__ | |_   / _ \|_   _|  _ \
 | |_  | | |/ _` | '_ \| __| | | | | | | | |_) |
 |  _| | | | (_| | | | | |_  | |_| | | | |  __/
 |_|   |_|_|\__, |_| |_|\__|  \___/  |_| |_|
             |___/
  D A S H B O A R D
"""


def main() -> None:
    # Fix Windows console encoding for Unicode output
    if sys.platform == "win32":
        os.system("chcp 65001 >nul 2>&1")
        import io as _io
        sys.stdout = _io.TextIOWrapper(
            sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True
        )
        sys.stderr = _io.TextIOWrapper(
            sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True
        )

    print(BANNER)

    resource_dir = _resource_dir()
    python_cmd = _find_system_python()
    workspace = _prepare_workspace(resource_dir)
    port = _find_free_port()
    url = f"http://127.0.0.1:{port}"

    print(f"  Workspace : {workspace}")
    print(f"  Port      : {port}")
    print(f"  URL       : {url}")
    print()
    print("  Starting marimo server ...")
    print("  Press Ctrl+C or close this window to stop.\n")

    # Build the command to start marimo via the system Python
    cmd_str = (
        f"{python_cmd} -m marimo run "
        f"\"{workspace / 'dashboard.py'}\" "
        f"--host 127.0.0.1 --port {port} --headless"
    )

    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"

    proc = subprocess.Popen(
        cmd_str,
        cwd=str(workspace),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        shell=True,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0,
    )

    # Wait for the server to be ready, then open the browser
    ready = False
    for _ in range(60):  # up to 30 seconds
        time.sleep(0.5)
        if proc.poll() is not None:
            out = proc.stdout.read().decode("utf-8", errors="replace") if proc.stdout else ""
            print(f"  ERROR: marimo exited with code {proc.returncode}")
            if out.strip():
                print()
                for line in out.strip().splitlines():
                    print(f"    {line}")
            print()
            input("  Press Enter to exit ...")
            _cleanup(workspace)
            sys.exit(1)
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                ready = True
                break
        except OSError:
            continue

    if ready:
        print(f"  Server ready - opening {url}\n")
        webbrowser.open(url)
    else:
        print("  WARNING: Server did not respond in 30s, opening browser anyway ...\n")
        webbrowser.open(url)

    # Stream server output to console
    try:
        while proc.poll() is None:
            line = proc.stdout.readline() if proc.stdout else b""
            if line:
                decoded = line.decode("utf-8", errors="replace").rstrip()
                if decoded:
                    print(f"  [marimo] {decoded}")
            else:
                time.sleep(0.2)
    except KeyboardInterrupt:
        print("\n  Shutting down ...")

    # Graceful shutdown
    _shutdown(proc)
    _cleanup(workspace)
    print("  Done. Goodbye!")


def _shutdown(proc: subprocess.Popen) -> None:
    """Terminate the marimo server process."""
    if proc.poll() is not None:
        return
    try:
        if sys.platform == "win32":
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            proc.terminate()
        proc.wait(timeout=5)
    except (subprocess.TimeoutExpired, OSError):
        proc.kill()
        proc.wait(timeout=3)


def _cleanup(workspace: Path) -> None:
    """Remove the temporary workspace."""
    try:
        shutil.rmtree(workspace, ignore_errors=True)
    except Exception:
        pass


if __name__ == "__main__":
    main()
