@echo off
REM ============================================================
REM  Build FlightOTPDashboard.exe for Windows 11
REM
REM  Prerequisites:
REM    pip install pyinstaller marimo plotly duckdb pyarrow faker pydantic
REM
REM  Output:
REM    dist\FlightOTPDashboard\FlightOTPDashboard.exe
REM ============================================================

echo.
echo  ========================================
echo   Building Flight OTP Dashboard .exe
echo  ========================================
echo.

cd /d "%~dp0"

REM Clean previous builds
if exist build rmdir /s /q build
if exist dist rmdir /s /q dist

echo  [1/2] Running PyInstaller ...
pyinstaller FlightOTPDashboard.spec --noconfirm

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo  ERROR: PyInstaller failed. See output above.
    pause
    exit /b 1
)

echo.
echo  [2/2] Build complete!
echo.
echo  Output: dist\FlightOTPDashboard\FlightOTPDashboard.exe
echo.
echo  To run:
echo    cd dist\FlightOTPDashboard
echo    FlightOTPDashboard.exe
echo.
echo  To distribute: zip the entire dist\FlightOTPDashboard folder.
echo.
pause
