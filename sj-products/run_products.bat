@echo off
echo ====================================================
echo Shopify Products Data Processor
echo ====================================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Error: Python is not installed or not in PATH
    echo Please install Python 3.7 or later and try again
    echo You can download Python from: https://www.python.org/downloads/
    pause
    exit /b 1
)

REM Check Python version (requires Python 3.7+)
python -c "import sys; exit(0 if sys.version_info >= (3, 7) else 1)"
if %ERRORLEVEL% NEQ 0 (
    echo Error: This application requires Python 3.7 or later
    python -c "import sys; print(f'Detected Python version: {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}')"
    echo Please upgrade your Python installation
    pause
    exit /b 1
)

REM Check if required packages are installed
echo Checking for required packages...
python -c "import pandas, openpyxl, xlrd" >nul 2>&1
if %errorlevel% neq 0 (
    echo Installing required packages (this may take a minute)...
    python -m pip install --upgrade pip
    python -m pip install pandas openpyxl xlrd --user
    if %errorlevel% neq 0 (
        echo Error: Failed to install required packages
        echo Please try running this script as administrator
        pause
        exit /b 1
    )
)

REM Set the working directory to the script's directory
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

REM Run the application
echo.
echo ====================================================
echo Starting the Products Data Processor...
echo ====================================================
echo.

python gui.py

if %errorlevel% neq 0 (
    echo.
    echo Error: The application encountered an error
    echo Please check the console output for details
)

echo.
pause
