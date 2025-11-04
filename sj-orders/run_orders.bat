@echo off
echo ====================================================
echo Shopify Order Data Validator
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
for /f "tokens=2" %%a in ('python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"') do set python_version=%%a
if %python_version% LSS 3.7 (
    echo Error: This application requires Python 3.7 or later
    echo Detected Python version: %python_version%
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
echo Starting the Order Data Validator...
echo ====================================================
echo.

python run.py

if %errorlevel% neq 0 (
    echo.
    echo Error: The application encountered an error
    echo Please check the console output for details
)

echo.
pause
