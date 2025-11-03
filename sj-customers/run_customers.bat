@echo off
echo Starting Shopify Customer Data Validator...
echo.

REM Check if Python is installed
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Error: Python is not installed or not in PATH
    echo Please install Python and try again
    pause
    exit /b 1
)

REM Check if required packages are installed
python -c "import pandas, openpyxl" >nul 2>&1
if %errorlevel% neq 0 (
    echo Installing required packages...
    pip install pandas openpyxl
    if %errorlevel% neq 0 (
        echo Error: Failed to install required packages
        pause
        exit /b 1
    )
)

REM Run the application
echo Starting the application...
python run.py

pause
