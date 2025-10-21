@echo off
:: Batch file to set up and run the WooCommerce to Shopify converter
:: Created on %date% %time%

echo ========================================
echo  WooCommerce to Shopify Converter Setup
echo ========================================
echo.

:: Check if Python is installed
python --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Python is not installed or not in PATH
    echo Please install Python 3.8 or later from https://www.python.org/downloads/
    pause
    exit /b 1
)

:: Check if virtual environment exists
if not exist "env\" (
    echo Creating virtual environment...
    python -m venv env
    if %ERRORLEVEL% NEQ 0 (
        echo [ERROR] Failed to create virtual environment
        pause
        exit /b 1
    )
    echo Virtual environment created successfully.
) else (
    echo Virtual environment already exists.
)

:: Activate the virtual environment
echo Activating virtual environment...
call env\Scripts\activate.bat
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed to activate virtual environment
    pause
    exit /b 1
)

:: Install required packages
echo Installing required packages...
pip install -r requirements.txt
if %ERRORLEVEL% NEQ 0 (
    echo [WARNING] Failed to install some dependencies, but will continue...
)

:: Create a desktop shortcut (optional)
echo Creating desktop shortcut...
echo [InternetShortcut] > "%USERPROFILE%\Desktop\WooCommerce to Shopify Converter.url"
echo URL=file:///%~dp0gui.py >> "%USERPROFILE%\Desktop\WooCommerce to Shopify Converter.url"
echo IconIndex=0 >> "%USERPROFILE%\Desktop\WooCommerce to Shopify Converter.url"
echo IconFile=%~dp0env\Scripts\python.exe >> "%USERPROFILE%\Desktop\WooCommerce to Shopify Converter.url"

:: Run the application
echo Starting the application...
echo ========================================
echo  WooCommerce to Shopify Converter
echo ========================================
echo.
python gui.py

:: Keep the console open if there was an error
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo The application closed with an error. Press any key to exit...
    pause >nul
)