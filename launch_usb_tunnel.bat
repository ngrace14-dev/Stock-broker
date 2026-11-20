@echo off
title Auto-Broker v3.7 Remote USB Tunnel Launcher
echo ============================================================================
echo   AUTO-BROKER TERMINAL v3.7 (DEFCON 0) - PORTABLE USB TUNNEL LAUNCHER
echo ============================================================================
echo.
echo Target Local Agent Server: http://127.0.0.1:8000
echo Protection: Personal Beta Password Gate active (REMOTE_AUTH_PASS in .env)
echo.

REM 1. Check if portable cloudflared.exe is on the same USB / folder as this script
if exist "%~dp0cloudflared.exe" (
    echo [USB] Found portable cloudflared.exe in current directory...
    echo [USB] Opening public HTTPS tunnel to http://127.0.0.1:8000 ...
    "%~dp0cloudflared.exe" tunnel --url http://127.0.0.1:8000
    goto :eof
)

REM 2. Check if cloudflared is installed on the host system PATH
where cloudflared >nul 2>nul
if %ERRORLEVEL% EQU 0 (
    echo [SYSTEM] Found cloudflared on PATH...
    echo [SYSTEM] Opening public HTTPS tunnel to http://127.0.0.1:8000 ...
    cloudflared tunnel --url http://127.0.0.1:8000
    goto :eof
)

REM 3. Fallback: Check if ngrok is on the USB or installed on the system PATH
if exist "%~dp0ngrok.exe" (
    echo [USB] Found portable ngrok.exe in current directory...
    "%~dp0ngrok.exe" http 8000
    goto :eof
)

where ngrok >nul 2>nul
if %ERRORLEVEL% EQU 0 (
    echo [SYSTEM] Found ngrok on PATH...
    ngrok http 8000
    goto :eof
)

echo [!] Neither cloudflared.exe nor ngrok.exe was found.
echo.
echo To make your USB flash drive 100%% portable (no GitHub needed):
echo   1. Download cloudflared-windows-amd64.exe from:
echo      https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-amd64.exe
echo   2. Rename it to "cloudflared.exe" and place it in the same folder as this .bat file on your USB.
echo   3. Double-click launch_usb_tunnel.bat to get your live HTTPS link!
echo.
pause
