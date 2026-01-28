$ErrorActionPreference = "Stop"

# Add Portable Node.js to PATH
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$NodePath = Join-Path $ScriptDir "..\node-v22.13.1-win-x64"
$env:PATH = "$NodePath;" + $env:PATH

Write-Host "Environment configured." -ForegroundColor Green
Write-Host "Node Version: $(node -v)"
Write-Host "Python Version: $(python --version)"

Write-Host "Starting Crew Dashboard Server..." -ForegroundColor Cyan
& python api_server.py
