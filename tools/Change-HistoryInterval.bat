@echo off
title Change History Save Interval
echo ========================================
echo    Change History Save Interval
echo ========================================
echo.
echo Current setting is in appsettings.json
echo at C:\Services\OdIsiIngest
echo.
echo Common values:
echo   1  = every 1 minute  (5,760 rows/day)
echo   5  = every 5 minutes (1,152 rows/day)
echo   10 = every 10 minutes  (576 rows/day)
echo   15 = every 15 minutes  (384 rows/day)
echo.
set /p INTERVAL="Enter new interval in minutes: "

if "%INTERVAL%"=="" (
    echo No value entered. Exiting.
    pause
    exit /b 1
)

echo.
echo Setting interval to %INTERVAL% minute(s)...
echo.

PowerShell -ExecutionPolicy Bypass -Command ^
  "$settingsPath = 'C:\Services\OdIsiIngest\appsettings.json'; " ^
  "$repoPath = 'C:\Users\axon4d-user\source\repos\OdIsiIngestService\appsettings.json'; " ^
  "" ^
  "function Update-Interval($path, $interval) { " ^
  "  if (-not (Test-Path $path)) { Write-Host ('File not found: ' + $path) -ForegroundColor Red; return $false } " ^
  "  $json = Get-Content $path -Raw | ConvertFrom-Json; " ^
  "  if ($json.PSObject.Properties['ODISI_HISTORY_WRITE_INTERVAL_MINUTES']) { " ^
  "    $json.ODISI_HISTORY_WRITE_INTERVAL_MINUTES = $interval " ^
  "  } else { " ^
  "    $json | Add-Member -NotePropertyName 'ODISI_HISTORY_WRITE_INTERVAL_MINUTES' -NotePropertyValue $interval " ^
  "  } " ^
  "  $json | ConvertTo-Json -Depth 10 | Set-Content $path -Encoding UTF8; " ^
  "  Write-Host ('Updated: ' + $path) -ForegroundColor Green; " ^
  "  return $true " ^
  "} " ^
  "" ^
  "$ok1 = Update-Interval $settingsPath '%INTERVAL%'; " ^
  "$ok2 = Update-Interval $repoPath '%INTERVAL%'; " ^
  "" ^
  "if ($ok1) { " ^
  "  Write-Host ''; " ^
  "  Write-Host 'Restarting OdIsiIngestService...' -ForegroundColor Yellow; " ^
  "  Stop-Service -Name 'OdIsiIngestService' -Force -ErrorAction SilentlyContinue; " ^
  "  Start-Sleep -Seconds 3; " ^
  "  Start-Service -Name 'OdIsiIngestService'; " ^
  "  Start-Sleep -Seconds 2; " ^
  "  $svc = Get-Service -Name 'OdIsiIngestService'; " ^
  "  if ($svc.Status -eq 'Running') { " ^
  "    Write-Host ('Service is running. History interval = ' + '%INTERVAL%' + ' minute(s).') -ForegroundColor Green " ^
  "  } else { " ^
  "    Write-Host ('WARNING: Service status is ' + $svc.Status) -ForegroundColor Red " ^
  "  } " ^
  "} "

echo.
echo ========================================
echo    Done
echo ========================================
echo.
pause
