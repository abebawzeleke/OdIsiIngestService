# Full deploy script for OdIsiIngestService
# Run from: C:\Users\axon4d-user\source\repos\OdIsiIngestService
# Requires: Admin PowerShell

Write-Host "=== Stopping service ===" -ForegroundColor Yellow
Stop-Service -Name "OdIsiIngestService" -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

Write-Host "=== Publishing ===" -ForegroundColor Yellow
dotnet publish -c Release -o "C:\Services\OdIsiIngest"

Write-Host "=== Starting service ===" -ForegroundColor Yellow
Start-Service -Name "OdIsiIngestService"
Start-Sleep -Seconds 2

$svc = Get-Service -Name "OdIsiIngestService"
if ($svc.Status -eq "Running") {
    Write-Host "=== Deploy complete. Service is running. ===" -ForegroundColor Green
} else {
    Write-Host "=== WARNING: Service status is $($svc.Status). Check Event Viewer. ===" -ForegroundColor Red
}
