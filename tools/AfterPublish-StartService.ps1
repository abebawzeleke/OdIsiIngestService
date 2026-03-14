# Run AFTER publishing OdIsiIngestService
# Starts the service and verifies it's running

Start-Service -Name "OdIsiIngestService"
Start-Sleep -Seconds 2
$svc = Get-Service -Name "OdIsiIngestService"

if ($svc.Status -eq "Running") {
    Write-Host "OdIsiIngestService is running." -ForegroundColor Green
} else {
    Write-Host "WARNING: Service status is $($svc.Status). Check Event Viewer for errors." -ForegroundColor Red
}
