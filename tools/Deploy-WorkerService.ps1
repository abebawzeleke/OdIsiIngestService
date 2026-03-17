param(
    [string]$ServiceName = "OdIsiIngestService",
    [string]$ServiceDisplayName = "OdIsi Ingest Service",
    [string]$ServicePath = "C:\Services\OdIsiIngest",
    [string]$ExeName = "OdIsiIngestService.exe",
    [string]$ProjectDir = "C:\Users\axon4d-user\source\repos\OdIsiIngestService",
    [int]$MaxStatusCheckAttempts = 10,
    [int]$StatusCheckDelaySeconds = 3
)

$ErrorActionPreference = "Stop"

function Write-ColorOutput {
    param([string]$Message, [string]$Color = "White")
    Write-Host $Message -ForegroundColor $Color
}

function Test-ServiceExists {
    param([string]$Name)
    return (Get-Service -Name $Name -ErrorAction SilentlyContinue) -ne $null
}

function Wait-ForServiceStatus {
    param(
        [string]$Name,
        [string]$DesiredStatus,
        [int]$MaxAttempts,
        [int]$DelaySeconds
    )

    for ($i = 1; $i -le $MaxAttempts; $i++) {
        $svc = Get-Service -Name $Name -ErrorAction SilentlyContinue
        if ($svc -and $svc.Status.ToString() -eq $DesiredStatus) {
            return $true
        }
        Start-Sleep -Seconds $DelaySeconds
    }
    return $false
}

function Get-ServiceWmi {
    param([string]$Name)
    return Get-WmiObject -Class Win32_Service -Filter ("Name='{0}'" -f $Name) -ErrorAction SilentlyContinue
}

function Kill-ServiceProcessIfAny {
    param([string]$Name)

    $wmi = Get-ServiceWmi -Name $Name
    if ($wmi -and $wmi.ProcessId -and $wmi.ProcessId -gt 0) {
        try { Stop-Process -Id $wmi.ProcessId -Force -ErrorAction SilentlyContinue } catch { }
    }
}

function Ensure-ServiceEnvironment {
    param([string]$Name)

    $regPath = "HKLM:\SYSTEM\CurrentControlSet\Services\$Name"
    New-ItemProperty -Path $regPath -Name "Environment" -PropertyType MultiString -Value @(
        "ASPNETCORE_ENVIRONMENT=Production"
    ) -Force | Out-Null
}

function Ensure-ServiceRecovery {
    param([string]$Name)
    sc.exe failure $Name reset= 0 actions= restart/5000/restart/5000/restart/5000 | Out-Null
    sc.exe failureflag $Name 1 | Out-Null
}

function Test-ServiceHealth {
    param([string]$Name, [string]$Path, [string]$ExeName)

    Write-ColorOutput "`nPerforming health check..." "Cyan"

    # Check 1: Service exists
    $svc = Get-Service -Name $Name -ErrorAction SilentlyContinue
    if (-not $svc) {
        Write-ColorOutput "Service not found in Services." "Red"
        return $false
    }

    # Check 2: Status
    $statusColor = "Yellow"
    if ($svc.Status -eq "Running") { $statusColor = "Green" }
    if ($svc.Status -ne "Running") { $statusColor = "Red" }
    Write-ColorOutput ("Service Status: {0}" -f $svc.Status) $statusColor

    # Check 3: Executable exists
    $exePath = Join-Path $Path $ExeName
    if (-not (Test-Path $exePath)) {
        Write-ColorOutput ("Executable not found: {0}" -f $exePath) "Red"
        return $false
    }
    Write-ColorOutput ("Executable exists: {0}" -f $exePath) "Green"

    # Check 4: Registered path matches expected
    $wmi = Get-ServiceWmi -Name $Name
    if ($wmi) {
        $registeredPath = $wmi.PathName
        if ($registeredPath) {
            $registeredPath = $registeredPath.Trim()
            $expectedQuoted = ('"{0}"' -f $exePath)

            if (($registeredPath -ne $exePath) -and ($registeredPath -ne $expectedQuoted)) {
                Write-ColorOutput "WARNING: Service path differs from expected." "Yellow"
                Write-ColorOutput ("Registered: {0}" -f $registeredPath) "Yellow"
                Write-ColorOutput ("Expected:   {0}" -f $expectedQuoted) "Yellow"
            } else {
                Write-ColorOutput "Service is registered with correct path." "Green"
            }
        }
    }

    # Check 5: Process is running
    $wmi2 = Get-ServiceWmi -Name $Name
    if ($wmi2 -and $wmi2.ProcessId -gt 0) {
        $p = Get-Process -Id $wmi2.ProcessId -ErrorAction SilentlyContinue
        if (-not $p) {
            Write-ColorOutput "Service process not found (PID missing)." "Red"
            return $false
        }
        Write-ColorOutput ("Service process is running (PID={0})." -f $wmi2.ProcessId) "Green"
    } else {
        Write-ColorOutput "Service has no running PID." "Red"
        return $false
    }

    Write-ColorOutput "Health check passed." "Green"
    return $true
}

try {
    Write-ColorOutput "========================================" "Cyan"
    Write-ColorOutput "   OdIsiIngest Full Deploy (PS5.1)" "Cyan"
    Write-ColorOutput "========================================`n" "Cyan"

    Write-ColorOutput ("Service:     {0}" -f $ServiceName) "Yellow"
    Write-ColorOutput ("ServicePath: {0}" -f $ServicePath) "Yellow"
    Write-ColorOutput ("ProjectDir:  {0}" -f $ProjectDir) "Yellow"
    Write-ColorOutput ("ExeName:     {0}" -f $ExeName) "Yellow"

    # --------------------------------------------------------
    # Step 0: Git pull latest code (auto-stash local changes)
    # --------------------------------------------------------
    Write-ColorOutput "`nStep 0: Pull latest code..." "Cyan"
    Push-Location $ProjectDir

    # Stash any local changes (e.g. appsettings.json edits) so pull never fails
    # Use cmd /c to prevent PowerShell 5.1 from treating git stderr as errors
    $ErrorActionPreference = "Continue"
    $stashOutput = cmd /c "git stash 2>&1"
    $didStash = ($stashOutput | Out-String) -match "Saved working directory"
    if ($didStash) {
        Write-ColorOutput "Stashed local changes." "Yellow"
    }

    $gitOutput = cmd /c "git pull origin main 2>&1"
    $gitExitCode = $LASTEXITCODE
    $ErrorActionPreference = "Stop"
    if ($gitOutput) { Write-ColorOutput ($gitOutput -join "`n") "Gray" }
    if ($gitExitCode -ne 0) { Pop-Location; throw "git pull failed (exit code $gitExitCode)." }

    Pop-Location
    Write-ColorOutput "Code updated." "Green"

    # --------------------------------------------------------
    # Step 1: Stop service if running (BEFORE publish so files aren't locked)
    # --------------------------------------------------------
    Write-ColorOutput "`nStep 1: Stop service if running..." "Cyan"
    if (Test-ServiceExists -Name $ServiceName) {
        $svc = Get-Service -Name $ServiceName
        if ($svc.Status -eq "Running") {
            Stop-Service -Name $ServiceName -Force -ErrorAction SilentlyContinue
            $okStop = Wait-ForServiceStatus -Name $ServiceName -DesiredStatus "Stopped" -MaxAttempts $MaxStatusCheckAttempts -DelaySeconds $StatusCheckDelaySeconds
            if (-not $okStop) {
                Write-ColorOutput "Service did not stop cleanly; killing process..." "Yellow"
                Kill-ServiceProcessIfAny -Name $ServiceName
            }
        }
        Write-ColorOutput "Service stopped." "Green"
    } else {
        Write-ColorOutput "Service not found; will create it." "Yellow"
    }

    # --------------------------------------------------------
    # Step 2: Build and publish
    # --------------------------------------------------------
    Write-ColorOutput "`nStep 2: Build and publish..." "Cyan"
    Push-Location $ProjectDir
    $pubOutput = & dotnet publish -c Release -o $ServicePath 2>&1 | Out-String
    $pubExitCode = $LASTEXITCODE
    Write-ColorOutput $pubOutput.Trim() "Gray"
    if ($pubExitCode -ne 0) { Pop-Location; throw "dotnet publish failed (exit code $pubExitCode)." }
    Pop-Location
    Write-ColorOutput "Publish complete." "Green"

    $exePath = Join-Path $ServicePath $ExeName
    if (-not (Test-Path $exePath)) {
        throw ("Service executable not found after publish: {0}" -f $exePath)
    }
    Write-ColorOutput ("Executable found: {0}" -f $exePath) "Green"

    # --------------------------------------------------------
    # Step 3: Create/Configure service
    # --------------------------------------------------------
    Write-ColorOutput "`nStep 3: Create/Configure service..." "Cyan"
    if (-not (Test-ServiceExists -Name $ServiceName)) {
        sc.exe create $ServiceName binPath= ('"{0}"' -f $exePath) DisplayName= ('"{0}"' -f $ServiceDisplayName) start= auto | Out-Null
        if ($LASTEXITCODE -ne 0) { throw ("sc create failed. ExitCode={0}" -f $LASTEXITCODE) }
        Write-ColorOutput "Service created." "Green"
    } else {
        sc.exe config $ServiceName binPath= ('"{0}"' -f $exePath) DisplayName= ('"{0}"' -f $ServiceDisplayName) start= auto | Out-Null
        if ($LASTEXITCODE -ne 0) { throw ("sc config failed. ExitCode={0}" -f $LASTEXITCODE) }
        Write-ColorOutput "Service configuration ensured." "Green"
    }

    # --------------------------------------------------------
    # Step 4: Set environment + recovery
    # --------------------------------------------------------
    Write-ColorOutput "`nStep 4: Set environment + recovery..." "Cyan"
    Ensure-ServiceEnvironment -Name $ServiceName
    Ensure-ServiceRecovery -Name $ServiceName
    Write-ColorOutput "Environment + recovery configured." "Green"

    # --------------------------------------------------------
    # Step 5: Start service
    # --------------------------------------------------------
    Write-ColorOutput "`nStep 5: Start service..." "Cyan"
    Start-Service -Name $ServiceName
    $okStart = Wait-ForServiceStatus -Name $ServiceName -DesiredStatus "Running" -MaxAttempts $MaxStatusCheckAttempts -DelaySeconds $StatusCheckDelaySeconds
    if (-not $okStart) { throw "Service failed to reach Running state." }

    # --------------------------------------------------------
    # Step 6: Health check
    # --------------------------------------------------------
    $health = Test-ServiceHealth -Name $ServiceName -Path $ServicePath -ExeName $ExeName
    if (-not $health) { throw "Health check failed." }

    Write-ColorOutput "`n========================================" "Cyan"
    Write-ColorOutput "   DEPLOY SUCCESSFUL" "Green"
    Write-ColorOutput "========================================" "Cyan"
    Write-ColorOutput ("Service is running from: {0}" -f $ServicePath) "White"
}
catch {
    Write-ColorOutput "`n========================================" "Red"
    Write-ColorOutput "   DEPLOY FAILED!" "Red"
    Write-ColorOutput "========================================" "Red"
    Write-ColorOutput ("Error: {0}" -f $_.Exception.Message) "Red"
    exit 1
}
