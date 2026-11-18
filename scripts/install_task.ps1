# Install TGParser as a Windows Scheduled Task running without an interactive
# window, starting at boot, restarting on failure.
# Run from the project root in an elevated PowerShell:
#   powershell -ExecutionPolicy Bypass -File .\scripts\install_task.ps1

$ErrorActionPreference = "Stop"

$projRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$python   = Join-Path $projRoot ".venv\Scripts\pythonw.exe"   # windowless host
if (-not (Test-Path $python)) {
    $python = Join-Path $projRoot ".venv\Scripts\python.exe"
}
$runpy    = Join-Path $projRoot "run.py"
$taskName = "TGParser"
$userName = "$env:USERDOMAIN\$env:USERNAME"

$action   = New-ScheduledTaskAction -Execute $python -Argument "`"$runpy`"" `
              -WorkingDirectory $projRoot

$triggers = @(
    New-ScheduledTaskTrigger -AtLogOn -User $userName
    New-ScheduledTaskTrigger -AtStartup
)

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Seconds 0) `
    -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) `
    -MultipleInstances IgnoreNew

$principal = New-ScheduledTaskPrincipal -UserId $userName -LogonType Interactive `
    -RunLevel Limited

if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
}

Register-ScheduledTask -TaskName $taskName -Action $action `
    -Trigger $triggers -Settings $settings -Principal $principal `
    -Description "TGParser 24/7 collector" | Out-Null

Start-ScheduledTask -TaskName $taskName
Write-Host "Task '$taskName' installed and started."
Get-ScheduledTask -TaskName $taskName | Format-Table TaskName, State, LastRunTime, LastTaskResult
