# Install TGParser as a proper Windows Scheduled Task from an elevated shell.
# Requires Run as Administrator. Removes the old user-scope task and the
# Startup shortcut left over from v3 before installing the new one.
#
#   powershell -ExecutionPolicy Bypass -File .\scripts\install_service.ps1

$ErrorActionPreference = "Stop"

# 0. Admin check.
$id = [Security.Principal.WindowsIdentity]::GetCurrent()
$pr = New-Object Security.Principal.WindowsPrincipal $id
if (-not $pr.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw "This script must be run from an elevated PowerShell (Run as Administrator)."
}

$projRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$python   = Join-Path $projRoot ".venv\Scripts\pythonw.exe"
if (-not (Test-Path $python)) {
    throw "pythonw.exe not found at $python. Create the venv first."
}
$runpy    = Join-Path $projRoot "run.py"
$taskName = "TGParser"
$userName = "$env:USERDOMAIN\$env:USERNAME"

# 1. Remove v3 leftovers: old task and Startup shortcut.
Write-Host "Removing legacy startup mechanisms..."
try {
    if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
    }
} catch { }
$startup = [Environment]::GetFolderPath("Startup")
$lnk = Join-Path $startup "TGParser.lnk"
if (Test-Path $lnk) { Remove-Item $lnk -Force }

# 2. Build the task.
$action = New-ScheduledTaskAction `
    -Execute $python `
    -Argument "`"$runpy`"" `
    -WorkingDirectory $projRoot

# Triggers in ONE task.
#   1. AtStartup       - recover after reboot
#   2. Once (past)     - fire immediately AND repeat every minute forever;
#                        serves as the in-session crash-recovery mechanism.
#                        MultipleInstances=IgnoreNew prevents duplicates.
$t1 = New-ScheduledTaskTrigger -AtStartup
$past = (Get-Date).AddMinutes(-1)
$t2 = New-ScheduledTaskTrigger -Once -At $past `
    -RepetitionInterval (New-TimeSpan -Minutes 1)
$trigger = @($t1, $t2)

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -RestartCount 999 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -MultipleInstances IgnoreNew `
    -Hidden

$principal = New-ScheduledTaskPrincipal `
    -UserId $userName `
    -LogonType S4U `
    -RunLevel Highest

Register-ScheduledTask `
    -TaskName $taskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Description "TGParser 24/7 Telegram collector (v4)" | Out-Null

# 3. Start it and report.
Start-ScheduledTask -TaskName $taskName
Start-Sleep -Seconds 3
Get-ScheduledTask -TaskName $taskName |
    Select-Object TaskName, State,
        @{N="RunAs";E={$_.Principal.UserId}},
        @{N="LogonType";E={$_.Principal.LogonType}},
        @{N="RunLevel";E={$_.Principal.RunLevel}} |
    Format-List
Get-ScheduledTaskInfo -TaskName $taskName |
    Select-Object LastRunTime, LastTaskResult, NextRunTime, NumberOfMissedRuns |
    Format-List
Write-Host "Installed."
