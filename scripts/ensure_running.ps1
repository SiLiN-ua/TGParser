# Idempotent watchdog: start TGParser if not already running.
# Safe to invoke every minute from Task Scheduler.
$proj = Split-Path -Parent $PSScriptRoot   # repo root = parent of scripts/
$lock = Join-Path $proj "tgparser.lock"
$py   = Join-Path $proj ".venv\Scripts\pythonw.exe"
if (-not (Test-Path $py)) {
    $py = Join-Path $proj ".venv\Scripts\python.exe"
}
$runpy = Join-Path $proj "run.py"

function Pid-Alive([int]$pidToCheck) {
    try { Get-Process -Id $pidToCheck -ErrorAction Stop | Out-Null; return $true }
    catch { return $false }
}

# Already running?
if (Test-Path $lock) {
    $pid = 0
    try { $pid = [int]((Get-Content $lock -Raw).Trim()) } catch {}
    if ($pid -gt 0 -and (Pid-Alive $pid)) { exit 0 }
    # Stale lock: a msvcrt-locked file left behind by a killed process.
    Remove-Item $lock -Force -ErrorAction SilentlyContinue
}

# Start detached, suppressing console window.
Start-Process -FilePath $py -ArgumentList "`"$runpy`"" -WorkingDirectory $proj -WindowStyle Hidden
