# Collects Windows events and Docker Desktop logs from a given time window after a crash.
# Usage: .\scripts\collect-crash-logs.ps1 "2026-11-16 21:30" "2026-11-16 23:00"

param(
    [Parameter(Mandatory = $true)][datetime]$Since,
    [Parameter(Mandatory = $true)][datetime]$Until
)

$out = Join-Path $HOME ("crash-logs\" + $Since.ToString("yyyy-MM-dd_HHmm"))
New-Item -ItemType Directory -Path "$out\docker" -Force | Out-Null
Write-Host "Window: $Since -> $Until"
Write-Host "Output: $out"

$logs = @(
    "System",
    "Application",
    "Microsoft-Windows-Hyper-V-Compute-Operational",
    "Microsoft-Windows-Hyper-V-Compute-Admin"
)
foreach ($l in $logs) {
    Get-WinEvent -FilterHashtable @{ LogName = $l; StartTime = $Since; EndTime = $Until } -ErrorAction SilentlyContinue |
        Sort-Object TimeCreated |
        Format-Table TimeCreated, ProviderName, Id, LevelDisplayName, Message -Wrap |
        Out-File (Join-Path $out ("win-" + ($l -replace '[/ ]', '_') + ".txt")) -Width 300
}

# Docker Desktop logs (host side and its VM), only files touched after the start of the window
$dockerLogs = Join-Path $env:LOCALAPPDATA "Docker\log"
if (Test-Path $dockerLogs) {
    Get-ChildItem $dockerLogs -Recurse -File |
        Where-Object { $_.LastWriteTime -gt $Since } |
        ForEach-Object { Copy-Item $_.FullName -Destination (Join-Path "$out\docker" ($_.Directory.Name + "-" + $_.Name)) }
}

# Event 2004 = Windows is low on memory, the message lists the biggest processes
Write-Host ""
Write-Host "--- low memory events (Resource-Exhaustion-Detector 2004) ---"
$lowMem = Get-WinEvent -FilterHashtable @{ LogName = "System"; Id = 2004; StartTime = $Since; EndTime = $Until } -ErrorAction SilentlyContinue
if ($lowMem) { $lowMem | Format-List TimeCreated, Message } else { Write-Host "(none)" }

Write-Host ""
Get-ChildItem $out -Recurse -File | Select-Object Length, Name | Format-Table -AutoSize
