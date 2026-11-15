# Enable the optional source simulator and daily GTFS download through NiFi.
. (Join-Path $PSScriptRoot 'common.ps1')
$null = Assert-ZtmSecret 'WEATHER_API_TOKEN'

function Test-GtfsArchive {
    $output = & docker compose -f docker-compose.yml exec -T namenode hdfs dfs -ls -R /data/raw/gtfs 2>$null
    if ($LASTEXITCODE -ne 0) { return $false }
    foreach ($line in $output) {
        $fields = ($line.Trim() -split '\s+')
        if ($fields.Count -ge 8 -and $fields[0] -match '^-' -and
            $fields[-1] -match '\.zip$' -and [long]$fields[4] -gt 0) {
            return $true
        }
    }
    return $false
}

$hasArchive = Test-GtfsArchive
$control = @('--profile','tools','run','--rm','--no-deps','nifi-init',
             'python','-u','scripts/init-nifi.py','--action','start-gtfs')
if ($hasArchive) {
    Write-Host '[GTFS] Existing nonempty ZIP found in HDFS; skipping initial download.'
} else {
    Write-Host '[GTFS] No ZIP found in HDFS; requesting initial NiFi download.'
    $control += '--initial-download'
}
Invoke-ZtmCompose $control
Invoke-ZtmCompose @('--profile','ingestion','up','-d','--build','--no-deps','simulator')

if (-not $hasArchive) {
    Write-Host '[GTFS] Waiting for the archived ZIP in HDFS (up to 300 seconds)...'
    for ($attempt = 1; $attempt -le 60; $attempt++) {
        if (Test-GtfsArchive) {
            Write-Host '[GTFS] Initial ZIP is available in HDFS.'
            Invoke-ZtmCompose @('--profile','tools','run','--rm','--no-deps','nifi-init',
                'python','-u','scripts/init-nifi.py','--action','enable-gtfs-trigger')
            return
        }
        Start-Sleep -Seconds 5
    }
    throw '[GTFS] No ZIP appeared within 300 seconds. Check NiFi Download/PutHDFS bulletins and the source URL.'
}
