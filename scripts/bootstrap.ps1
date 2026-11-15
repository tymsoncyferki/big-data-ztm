<#
.SYNOPSIS
Starts the full Big Data ZTM stack and runs one batch; toggles optional services.
.EXAMPLE
.\scripts\bootstrap.ps1 -WithData -WithSchedule
#>
param(
    [switch]$WithData,
    [switch]$NoData,
    [switch]$WithSchedule,
    [switch]$NoSchedule
)
. (Join-Path $PSScriptRoot 'common.ps1')
if ($WithData -and $NoData) { throw 'Choose either -WithData or -NoData.' }
if ($WithSchedule -and $NoSchedule) { throw 'Choose either -WithSchedule or -NoSchedule.' }
foreach ($secret in @('NIFI_USERNAME','NIFI_PASSWORD','NIFI_SENSITIVE_PROPS_KEY','HIVE_DB_PASSWORD')) {
    $null = Assert-ZtmSecret $secret
}
if ((Get-ZtmSecret 'NIFI_PASSWORD').Length -lt 12 -or (Get-ZtmSecret 'NIFI_SENSITIVE_PROPS_KEY').Length -lt 12) {
    throw 'NiFi password and sensitive properties key must be at least 12 characters.'
}
if ($WithData) { $null = Assert-ZtmSecret 'WEATHER_API_TOKEN' }

Write-Host '[1/7] Starting HDFS, Kafka and NiFi...'
Invoke-ZtmCompose @('up', '-d', '--build')

Write-Host '[2/7] Preparing Kafka topics...'
for ($i = 0; $i -lt 45; $i++) {
    & docker compose -f docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list *> $null
    if ($LASTEXITCODE -eq 0) { break }
    if ($i -eq 44) { throw 'Kafka is not ready.' }
    Start-Sleep -Seconds 2
}
foreach ($topic in @('gps','weather','air_quality')) {
    Invoke-ZtmCompose @('exec','-T','kafka','/opt/kafka/bin/kafka-topics.sh',
        '--bootstrap-server','kafka:19092','--create','--if-not-exists',
        '--topic',$topic,'--partitions','3','--replication-factor','1')
}

Write-Host '[3/7] Initializing HDFS and Hive...'
for ($i = 0; $i -lt 60; $i++) {
    $status = & docker compose -f docker-compose.yml exec -T namenode hdfs dfsadmin -safemode get 2>$null
    if ($LASTEXITCODE -eq 0 -and ($status -join ' ') -match 'OFF') { break }
    if ($i -eq 59) { throw 'HDFS is not ready.' }
    Start-Sleep -Seconds 2
}
$raw = @('gps','weather','air_quality','gtfs')
$cleaned = @('gps_observations','weather_observations','air_quality_observations','gtfs_routes',
    'gtfs_stops','gtfs_trips','gtfs_stop_times','gtfs_calendar','gtfs_calendar_dates')
$paths = @('/data/warehouse','/data/raw','/data/cleaned')
$paths += $raw | ForEach-Object { "/data/raw/$_" }
$cleanedPaths = @($cleaned | ForEach-Object { "/data/cleaned/$_" })
$paths += $cleanedPaths
Invoke-ZtmCompose (@('exec','-T','namenode','hdfs','dfs','-mkdir','-p') + $paths)
Invoke-ZtmCompose (@('exec','-T','namenode','hdfs','dfs','-chmod','775','/data/warehouse','/data/cleaned') + $cleanedPaths)
$state = '/data/cleaned/_pipeline_state'
$statePaths = @($state,"$state/raw","$state/gtfs","$state/staging","$state/backups")
Invoke-ZtmCompose (@('exec','-T','namenode','hdfs','dfs','-mkdir','-p') + $statePaths)
Invoke-ZtmCompose (@('exec','-T','namenode','hdfs','dfs','-chmod','775') + $statePaths)
Invoke-ZtmCompose @('--profile','analytics','up','-d','--build','hive-db','hive-metastore','hive-server2')
for ($i = 0; $i -lt 80; $i++) {
    & docker compose -f docker-compose.yml --profile analytics exec -T hive-server2 /opt/hive/bin/beeline -u 'jdbc:hive2://localhost:10000/default' -e 'SHOW DATABASES;' *> $null
    if ($LASTEXITCODE -eq 0) { break }
    if ($i -eq 79) { throw 'HiveServer2 is not ready.' }
    Start-Sleep -Seconds 5
}
Invoke-ZtmCompose @('--profile','analytics','exec','-T','hive-server2','/opt/hive/bin/beeline',
    '-u','jdbc:hive2://localhost:10000/default','-f','/opt/hive/project-sql/cleaned_tables.sql')
Invoke-ZtmCompose @('--profile','analytics','exec','-T','hive-server2','/opt/hive/bin/beeline',
    '-u','jdbc:hive2://localhost:10000/default','-f','/opt/hive/project-sql/refresh_partitions.sql')
Invoke-ZtmCompose @('--profile','analytics','build','batch-runner')

Write-Host '[4/7] Initializing NiFi in a Docker container...'
Invoke-ZtmCompose @('--profile','tools','build','nifi-init')
Invoke-ZtmCompose @('--profile','tools','run','--rm','--no-deps','nifi-init')

Write-Host '[5/7] Configuring optional source acquisition (simulator + GTFS)...'
if ($WithData) {
    & (Join-Path $PSScriptRoot 'start-data.ps1')
} else {
    & (Join-Path $PSScriptRoot 'stop-data.ps1')
}

Write-Host '[6/7] Running one incremental batch...'
Invoke-ZtmCompose @('--profile','analytics','run','--rm','--no-deps','batch-runner')

Write-Host '[7/7] Configuring optional in-Docker scheduler...'
if ($WithSchedule) {
    Invoke-ZtmCompose @('--profile','analytics','--profile','scheduler','up','-d','--no-deps','batch-scheduler')
} else {
    Invoke-ZtmCompose @('--profile','analytics','--profile','scheduler','stop','batch-scheduler')
}
Write-Host 'Bootstrap completed. NiFi: https://localhost:8443/nifi; Hive JDBC: localhost:10000.'
