# Disable source acquisition, leaving HDFS, Kafka and batch scheduling intact.
. (Join-Path $PSScriptRoot 'common.ps1')
Invoke-ZtmCompose @('--profile','ingestion','stop','simulator')
Invoke-ZtmCompose @('--profile','tools','run','--rm','--no-deps','nifi-init',
    'python','-u','scripts/init-nifi.py','--action','stop-gtfs')
