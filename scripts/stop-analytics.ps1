. (Join-Path $PSScriptRoot 'common.ps1')
Invoke-ZtmCompose @('--profile','analytics','--profile','scheduler','stop',
    'batch-scheduler','hive-server2','hive-metastore','hive-db')
