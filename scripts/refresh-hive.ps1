. (Join-Path $PSScriptRoot 'common.ps1')
Invoke-ZtmCompose @('--profile','analytics','exec','-T','hive-server2','/opt/hive/bin/beeline',
    '-u','jdbc:hive2://localhost:10000/default','-f','/opt/hive/project-sql/refresh_partitions.sql')
