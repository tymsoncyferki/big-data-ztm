. (Join-Path $PSScriptRoot 'common.ps1')
Invoke-ZtmCompose @('--profile','ingestion','stop','simulator')
