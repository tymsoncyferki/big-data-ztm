. (Join-Path $PSScriptRoot 'common.ps1')
Invoke-ZtmCompose @('--profile','analytics','--profile','scheduler','stop','batch-scheduler')
