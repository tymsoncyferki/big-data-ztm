. (Join-Path $PSScriptRoot 'common.ps1')
Invoke-ZtmCompose @('--profile','analytics','--profile','scheduler','up','-d','--no-deps','batch-scheduler','heartbeat')
