. (Join-Path $PSScriptRoot 'common.ps1')
$null = Assert-ZtmSecret 'WEATHER_API_TOKEN'
Invoke-ZtmCompose @('--profile','ingestion','up','-d','--build','--no-deps','simulator')
