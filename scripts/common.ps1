# Shared host-side PowerShell helpers. Docker is the only required runtime.
$ErrorActionPreference = 'Stop'
Set-Location (Resolve-Path (Join-Path $PSScriptRoot '..')).Path

function Invoke-ZtmCompose {
    param([string[]]$ComposeArgs)
    & docker compose -f docker-compose.yml @ComposeArgs
    if ($LASTEXITCODE -ne 0) { throw "Docker Compose failed (exit $LASTEXITCODE): $($ComposeArgs -join ' ')" }
}

function Get-ZtmSecret {
    param([string]$Name)
    if (-not (Test-Path '.env')) { throw 'Missing .env. Copy .env.example and configure credentials.' }
    $match = [regex]::Match((Get-Content '.env' -Raw), "(?m)^\s*$([regex]::Escape($Name))\s*=\s*(.*)$")
    if (-not $match.Success) { return '' }
    return $match.Groups[1].Value.Trim().Trim('"').Trim("'")
}

function Assert-ZtmSecret {
    param([string]$Name)
    $value = Get-ZtmSecret $Name
    if ([string]::IsNullOrWhiteSpace($value) -or $value.StartsWith('REPLACE_')) {
        throw "Set $Name in .env to a non-placeholder value."
    }
    return $value
}
