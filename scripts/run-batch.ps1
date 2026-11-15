param(
    [ValidateSet('all', 'gps', 'weather', 'air_quality', 'gtfs')]
    [string]$Dataset = 'all',
    [string]$Archive = '',
    [switch]$Force
)
. (Join-Path $PSScriptRoot 'common.ps1')
if (($Archive -ne '' -or $Force) -and $Dataset -ne 'gtfs') { throw '-Archive and -Force require -Dataset gtfs.' }
$commands = @('--profile','analytics','run','--rm','--no-deps','batch-runner',
    'python3','-u','/opt/project/scripts/container_scheduler.py','--dataset',$Dataset)
if ($Archive -ne '') { $commands += @('--archive', $Archive) }
if ($Force) { $commands += '--force' }
Invoke-ZtmCompose $commands
