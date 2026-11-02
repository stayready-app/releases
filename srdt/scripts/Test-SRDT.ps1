[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)]
  [string]$Token,

  [string]$ServerOverride = "",

  [ValidateRange(0, 5000)]
  [int]$StressClients = 0,

  [string]$ResultsPath = ""
)

$ErrorActionPreference = "Stop"

function Decode-Base64Url([string]$Value)
{
  $base64 = $Value.Replace('-', '+').Replace('_', '/')
  switch ($base64.Length % 4)
  {
    0 { }
    2 { $base64 += "==" }
    3 { $base64 += "=" }
    default { throw "Invalid base64url token length" }
  }
  $bytes = [Convert]::FromBase64String($base64)
  return [Text.Encoding]::UTF8.GetString($bytes)
}

function Parse-Token([string]$Value)
{
  if (-not $Value.StartsWith("SRDT1."))
  {
    throw "Unsupported token. Expected SRDT1.<base64url>."
  }
  $json = Decode-Base64Url $Value.Substring(6)
  $cfg = $json | ConvertFrom-Json
  if ($cfg.v -ne 1 -or -not $cfg.host -or -not $cfg.port -or -not $cfg.psk -or -not $cfg.pub)
  {
    throw "Token is missing required fields."
  }
  if ($cfg.psk -notmatch '^[0-9a-fA-F]{64}$')
  {
    throw "Token PSK has invalid format."
  }
  if ($cfg.pub -notmatch '^[0-9a-fA-F]{64}$')
  {
    throw "Token server public key has invalid format."
  }
  return $cfg
}

function Format-Endpoint([string]$HostName, [int]$Port)
{
  if ($HostName.Contains(':') -and -not $HostName.StartsWith('['))
  {
    return "[$HostName]:$Port"
  }
  return "${HostName}:$Port"
}

function Extract-Json([string]$Text)
{
  $first = $Text.IndexOf('{')
  $last = $Text.LastIndexOf('}')
  if ($first -lt 0 -or $last -lt $first)
  {
    throw "No JSON result found in command output:`n$Text"
  }
  return $Text.Substring($first, $last - $first + 1) | ConvertFrom-Json
}

function Invoke-SrdtLoad
{
  param(
    [string]$Name,
    [string[]]$Arguments,
    [long]$ExpectedBytes,
    [int]$ExpectedClients = 1
  )

  Write-Host ""
  Write-Host "=== $Name ===" -ForegroundColor Cyan
  $started = Get-Date
  $raw = (& $script:LoadExe @Arguments 2>&1 | Out-String)
  $exitCode = $LASTEXITCODE
  $elapsed = (Get-Date) - $started

  $record = [ordered]@{
    name = $Name
    passed = $false
    exit_code = $exitCode
    elapsed_seconds = [Math]::Round($elapsed.TotalSeconds, 3)
    raw = $raw.Trim()
  }

  try
  {
    $json = Extract-Json $raw
    $record["result"] = $json
    $passed = ($exitCode -eq 0) -and ($json.succeeded -eq $ExpectedClients)
    if ($ExpectedBytes -ge 0)
    {
      $passed = $passed -and ([int64]$json.verified_bytes_one_direction -eq $ExpectedBytes)
    }
    $record["passed"] = [bool]$passed
  }
  catch
  {
    $record["parse_error"] = $_.Exception.Message
  }

  if ([bool]$record["passed"])
  {
    Write-Host "PASS" -ForegroundColor Green
    if ($record.Contains("result"))
    {
      Write-Host ("  clients : {0}/{1}" -f $record["result"].succeeded, $ExpectedClients)
      Write-Host ("  bytes   : {0}" -f $record["result"].verified_bytes_one_direction)
      Write-Host ("  seconds : {0}" -f $record["result"].seconds)
      if ($record["result"].verified_one_direction_mbps)
      {
        Write-Host ("  Mi/bit/s: {0:N2}" -f [double]$record["result"].verified_one_direction_mbps)
      }
    }
  }
  else
  {
    Write-Host "FAIL" -ForegroundColor Red
    Write-Host $raw
  }

  return [pscustomobject]$record
}

$cfg = Parse-Token $Token
$hostName = [string]$cfg.host
if ($ServerOverride)
{
  $hostName = $ServerOverride
}
$server = Format-Endpoint $hostName ([int]$cfg.port)

$root = Split-Path -Parent $PSScriptRoot
$LoadExe = Join-Path $root "bin\srdt-load-windows-amd64.exe"
if (-not (Test-Path -LiteralPath $LoadExe))
{
  throw "Windows load binary not found: $LoadExe. Extract the full Stage 25 archive before running this script."
}
$script:LoadExe = $LoadExe

if (-not $ResultsPath)
{
  $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
  $ResultsPath = Join-Path (Get-Location) "srdt-test-results-$stamp.json"
}

Write-Host "SRDT Stage 25 personal verification" -ForegroundColor White
Write-Host "Server : $server"
Write-Host "Binary : $LoadExe"
Write-Host "Tests  : smoke, 100 MiB, size-drop, keepalive" 
if ($StressClients -gt 0)
{
  Write-Host "Stress : $StressClients clients"
}

$common = @(
  "--server", $server,
  "--server-public", [string]$cfg.pub,
  "--psk", [string]$cfg.psk
)

$results = @()

$testArgs = $common + @(
  "--mode", "bridge",
  "--clients", "1",
  "--messages", "16",
  "--bytes", "4096",
  "--timeout", "30s"
)
$results += Invoke-SrdtLoad -Name "Smoke / real Windows -> Ubuntu bridge" -ExpectedBytes 65536 -Arguments $testArgs

$testArgs = $common + @(
  "--mode", "bridge",
  "--clients", "1",
  "--messages", "6400",
  "--bytes", "16384",
  "--timeout", "5m"
)
$results += Invoke-SrdtLoad -Name "Bulk / 100 MiB exact byte verification" -ExpectedBytes 104857600 -Arguments $testArgs

$testArgs = $common + @(
  "--mode", "bridge",
  "--clients", "1",
  "--messages", "64",
  "--bytes", "4096",
  "--drop-min", "800",
  "--drop-max", "1100",
  "--timeout", "90s"
)
$results += Invoke-SrdtLoad -Name "Selective size loss / drop encrypted datagrams 800..1100" -ExpectedBytes 262144 -Arguments $testArgs

$testArgs = $common + @(
  "--mode", "bridge-idle",
  "--clients", "1",
  "--messages", "1",
  "--bytes", "1",
  "--hold", "4s",
  "--keepalive", "500ms",
  "--keepalive-timeout", "2s",
  "--timeout", "20s"
)
$results += Invoke-SrdtLoad -Name "Silent bridge / keepalive survives server idle timeout" -ExpectedBytes 1 -Arguments $testArgs

if ($StressClients -gt 0)
{
  $expectedStressBytes = [int64]$StressClients * 16 * 4096
  $testArgs = $common + @(
    "--mode", "bridge",
    "--clients", "$StressClients",
    "--messages", "16",
    "--bytes", "4096",
    "--timeout", "3m"
  )
  $results += Invoke-SrdtLoad -Name "Stress / $StressClients concurrent bridge clients" -ExpectedClients $StressClients -ExpectedBytes $expectedStressBytes -Arguments $testArgs
}

$failed = @($results | Where-Object { -not $_.passed })
$summary = [ordered]@{
  stage = 25
  server = $server
  generated_at = (Get-Date).ToString("o")
  passed = ($failed.Count -eq 0)
  passed_count = @($results | Where-Object { $_.passed }).Count
  failed_count = $failed.Count
  stress_clients = $StressClients
  tests = $results
}

$summary | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $ResultsPath -Encoding UTF8

Write-Host ""
Write-Host "============================================================"
if ($failed.Count -eq 0)
{
  Write-Host "OVERALL: PASS ($($summary.passed_count)/$($results.Count))" -ForegroundColor Green
}
else
{
  Write-Host "OVERALL: FAIL ($($failed.Count) failed)" -ForegroundColor Red
  foreach ($item in $failed)
  {
    Write-Host "  - $($item.name)"
  }
}
Write-Host "Results: $ResultsPath"
Write-Host "============================================================"

if ($failed.Count -ne 0)
{
  Write-Host "If Smoke fails immediately, first check that UDP/$($cfg.port) is allowed to $hostName." -ForegroundColor Yellow
  Write-Host "If the token contains a private/unreachable address, restart Linux with --advertise <reachable-ip>." -ForegroundColor Yellow
  exit 1
}
exit 0
