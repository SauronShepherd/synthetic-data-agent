param(
    [string]$Output = "dist\synthetic-data-agent-0.5.0.zip"
)

$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..")
$outputPath = Join-Path $root $Output
$outputDir = Split-Path -Parent $outputPath
New-Item -ItemType Directory -Path $outputDir -Force | Out-Null

git -C $root archive --format=zip --output=$outputPath HEAD
Write-Output "Created tracked release archive: $outputPath"
