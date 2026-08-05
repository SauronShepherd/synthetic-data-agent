param(
    [string]$Output = "dist\synthetic-data-agent-0.6.0.dev0.zip"
)

$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..")
$outputPath = Join-Path $root $Output
$outputDir = Split-Path -Parent $outputPath
New-Item -ItemType Directory -Path $outputDir -Force | Out-Null

if ((git -C $root status --porcelain)) {
    throw "Release packaging requires a clean worktree; commit or remove local changes first."
}

git -C $root archive --format=zip --output=$outputPath HEAD
$checkDir = Join-Path $env:TEMP "sda-release-check"
if (Test-Path -LiteralPath $checkDir) { Remove-Item -LiteralPath $checkDir -Recurse -Force }
New-Item -ItemType Directory -Path $checkDir -Force | Out-Null
Expand-Archive -LiteralPath $outputPath -DestinationPath $checkDir -Force
python (Join-Path $root "scripts\check_release.py") $checkDir
Write-Output "Created tracked release archive: $outputPath"
