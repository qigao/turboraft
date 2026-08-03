[CmdletBinding()]
param(
    [ValidateSet('raft_core.pml', 'membership.pml', 'ready_lifecycle.pml', 'snapshot_meta.pml')]
    [string] $Model = 'raft_core.pml',
    [string] $SpinCommand = 'spin',
    [string] $ClangCommand = 'clang',
    [switch] $KeepArtifacts
)

$ErrorActionPreference = 'Stop'
$formalRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$modelPath = Join-Path $formalRoot $Model
$compatPath = Join-Path $formalRoot 'spin_win_compat.h'

if (-not (Test-Path -LiteralPath $modelPath -PathType Leaf)) {
    throw "SPIN model not found: $modelPath"
}

$spin = Get-Command $SpinCommand -ErrorAction SilentlyContinue
if ($null -eq $spin) {
    throw "SPIN executable not found: $SpinCommand"
}

$clang = Get-Command $ClangCommand -ErrorAction SilentlyContinue
if ($null -eq $clang) {
    throw "Clang executable not found: $ClangCommand"
}

$artifactRoot = Join-Path ([System.IO.Path]::GetTempPath()) `
    ("turboraft-spin-{0}" -f [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $artifactRoot | Out-Null
$completed = $false

try {
    Copy-Item -LiteralPath $modelPath -Destination (Join-Path $artifactRoot $Model)
    Copy-Item -LiteralPath $compatPath -Destination (Join-Path $artifactRoot 'spin_win_compat.h')
    Push-Location $artifactRoot

    & $spin.Source '-Pclang -E -x c' -a $Model
    if ($LASTEXITCODE -ne 0) {
        throw "SPIN verifier generation failed for $Model"
    }

    & $clang.Source -O2 -DSAFETY -DWIN32 -D_CRT_SECURE_NO_WARNINGS `
        -Wno-deprecated-declarations -Wno-format -include spin_win_compat.h `
        -o pan.exe pan.c
    if ($LASTEXITCODE -ne 0) {
        throw "Clang failed to compile the SPIN verifier for $Model"
    }

    $panOutput = & .\pan.exe -m1000000 2>&1
    $panExitCode = $LASTEXITCODE
    $panText = $panOutput -join [Environment]::NewLine
    Write-Host $panText
    if ($panExitCode -ne 0 -or $panText -notmatch 'errors:\s+0') {
        throw "SPIN verification failed for $Model. Artifacts: $artifactRoot"
    }

    $completed = $true
    Write-Host "SPIN verification passed: $Model"
    Write-Host "Artifacts: $artifactRoot"
} catch {
    Write-Host "Artifacts: $artifactRoot"
    throw
} finally {
    Pop-Location
    if (-not $KeepArtifacts -and $completed) {
        Remove-Item -LiteralPath $artifactRoot -Recurse -Force
    }
}
