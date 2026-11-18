$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$withVs = Join-Path $PWD 'cmake/WithVs.cmd'
function Build-CMake {
    if ($IsWindows) { & $withVs cmake @args } else { & cmake @args }
    if ($LASTEXITCODE) { throw "cmake $args failed ($LASTEXITCODE)" }
}

foreach ($project in 'chttp','turbodb') {
    Push-Location "build/upstream/$project"
    try {
        $preset = $env:NATIVE_PRESET
        $options = @('-DBUILD_TESTS=OFF','-DBUILD_BENCHMARK=OFF')
        if ($project -eq 'chttp' -and $preset -eq 'llvm-native-asan-ubsan-user') {
            $preset = 'llvm-native-release-user'
            $options += '-DENABLE_ASAN=ON','-DENABLE_UBSAN=ON'
        }
        $prefix = [IO.Path]::GetFullPath("build/$preset/stage").Replace('\','/')
        Build-CMake --fresh --preset $preset "-DCMAKE_INSTALL_PREFIX=$prefix" @options
        Build-CMake --build --preset $preset
        Build-CMake --install "build/$preset"
        "$($project.ToUpperInvariant())_ROOT=$prefix" >> $env:GITHUB_ENV
    } finally { Pop-Location }
}
