# Start the existing local model service on CPU, without changing user/system settings.
$ErrorActionPreference = 'Stop'
$deskOllamaPath = Join-Path $env:LOCALAPPDATA 'Programs/Ollama/ollama.exe'
if (-not (Test-Path -LiteralPath $deskOllamaPath)) {
    throw 'Instala Ollama y descarga qwen3.5:9b antes de iniciar el desk.'
}
if (Get-NetTCPConnection -LocalPort 11434 -State Listen -ErrorAction SilentlyContinue) {
    throw 'El puerto 11434 ya esta ocupado. Cierra Ollama desde su icono de la bandeja antes de iniciar este servidor CPU.'
}
$deskModelEnvironment = @{
    OLLAMA_HOST = '127.0.0.1:11434'
    OLLAMA_VULKAN = '0'
    GGML_VK_VISIBLE_DEVICES = '-1'
    ROCR_VISIBLE_DEVICES = '-1'
    CUDA_VISIBLE_DEVICES = '-1'
}
$deskPreviousEnvironment = @{}
try {
    foreach ($deskVariable in $deskModelEnvironment.Keys) {
        $deskPreviousEnvironment[$deskVariable] = [Environment]::GetEnvironmentVariable($deskVariable, 'Process')
        [Environment]::SetEnvironmentVariable($deskVariable, $deskModelEnvironment[$deskVariable], 'Process')
    }
    Write-Output 'Servidor local CPU en 127.0.0.1:11434. En otra terminal: python -m scripts.run_thesis_desk --local --once'
    & $deskOllamaPath serve
    if ($LASTEXITCODE -ne 0) { throw "Ollama termino con codigo $LASTEXITCODE" }
} finally {
    foreach ($deskVariable in $deskPreviousEnvironment.Keys) {
        [Environment]::SetEnvironmentVariable($deskVariable, $deskPreviousEnvironment[$deskVariable], 'Process')
    }
}
