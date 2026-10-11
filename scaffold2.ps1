$ErrorActionPreference = "Stop"

Set-Location "activation-poc"

# Setup Vite React app
Set-Location "apps"
# Remove if exists
if (Test-Path "demo-ui") {
    Remove-Item -Recurse -Force "demo-ui"
}
npx create-vite@latest demo-ui --template react-ts --yes
Set-Location ".."
# Adjust demo-ui package name
$demoUiPkg = Get-Content "apps/demo-ui/package.json" | ConvertFrom-Json
$demoUiPkg.name = "@activation-poc/demo-ui"
$demoUiPkg | ConvertTo-Json -Depth 10 | Set-Content "apps/demo-ui/package.json"

# Setup Libs (re-run just in case)
function Setup-Lib {
    param([string]$Name)
    $LibPath = "libs/$Name"
    if (-not (Test-Path $LibPath)) {
        New-Item -ItemType Directory -Force -Path $LibPath
    }
    Set-Location $LibPath
    npm init -y
    npm pkg set name="@activation-poc/$Name" version="1.0.0" main="dist/index.js" types="dist/index.d.ts"
    if (-not (Test-Path "src")) {
        New-Item -ItemType Directory -Force -Path "src"
    }
    New-Item -ItemType File -Force -Path "tsconfig.json" -Value '{
  "compilerOptions": {
    "module": "commonjs",
    "declaration": true,
    "target": "ES2021",
    "outDir": "./dist",
    "strict": true
  }
}'
    Set-Location "../.."
}

Setup-Lib "contracts"
Setup-Lib "kafka-toolkit"

# Create Docker compose and scripts
New-Item -ItemType Directory -Force -Path "docker"
New-Item -ItemType Directory -Force -Path "docs"

Write-Host "Scaffolding complete."
