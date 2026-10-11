$ErrorActionPreference = "Stop"

# Create root directory
New-Item -ItemType Directory -Force -Path "activation-poc"
Set-Location "activation-poc"

# Initialize empty package.json
npm init -y
npm pkg set name="activation-poc" private=true workspaces.0="apps/*" workspaces.1="libs/*"

# Create apps and libs directories
New-Item -ItemType Directory -Force -Path "apps"
New-Item -ItemType Directory -Force -Path "libs"

# Function to setup a basic NestJS microservice package
function Setup-NestApp {
    param([string]$Name)
    $AppPath = "apps/$Name"
    New-Item -ItemType Directory -Force -Path $AppPath
    Set-Location $AppPath
    npm init -y
    npm pkg set name="@activation-poc/$Name" version="1.0.0"
    
    # We will add dependencies later at the root level
    New-Item -ItemType Directory -Force -Path "src"
    New-Item -ItemType File -Force -Path "tsconfig.json" -Value '{
  "compilerOptions": {
    "module": "commonjs",
    "declaration": true,
    "removeComments": true,
    "emitDecoratorMetadata": true,
    "experimentalDecorators": true,
    "allowSyntheticDefaultImports": true,
    "target": "ES2021",
    "sourceMap": true,
    "outDir": "./dist",
    "baseUrl": "./",
    "incremental": true
  }
}'
    Set-Location "../.."
}

# Setup NestJS apps
Setup-NestApp "activation-api"
Setup-NestApp "billing-service"
Setup-NestApp "provisioning-service"
Setup-NestApp "notification-service"
Setup-NestApp "crm-analytics-service"

# Setup Vite React app
Set-Location "apps"
npx create-vite demo-ui --template react-ts
Set-Location ".."
# Adjust demo-ui package name
$demoUiPkg = Get-Content "apps/demo-ui/package.json" | ConvertFrom-Json
$demoUiPkg.name = "@activation-poc/demo-ui"
$demoUiPkg | ConvertTo-Json -Depth 10 | Set-Content "apps/demo-ui/package.json"

# Setup Libs
function Setup-Lib {
    param([string]$Name)
    $LibPath = "libs/$Name"
    New-Item -ItemType Directory -Force -Path $LibPath
    Set-Location $LibPath
    npm init -y
    npm pkg set name="@activation-poc/$Name" version="1.0.0" main="dist/index.js" types="dist/index.d.ts"
    New-Item -ItemType Directory -Force -Path "src"
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
