# =============================================================================
# ClearVue Streaming Pipeline - Setup Script (PowerShell)
# =============================================================================
# Sets up:
# - Python virtual environment
# - Required dependencies
# - Kafka infrastructure (Docker)
# - MongoDB Atlas connection verification
# =============================================================================

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Write-Host "`n========================================================================"
Write-Host "   ______ __      ______   ___    ____    __  ________"
Write-Host "  / ____// /     / ____/  /   |  / __ \  | | / / ____/"
Write-Host " / /    / /     / __/    / /| | / /_/ /  | |/ / __/   "
Write-Host "/ /___ / /___  / /___   / ___ |/ _, _/   |   / /___   "
Write-Host "\____//_____/ /_____/  /_/  |_/_/ |_|    |__/_____/   "
Write-Host ""
Write-Host "           Real-time Streaming Analytics Platform"
Write-Host "                        SETUP SCRIPT"
Write-Host "========================================================================`n"

# -----------------------------------------------------------------------------
# Check Prerequisites
# -----------------------------------------------------------------------------
Write-Host "🔍 Checking prerequisites..."`n

# Check Python
$python = Get-Command python -ErrorAction SilentlyContinue
if (-not $python) {
    Write-Host "❌ Python 3 is not installed. Please install Python 3.8 or higher."
    exit 1
}

$pythonVersion = (& python --version).Split()[1]
Write-Host "✅ Python found: $pythonVersion"

# Check Docker
$docker = Get-Command docker -ErrorAction SilentlyContinue
if (-not $docker) {
    Write-Host "❌ Docker is not installed. Please install Docker Desktop."
    exit 1
}
$dockerVersion = (& docker --version) -replace 'Docker version ',''
Write-Host "✅ Docker found: $dockerVersion"

# Check Docker Compose
$dockerCompose = Get-Command docker-compose -ErrorAction SilentlyContinue
if (-not $dockerCompose) {
    Write-Host "❌ Docker Compose is not installed. Please install Docker Compose."
    exit 1
}
$dockerComposeVersion = (& docker-compose --version) -replace 'docker-compose version ',''
Write-Host "✅ Docker Compose found: $dockerComposeVersion`n"

# -----------------------------------------------------------------------------
# Python Virtual Environment
# -----------------------------------------------------------------------------
Write-Host "📦 Setting up Python virtual environment..."`n

if (-not (Test-Path "venv")) {
    Write-Host "Creating virtual environment..."
    python -m venv venv
    Write-Host "✅ Virtual environment created"
} else {
    Write-Host "✅ Virtual environment already exists"
}

# Activate virtual environment
Write-Host "Activating virtual environment..."
& .\venv\Scripts\Activate.ps1
Write-Host "✅ Virtual environment activated`n"

# -----------------------------------------------------------------------------
# Install Python Dependencies
# -----------------------------------------------------------------------------
Write-Host "📚 Installing Python dependencies..."`n

Write-Host "Upgrading pip..."
python -m pip install --upgrade pip | Out-Null

Write-Host "Installing packages..."
python -m pip install pymongo kafka-python Faker | Out-Null

Write-Host "✅ Python dependencies installed:"
python -m pip show pymongo kafka-python Faker | ForEach-Object { Write-Host "   $_" }

Write-Host ""

# -----------------------------------------------------------------------------
# Verify Configuration File
# -----------------------------------------------------------------------------
Write-Host "⚙️  Verifying configuration..."`n

if (-not (Test-Path "ClearVueConfig.py")) {
    Write-Host "❌ ClearVueConfig.py not found!"
    Write-Host "   Please ensure ClearVueConfig.py is in the current directory."
    exit 1
}
Write-Host "✅ Configuration file found`n"

# -----------------------------------------------------------------------------
# Start Kafka Infrastructure (Docker)
# -----------------------------------------------------------------------------
Write-Host "🐳 Starting Kafka infrastructure..."`n

if (-not (Test-Path "docker-compose.yml")) {
    Write-Host "❌ docker-compose.yml not found!"
    Write-Host "   Please ensure docker-compose.yml is in the current directory."
    exit 1
}

# Check if containers are running
$containers = & docker-compose ps -q
if ($containers) {
    Write-Host "⚠️  Kafka containers are already running. Restarting..."
    docker-compose down
    Start-Sleep -Seconds 2
}

Write-Host "Starting Kafka, Zookeeper..."
docker-compose up -d

Write-Host "`n⏳ Waiting for Kafka to be ready (30 seconds)..."
Start-Sleep -Seconds 30

# Verify Kafka is running
$upContainers = & docker-compose ps | Select-String "Up"
if ($upContainers) {
    Write-Host "✅ Kafka infrastructure is running"
    docker-compose ps
} else {
    Write-Host "❌ Failed to start Kafka infrastructure"
    exit 1
}
Write-Host ""

# -----------------------------------------------------------------------------
# Test MongoDB Atlas Connection
# -----------------------------------------------------------------------------
Write-Host "🔌 Testing MongoDB Atlas connection..."`n

$mongoTestScript = @"
import sys
try:
    from ClearVueConfig import ClearVueConfig
    from pymongo import MongoClient

    uri = ClearVueConfig.get_mongo_uri()
    db_name = ClearVueConfig.get_database_name()

    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command('ping')

    db = client[db_name]
    collections = db.list_collection_names()

    print(f"✅ MongoDB Atlas connection successful!")
    print(f"   Database: {db_name}")
    print(f"   Collections found: {len(collections)}")
    if collections:
        for coll in collections:
            count = db[coll].count_documents({})
            print(f"      - {coll}: {count:,} documents")

    client.close()
    sys.exit(0)

except Exception as e:
    print(f"❌ MongoDB Atlas connection failed: {e}")
    print("")
    print("   Please check:")
    print("   1. Your internet connection")
    print("   2. MongoDB Atlas IP whitelist (allow your IP)")
    print("   3. Database credentials in ClearVueConfig.py")
    print("   4. Database name is correct: Nova_Analytix")
    sys.exit(1)
"@

$mongoTestScript | Set-Content -Path "test_mongo_connection.py"
python test_mongo_connection.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "`n❌ MongoDB Atlas connection failed. Please fix the connection before proceeding."
    Remove-Item "test_mongo_connection.py"
    exit 1
}
Remove-Item "test_mongo_connection.py"
Write-Host ""

# -----------------------------------------------------------------------------
# Create Helper Scripts
# -----------------------------------------------------------------------------
Write-Host "📝 Creating helper scripts..."`n

# Start script
@"
python .\ClearVue_Streaming_Pipeline.py
"@ | Set-Content start_pipeline.ps1
Write-Host "✅ Created start_pipeline.ps1"

# Simulator script
@"
python .\ClearVue_Transaction_Simulator.py
"@ | Set-Content start_simulator.ps1
Write-Host "✅ Created start_simulator.ps1"

# Stop script
@"
Write-Host 'Stopping ClearVue services...'
docker-compose down
Write-Host '✅ All services stopped'
"@ | Set-Content stop_all.ps1
Write-Host "✅ Created stop_all.ps1"

# Status script
@"
Write-Host '========================================================================'
Write-Host 'CLEARVUE STATUS CHECK'
Write-Host '========================================================================'
Write-Host ''
Write-Host '🐳 Docker Containers:'
docker-compose ps
Write-Host ''
Write-Host '📊 Kafka Topics:'
try {
    docker exec -it (docker-compose ps -q kafka) kafka-topics --list --bootstrap-server localhost:9092
} catch {
    Write-Host '   ⚠️  Kafka not running'
}
"@ | Set-Content check_status.ps1
Write-Host "✅ Created check_status.ps1"
Write-Host ""

# -----------------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------------
Write-Host "========================================================================"
Write-Host "✅ SETUP COMPLETE!"
Write-Host "========================================================================"
Write-Host ""
Write-Host "📋 What was set up:"
Write-Host "   ✅ Python virtual environment (venv/)"
Write-Host "   ✅ Python dependencies (pymongo, kafka-python, Faker)"
Write-Host "   ✅ Kafka + Zookeeper (Docker containers running)"
Write-Host "   ✅ MongoDB Atlas connection verified"
Write-Host "   ✅ Helper scripts created`n"
Write-Host "🚀 Quick Start Commands:`n"
Write-Host "   1. Generate test data:"
Write-Host "      ./start_simulator.ps1"
Write-Host "      or: .\\venv\\Scripts\\Activate.ps1; python ClearVue_Transaction_Simulator.py`n"
Write-Host "   2. Start streaming pipeline:"
Write-Host "      ./start_pipeline.ps1"
Write-Host "      or: .\\venv\\Scripts\\Activate.ps1; python ClearVue_Streaming_Pipeline.py`n"
Write-Host "   3. Check status:"
Write-Host "      ./check_status.ps1`n"
Write-Host "   4. Stop all services:"
Write-Host "      ./stop_all.ps1`n"
Write-Host "📚 For detailed instructions, see README.md`n"
Write-Host "========================================================================"
Write-Host "💡 TIP: Run the simulator first to generate test data, then start the pipeline to stream changes to Kafka in real-time!"
Write-Host "========================================================================`n"
