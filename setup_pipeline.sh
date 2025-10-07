#!/bin/bash

##############################################################################
# ClearVue Streaming Pipeline Setup Script
# 
# This script sets up the complete streaming infrastructure:
# 1. Docker volumes and networks
# 2. Kafka and MongoDB services
# 3. Python dependencies
# 4. Initial health checks
#
# Author: ClearVue Streaming Team
# Date: October 2025
##############################################################################

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print functions
print_header() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Check if Docker is installed
check_docker() {
    print_header "Checking Docker Installation"
    
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed!"
        echo "Please install Docker from: https://www.docker.com/get-started"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed!"
        echo "Please install Docker Compose"
        exit 1
    fi
    
    print_success "Docker is installed"
    docker --version
    docker-compose --version
    echo ""
}

# Check if Docker daemon is running
check_docker_running() {
    print_header "Checking Docker Status"
    
    if ! docker info &> /dev/null; then
        print_error "Docker daemon is not running!"
        echo "Please start Docker Desktop or Docker service"
        exit 1
    fi
    
    print_success "Docker daemon is running"
    echo ""
}

# Create Docker volumes
create_volumes() {
    print_header "Creating Docker Volumes"
    
    volumes=("clearvue_mongo_data" "clearvue_zookeeper_data" "clearvue_zookeeper_logs" "clearvue_kafka_data")
    
    for volume in "${volumes[@]}"; do
        if docker volume inspect $volume &> /dev/null; then
            print_warning "Volume $volume already exists"
        else
            docker volume create $volume
            print_success "Created volume: $volume"
        fi
    done
    echo ""
}

# Create Docker network
create_network() {
    print_header "Creating Docker Network"
    
    if docker network inspect clearvue_streaming_network &> /dev/null; then
        print_warning "Network clearvue_streaming_network already exists"
    else
        docker network create clearvue_streaming_network
        print_success "Created network: clearvue_streaming_network"
    fi
    echo ""
}

# Install Python dependencies
install_python_deps() {
    print_header "Installing Python Dependencies"
    
    if ! command -v python3 &> /dev/null; then
        print_error "Python 3 is not installed!"
        exit 1
    fi
    
    print_info "Installing required packages..."
    pip3 install pymongo kafka-python faker > /dev/null 2>&1
    
    print_success "Python dependencies installed"
    echo ""
}

# Start Docker services
start_services() {
    print_header "Starting Docker Services"
    
    print_info "Starting Zookeeper, Kafka, MongoDB..."
    docker-compose up -d
    
    print_success "Docker services started"
    echo ""
}

# Wait for services to be healthy
wait_for_services() {
    print_header "Waiting for Services to be Healthy"
    
    print_info "Waiting for Kafka (this may take 60-90 seconds)..."
    
    max_attempts=30
    attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        if docker-compose ps | grep -q "healthy.*kafka"; then
            print_success "Kafka is healthy"
            break
        fi
        
        attempt=$((attempt + 1))
        echo -n "."
        sleep 3
    done
    
    if [ $attempt -eq $max_attempts ]; then
        print_error "Kafka failed to become healthy"
        docker-compose logs kafka
        exit 1
    fi
    
    echo ""
    print_info "Waiting for MongoDB..."
    
    attempt=0
    while [ $attempt -lt $max_attempts ]; do
        if docker-compose ps | grep -q "healthy.*mongodb"; then
            print_success "MongoDB is healthy"
            break
        fi
        
        attempt=$((attempt + 1))
        echo -n "."
        sleep 3
    done
    
    if [ $attempt -eq $max_attempts ]; then
        print_error "MongoDB failed to become healthy"
        docker-compose logs mongodb
        exit 1
    fi
    
    echo ""
}

# Display service URLs
display_urls() {
    print_header "Service URLs"
    
    echo "Kafka UI:        http://localhost:8080"
    echo "Mongo Express:   http://localhost:8081 (user: clearvue, pass: admin123)"
    echo "Kafka Connect:   http://localhost:8083"
    echo ""
    
    print_info "MongoDB Connection String:"
    echo "mongodb://localhost:27017/"
    echo ""
    
    print_info "Kafka Bootstrap Servers:"
    echo "localhost:29092"
    echo ""
}

# Run health checks
health_check() {
    print_header "Running Health Checks"
    
    # Check Kafka
    print_info "Testing Kafka connection..."
    if docker exec clearvue_kafka kafka-topics --bootstrap-server localhost:9092 --list &> /dev/null; then
        print_success "Kafka is responsive"
    else
        print_error "Kafka is not responding"
    fi
    
    # Check MongoDB
    print_info "Testing MongoDB connection..."
    if docker exec clearvue_mongodb mongosh --eval "db.adminCommand('ping')" &> /dev/null; then
        print_success "MongoDB is responsive"
    else
        print_error "MongoDB is not responding"
    fi
    
    echo ""
}

# Display next steps
display_next_steps() {
    print_header "Setup Complete! Next Steps"
    
    echo "1. Load data into MongoDB Atlas:"
    echo "   python3 load_mongodb_atlas.py ./cleaned_data"
    echo ""
    echo "2. Start the streaming pipeline:"
    echo "   python3 clearvue_streaming_pipeline_final.py"
    echo ""
    echo "3. (Optional) Run transaction simulator in another terminal:"
    echo "   python3 transaction_simulator_enhanced.py"
    echo ""
    echo "4. (Optional) Test Kafka consumer:"
    echo "   python3 kafka_consumer_test.py"
    echo ""
    
    print_info "To stop all services:"
    echo "   docker-compose down"
    echo ""
    
    print_info "To view logs:"
    echo "   docker-compose logs -f [service_name]"
    echo ""
}

# Main execution
main() {
    clear
    
    print_header "ClearVue Streaming Pipeline Setup"
    echo "This will set up the complete streaming infrastructure"
    echo ""
    
    check_docker
    check_docker_running
    create_volumes
    create_network
    install_python_deps
    start_services
    wait_for_services
    health_check
    display_urls
    display_next_steps
    
    print_success "Setup completed successfully!"
}

# Run main function
main