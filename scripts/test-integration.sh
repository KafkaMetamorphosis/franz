#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Franz Integration Test Runner${NC}"
echo "=============================="

# Function to cleanup
cleanup() {
    if [ "$CLEANUP_DOCKER" = "true" ]; then
        echo -e "\n${YELLOW}Cleaning up Docker containers...${NC}"
        docker-compose -f docker-compose.test.yaml down -v 2>/dev/null || true
    fi
}

# Trap cleanup on script exit
trap cleanup EXIT

# Parse command line arguments
RUN_DOCKER_TESTS=false
RUN_UNIT_TESTS=false
RUN_INTEGRATION_TESTS=false
CLEANUP_DOCKER=true
VERBOSE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --docker)
            RUN_DOCKER_TESTS=true
            shift
            ;;
        --unit)
            RUN_UNIT_TESTS=true
            shift
            ;;
        --integration)
            RUN_INTEGRATION_TESTS=true
            shift
            ;;
        --all)
            RUN_UNIT_TESTS=true
            RUN_INTEGRATION_TESTS=true
            RUN_DOCKER_TESTS=true
            shift
            ;;
        --no-cleanup)
            CLEANUP_DOCKER=false
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Usage: $0 [--docker] [--unit] [--integration] [--all] [--no-cleanup] [-v|--verbose]"
            exit 1
            ;;
    esac
done

# If no specific test type selected, run all
if [ "$RUN_DOCKER_TESTS" = false ] && [ "$RUN_UNIT_TESTS" = false ] && [ "$RUN_INTEGRATION_TESTS" = false ]; then
    RUN_UNIT_TESTS=true
    RUN_INTEGRATION_TESTS=true
fi

# Run unit tests
if [ "$RUN_UNIT_TESTS" = true ]; then
    echo -e "\n${GREEN}Running unit tests...${NC}"
    if [ "$VERBOSE" = true ]; then
        go test -v -short ./...
    else
        go test -short ./...
    fi
    echo -e "${GREEN}✓ Unit tests passed${NC}"
fi

# Run integration tests with testcontainers
if [ "$RUN_INTEGRATION_TESTS" = true ]; then
    echo -e "\n${GREEN}Running integration tests (with testcontainers)...${NC}"
    echo "This will automatically spin up Kafka containers for testing..."
    
    if [ "$VERBOSE" = true ]; then
        go test -v -tags=integration ./core/store/... -timeout 5m
    else
        go test -tags=integration ./core/store/... -timeout 5m
    fi
    echo -e "${GREEN}✓ Integration tests passed${NC}"
fi

# Run Docker compose tests (full application in Docker)
if [ "$RUN_DOCKER_TESTS" = true ]; then
    echo -e "\n${GREEN}Running Docker compose tests...${NC}"
    
    # Check if docker-compose is available
    if ! command -v docker-compose &> /dev/null; then
        echo -e "${RED}docker-compose is not installed${NC}"
        exit 1
    fi
    
    # Start services
    echo "Starting services with docker-compose..."
    docker-compose -f docker-compose.test.yaml up -d --build
    
    # Wait for services to be healthy
    echo "Waiting for services to be healthy..."
    
    # Wait for Kafka
    MAX_RETRIES=30
    RETRY_COUNT=0
    until docker-compose -f docker-compose.test.yaml exec -T kafka-test /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-test:9092 --list &> /dev/null; do
        RETRY_COUNT=$((RETRY_COUNT + 1))
        if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
            echo -e "${RED}Kafka failed to start${NC}"
            docker-compose -f docker-compose.test.yaml logs kafka-test
            exit 1
        fi
        echo "Waiting for Kafka... ($RETRY_COUNT/$MAX_RETRIES)"
        sleep 2
    done
    echo -e "${GREEN}✓ Kafka is ready${NC}"
    
    # Wait for Franz app
    RETRY_COUNT=0
    until curl -s -f http://localhost:8080/health > /dev/null 2>&1; do
        RETRY_COUNT=$((RETRY_COUNT + 1))
        if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
            echo -e "${RED}Franz app failed to start${NC}"
            docker-compose -f docker-compose.test.yaml logs franz-app
            exit 1
        fi
        echo "Waiting for Franz app... ($RETRY_COUNT/$MAX_RETRIES)"
        sleep 2
    done
    echo -e "${GREEN}✓ Franz app is ready${NC}"
    
    # Test the API endpoints
    echo -e "\n${GREEN}Testing API endpoints...${NC}"
    
    # Test health endpoint
    if curl -s -f http://localhost:8080/health > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Health endpoint working${NC}"
    else
        echo -e "${RED}✗ Health endpoint failed${NC}"
        exit 1
    fi
    
    # Test cluster metadata endpoint
    if curl -s -f http://localhost:8080/api/metadata/clusters > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Clusters endpoint working${NC}"
    else
        echo -e "${YELLOW}⚠ Clusters endpoint returned error (might be expected if no clusters synced yet)${NC}"
    fi
    
    # Test topics metadata endpoint
    if curl -s -f http://localhost:8080/api/metadata/topics > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Topics endpoint working${NC}"
    else
        echo -e "${YELLOW}⚠ Topics endpoint returned error (might be expected if no topics synced yet)${NC}"
    fi
    
    # Show logs if verbose
    if [ "$VERBOSE" = true ]; then
        echo -e "\n${YELLOW}Application logs:${NC}"
        docker-compose -f docker-compose.test.yaml logs franz-app
    fi
    
    echo -e "\n${GREEN}✓ Docker compose tests passed${NC}"
    echo -e "${YELLOW}Services are still running. Use 'docker-compose -f docker-compose.test.yaml down' to stop them.${NC}"
    echo -e "${YELLOW}Or re-run with --no-cleanup flag to keep them running.${NC}"
fi

echo -e "\n${GREEN}=============================="
echo -e "All tests completed successfully!${NC}"

