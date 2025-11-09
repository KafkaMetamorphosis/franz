# Franz Testing Guide

This document describes how to run tests for the Franz Kafka metadata management system.

## Overview

Franz has three levels of testing:

1. **Unit Tests**: Fast tests with mocked dependencies
2. **Integration Tests**: Tests with real Kafka using testcontainers
3. **Docker Compose Tests**: Full application stack in Docker

## Quick Start

Run all tests:
```bash
./scripts/test-integration.sh --all
```

Run only unit tests:
```bash
./scripts/test-integration.sh --unit
```

Run only integration tests:
```bash
./scripts/test-integration.sh --integration
```

Run Docker compose tests:
```bash
./scripts/test-integration.sh --docker
```

## Unit Tests

Unit tests use mocked Kafka clients and are fast to run. They don't require any external dependencies.

```bash
go test -short ./...
```

These tests run in CI/CD pipelines and during local development.

## Integration Tests

Integration tests use [testcontainers-go](https://golang.testcontainers.org/) to automatically spin up real Kafka containers. These tests verify that our code works correctly with actual Kafka instances.

### Running Integration Tests

```bash
# Run integration tests
go test -tags=integration ./core/store/... -timeout 5m

# With verbose output
go test -v -tags=integration ./core/store/... -timeout 5m
```

### Requirements

- Docker installed and running
- Docker daemon accessible to the current user
- Sufficient resources (Kafka containers require ~1GB RAM)

### What Integration Tests Cover

- **Full write/read cycles**: Write metadata to Kafka and verify it can be read back
- **Hostname resolution**: Tests that reproduce and verify the fix for "kafka-fleet: no such host" errors
- **SyncTopic flow**: End-to-end test simulating the production metadata sync flow
- **Data persistence**: Verify data survives store restarts
- **Concurrent operations**: Test multiple simultaneous writes
- **Tombstone handling**: Test deletion markers in compacted topics
- **Large payloads**: Test topics with many partitions

### How It Works

1. `SetupTestKafka()` uses testcontainers to start a Kafka instance
2. Tests get a broker address (e.g., `localhost:12345`)
3. `SetupTestMetadataStore()` creates a store connected to that broker
4. Tests run against real Kafka
5. Cleanup happens automatically when tests finish

## Docker Compose Tests

Docker compose tests run the full Franz application in Docker alongside Kafka, simulating a production-like environment. This is the most comprehensive test.

### Running Docker Compose Tests

```bash
# Run full Docker test suite
./scripts/test-integration.sh --docker

# Keep services running after tests (for debugging)
./scripts/test-integration.sh --docker --no-cleanup

# With verbose output
./scripts/test-integration.sh --docker -v
```

### What's Tested

- Franz application starts successfully in Docker
- Franz can connect to Kafka using internal Docker hostnames
- Health endpoint responds correctly
- API endpoints are accessible
- Metadata can be written to Kafka topics without hostname errors

### Manual Testing with Docker Compose

Start the services:
```bash
docker-compose -f docker-compose.test.yaml up --build
```

The services will be available at:
- Franz API: http://localhost:8080
- Kafka: localhost:29092 (from host)

Test the health endpoint:
```bash
curl http://localhost:8080/health
```

View logs:
```bash
# All services
docker-compose -f docker-compose.test.yaml logs -f

# Just Franz
docker-compose -f docker-compose.test.yaml logs -f franz-app

# Just Kafka
docker-compose -f docker-compose.test.yaml logs -f kafka-test
```

Stop services:
```bash
docker-compose -f docker-compose.test.yaml down -v
```

## Environment Variables

When running Franz (either locally or in Docker), you can configure it using environment variables:

### Kafka Configuration

- `KAFKA_BROKERS` or `KAFKA_FLEET_BOOTSTRAP_URLS`: Comma-separated list of Kafka brokers (default: `localhost:19092`)
- `PRIMARY_KAFKA_BROKERS` or `KAFKA_PRIMARY_DB_BOOTSTRAP_URLS`: Brokers for metadata storage (default: `localhost:19092`)

### Topic Configuration

- `CLUSTERS_TOPIC_NAME` or `KAFKA_METADATA_CLUSTERS_TOPIC`: Topic for cluster metadata (default: `franz.metadata.clusters`)
- `TOPICS_TOPIC_NAME` or `KAFKA_METADATA_TOPICS_TOPIC`: Topic for topic metadata (default: `franz.metadata.topics`)

### Server Configuration

- `SERVER_HOST`: Host to bind HTTP server (default: `0.0.0.0`)
- `SERVER_PORT`: Port for HTTP server (default: `8080`)

## Troubleshooting

### "no such host" Errors

If you see errors like `dial tcp: lookup kafka-fleet: no such host`, this means:

1. **From host machine**: The app is trying to connect to a Docker internal hostname. Use `localhost:19092` instead of `kafka-fleet:9092`
2. **From Docker**: The app can't resolve the Kafka hostname. Check that both services are on the same Docker network

**Solution**: Run Franz inside Docker using `docker-compose.test.yaml`, where internal hostnames work correctly.

### Integration Tests Fail to Start Kafka

If integration tests fail with "failed to start Kafka container":

1. Ensure Docker is running: `docker ps`
2. Check Docker resources (need ~1GB free RAM)
3. Try pulling the image manually: `docker pull apache/kafka:latest`
4. Check Docker logs for errors

### Docker Compose Services Won't Start

If services fail to start:

1. Check if ports are already in use:
   ```bash
   lsof -i :8080  # Franz port
   lsof -i :29092 # Kafka port
   ```

2. View service logs:
   ```bash
   docker-compose -f docker-compose.test.yaml logs
   ```

3. Rebuild images:
   ```bash
   docker-compose -f docker-compose.test.yaml build --no-cache
   docker-compose -f docker-compose.test.yaml up
   ```

### Tests Timeout

If tests timeout:

1. Increase timeout: `go test -timeout 10m ...`
2. Check system resources (Docker needs RAM)
3. Run with verbose output to see where it hangs: `go test -v ...`

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Go
        uses: actions/setup-go@v4
        with:
          go-version: '1.24'
      
      - name: Run unit tests
        run: go test -short ./...
      
      - name: Run integration tests
        run: go test -tags=integration ./core/store/... -timeout 5m
```

## Best Practices

1. **Run unit tests frequently** during development (they're fast)
2. **Run integration tests** before committing significant changes
3. **Run Docker tests** before releasing or when debugging hostname issues
4. **Use verbose mode** (`-v`) when debugging test failures
5. **Keep test Kafka instances isolated** - integration tests use separate topics
6. **Clean up Docker resources** regularly: `docker system prune -a`

## Writing New Tests

### Unit Test Example

```go
func TestSomething(t *testing.T) {
    // Use mock client
    mockClient := store.NewMockKafkaClient()
    store := &store.MetadataStore{
        client: mockClient,
        // ...
    }
    
    // Test your logic
    // ...
}
```

### Integration Test Example

```go
//go:build integration
// +build integration

func TestIntegration_Something(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping integration test")
    }
    
    ctx := context.Background()
    broker, cleanup := SetupTestKafka(t, ctx)
    defer cleanup()
    
    store, storeCleanup := SetupTestMetadataStore(t, ctx, broker)
    defer storeCleanup()
    
    // Test with real Kafka
    // ...
}
```

## Architecture Notes

### Why Three Test Levels?

1. **Unit tests** catch logic errors quickly without infrastructure
2. **Integration tests** verify Kafka interactions work correctly
3. **Docker tests** ensure deployment configuration is correct

### Hostname Resolution Strategy

Franz handles hostname resolution intelligently:

- **Inside Docker**: Uses internal hostnames directly (e.g., `kafka-test:9092`)
- **On host machine**: Maps internal hostnames to localhost ports (e.g., `kafka-fleet:9092` → `localhost:19092`)
- **Resolution happens automatically** in the `mapBrokerAddress` function in `kafka_client.go`

This dual-mode approach allows Franz to work in both development (on host) and production (in Docker) environments without code changes.

