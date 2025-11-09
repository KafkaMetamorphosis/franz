# Franz Kafka

Franz is a fleet wide Kafka cluster project that manages a pool of Kafka cluster and the Kafka topics, users and quotas, providing a topic allocation given the defined affinities among clusters and topics and a traffic control mechanism.

![Project Overview](docs/imgs/overview.jpeg)

## The cluster manager

The cluster manager is responsible for keeping the cluster configuration given the configuration.

The dimensions
```yaml

apiVersion: kafka.franz.io/v1beta2
kind: KafkaTopology
metadata:
  name: organization
spec:
    name: org-name
---
apiVersion: kafka.franz.io/v1beta2
kind: KafkaTopology
metadata:
  name: country
spec:
    parent:
        kind: organization
        name: org-name
    name: br
---
apiVersion: kafka.franz.io/v1beta2
kind: KafkaTopology
metadata:
  name: country
spec:
    parent:
        kind: organization
        name: org-name
    name: mx
---
apiVersion: kafka.franz.io/v1beta2
kind: KafkaTopology
metadata:
  name: environment
spec:
    parent:
        kind: country
    name: staging
---
apiVersion: kafka.franz.io/v1beta2
kind: KafkaTopology
metadata:
  name: environment
spec:
    parent:
        kind: country
    name: prod
---
apiVersion: kafka.franz.io/v1beta2
kind: KafkaTopology
metadata:
  name: global
spec:
    parent:
        kind: environment
    name: s0
---
apiVersion: kafka.franz.io/v1beta2
kind: KafkaTopology
metadata:
  name: shard
spec:
    parent:
        kind: environment
    name: s0
---
apiVersion: kafka.franz.io/v1beta2
kind: KafkaTopology
metadata:
  name: shard
spec:
    parent:
        kind: environment
        name: prod
    name: s1
---
apiVersion: kafka.franz.io/v1beta2
kind: KafkaTopology
metadata:
  name: flavor
spec:
    name: standard-0
---
apiVersion: kafka.franz.io/v1beta2
kind: KafkaTopology
metadata:
  name: flavor
spec:
    name: high-volume-0
---
apiVersion: kafka.franz.io/v1beta2
kind: KafkaTopology
metadata:
  name: flavor
spec:
    name: high-volume-1
---
apiVersion: kafka.franz.io/v1beta2
kind: KafkaTopology
metadata:
  name: flavor
spec:
    name: low-latency-1
```

The kafka template
```yaml
apiVersion: kafka.franz.io/v1beta2
kind: KafkaTemplate
metadata:
  name: low-latency
spec:
  kafka:
---
apiVersion: kafka.franz.io/v1beta2
kind: KafkaTemplate
metadata:
  name: standard
spec:
  kafka:
---
apiVersion: kafka.franz.io/v1beta2
kind: KafkaTemplate
metadata:
  name: high-volume
spec:
  kafka:
---
apiVersion: kafka.franz.io/v1beta2
kind: KafkaTemplate
metadata:
  name: staging
spec:
  kafka:
```

The kafka pool
```yaml
apiVersion: kafka.franz.io/v1beta2
kind: KafkaPool
metadata:
  name: standard-pool
spec:
  template: high-volume
  selectors:
    countries:
        - br
        - mx
    environments:
        - prod
    flavors: 
        - "*"
  kafka:
```

## The resource manager

## The traffic manager

### The traffic coordinator 

## The governance director

## HTTP Server

The HTTP server provides a REST API to retrieve information from Kafka admin operations.

### Running Kafka Cluster

A Docker Compose file is provided to spin up a local Kafka cluster for development and testing:

```bash
# Start Kafka cluster (Zookeeper + Kafka + Kafka UI)
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f kafka

# Stop the cluster
docker-compose down
```

The cluster includes:
- **Zookeeper** on port `2181`
- **Kafka broker** on port `9092` (for external clients) and `9093`
- **Kafka UI** on port `8081` - Web interface to manage and monitor Kafka

### Configuration

The server can be configured using environment variables:

- `SERVER_PORT`: HTTP server port (default: `8080`)
- `SERVER_HOST`: HTTP server host (default: `0.0.0.0`)
- `KAFKA_BROKERS`: Comma-separated list of Kafka broker addresses (default: `localhost:9092`)

### Building and Running

```bash
# Build the server
go build .

# Run the server
./server

# Or with custom configuration
KAFKA_BROKERS=localhost:9092 SERVER_PORT=8080 ./server
```

### API Endpoints

- `GET /health` - Health check endpoint
- `GET /api/topics` - List all topics in the cluster
- `GET /api/topics/{topic}` - Get metadata for a specific topic
- `GET /api/brokers` - Get broker information
- `GET /api/cluster` - Get cluster metadata

### Example Usage

```bash
# Health check
curl http://localhost:8080/health

# List all topics
curl http://localhost:8080/api/topics

# Get specific topic
curl http://localhost:8080/api/topics/my-topic

# Get brokers
curl http://localhost:8080/api/brokers

# Get cluster metadata
curl http://localhost:8080/api/cluster
```

## Testing

Franz includes comprehensive testing at multiple levels:

- **Unit Tests**: Fast tests with mocked dependencies
- **Integration Tests**: Tests with real Kafka using testcontainers
- **Docker Tests**: Full application stack in Docker

### Quick Start

```bash
# Run all tests
./scripts/test-integration.sh --all

# Run only unit tests
./scripts/test-integration.sh --unit

# Run only integration tests
./scripts/test-integration.sh --integration

# Run Docker compose tests
./scripts/test-integration.sh --docker
```

For detailed testing documentation, see [TESTING.md](TESTING.md).

## Development

### Running with Docker Compose for Testing

For development and testing, use the test Docker Compose setup:

```bash
# Start Franz app with Kafka
docker-compose -f docker-compose.test.yaml up --build

# The services will be available at:
# - Franz API: http://localhost:8080
# - Kafka: localhost:29092
```

This setup runs Franz inside Docker, which properly handles internal Kafka hostnames and simulates a production-like environment.
