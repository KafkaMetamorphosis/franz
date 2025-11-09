# Franz - Kafka Configuration Management System

Franz is a configuration management system for Kafka clusters and topics. It uses Kafka itself as a storage backend (via compacted topics) to store cluster definitions, topic templates, and their mappings, while providing REST APIs for CRUD operations and live queries to actual Kafka clusters.

![Project Overview](docs/imgs/overview.jpeg)

## Key Features

- **Configuration Storage**: Stores cluster and topic definitions in Kafka compacted topics
- **REST API**: Full CRUD operations for managing configurations
- **Live Queries**: Real-time queries to actual Kafka clusters for topics, brokers, and metadata
- **Kafka-Native**: Uses Kafka as its storage backend - no external databases required
- **Docker-Ready**: Fully containerized with Docker Compose support

## Architecture

Franz separates **configuration** (what you want) from **live state** (what exists):

1. **Configuration Storage** (stored in Kafka compacted topics):
   - Cluster definitions with bootstrap URLs and metadata
   - Topic templates/definitions with configuration parameters
   - Cluster-to-topic mappings

2. **Live Query Layer** (read from actual Kafka clusters):
   - Real-time topic discovery
   - Broker information
   - Topic metadata

### Storage Topics

Franz uses three Kafka compacted topics:
- `franz.clusters.store` - Cluster definitions
- `franz.topic.definitions.store` - Topic templates
- `franz.cluster.topics.store` - Cluster-to-topic mappings

## Quick Start

### Using Docker Compose

Start Franz with Kafka clusters:

```bash
# Start all services (Franz + 3 Kafka clusters + Kafka UI)
docker-compose up --build -d

# Check status
docker-compose ps

# View Franz logs
docker-compose logs -f franz-app

# Stop all services
docker-compose down
```

The stack includes:
- **Franz API** on port `8080`
- **Kafka Fleet** on port `19092` (used for storage)
- **Kafka-2** on port `29092`
- **Kafka-3** on port `39092`
- **Kafka UI** on port `8081` - Web interface to manage and monitor Kafka clusters

### Configuration

Franz can be configured using environment variables:

**Server Configuration:**
- `SERVER_PORT`: HTTP server port (default: `8080`)
- `SERVER_HOST`: HTTP server host (default: `0.0.0.0`)

**Storage Configuration:**
- `STORAGE_KAFKA_BROKERS`: Kafka brokers for storing Franz configuration (default: `localhost:19092`)
- `STORAGE_CLUSTERS_TOPIC`: Topic name for cluster definitions (default: `franz.clusters.store`)
- `STORAGE_DEFINITIONS_TOPIC`: Topic name for topic templates (default: `franz.topic.definitions.store`)
- `STORAGE_CLUSTER_TOPICS_TOPIC`: Topic name for cluster-topic mappings (default: `franz.cluster.topics.store`)

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

Franz provides REST APIs for both configuration management and live queries.

**Configuration Endpoints:**
- `POST /api/clusters` - Create cluster definition
- `GET /api/clusters` - List all cluster definitions
- `GET /api/cluster/{name}` - Get cluster definition
- `PUT /api/cluster/{name}` - Update cluster definition
- `DELETE /api/cluster/{name}` - Delete cluster definition
- `POST /api/topic_definitions` - Create topic template
- `GET /api/topic_definitions` - List topic templates
- `POST /api/cluster/{name}/topics` - Assign topic to cluster

**Live Query Endpoints:**
- `GET /api/cluster/{name}/live/topics` - Query actual topics from Kafka
- `GET /api/cluster/{name}/live/brokers` - Query actual brokers from Kafka
- `GET /api/cluster/{name}/live/topic/{topic}` - Query topic metadata from Kafka

**Health:**
- `GET /health` - Health check endpoint

For complete API documentation with examples, see [API.md](API.md).

### Example Workflow

```bash
# 1. Define a cluster
curl -X POST http://localhost:8080/api/clusters \
  -H "Content-Type: application/json" \
  -d '{
    "name": "production-cluster",
    "bootstrap_url": "kafka-fleet:9092",
    "metadata": {"environment": "production"}
  }'

# 2. Create a topic template
curl -X POST http://localhost:8080/api/topic_definitions \
  -H "Content-Type: application/json" \
  -d '{
    "name": "high-throughput",
    "metadata": {"partitions": "24", "replication": "3"}
  }'

# 3. Assign topic to cluster
curl -X POST http://localhost:8080/api/cluster/production-cluster/topics \
  -H "Content-Type: application/json" \
  -d '{
    "topic_name": "user-events",
    "topic_template": "high-throughput"
  }'

# 4. Query live cluster data
curl http://localhost:8080/api/cluster/production-cluster/live/topics
curl http://localhost:8080/api/cluster/production-cluster/live/brokers
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
