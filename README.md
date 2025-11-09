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

## Development

### Building from Source

```bash
# Build the application
go build -o franz .

# Run locally (requires Kafka running)
STORAGE_KAFKA_BROKERS=localhost:19092 ./franz
```

### Running with Docker

```bash
# Build Docker image
docker build -t franz:latest .

# Run with docker-compose (recommended)
docker-compose up --build
```

## Project Structure

```
franz/
├── core/
│   ├── config/         # Configuration loading
│   ├── handlers/       # HTTP request handlers
│   │   ├── cluster_handlers.go            # Cluster CRUD
│   │   ├── topic_definition_handlers.go   # Topic definition CRUD
│   │   ├── cluster_topic_handlers.go      # Cluster-topic mappings
│   │   └── live_query_handlers.go         # Live Kafka queries
│   ├── kafka/          # Kafka admin operations
│   ├── models/         # Data models
│   │   ├── cluster.go
│   │   ├── topic_definition.go
│   │   └── cluster_topic.go
│   └── store/          # Storage layer (Kafka compacted topics)
│       ├── config_store.go
│       ├── cluster_store.go
│       ├── topic_definition_store.go
│       └── cluster_topic_store.go
├── main.go             # Application entry point
├── docker-compose.yaml # Development environment
├── Dockerfile          # Container image
├── API.md              # Complete API documentation
└── README.md           # This file
```

## Use Cases

1. **Multi-Cluster Management**: Define and manage multiple Kafka clusters with metadata
2. **Topic Standardization**: Create topic templates for consistent topic configurations
3. **Environment Tracking**: Track which topics should exist in which clusters
4. **Discovery**: Query actual state of Kafka clusters without direct access
5. **Configuration Audit**: Keep track of cluster and topic configurations over time

## License

See [LICENSE](LICENSE) file for details.
