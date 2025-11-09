# Franz API Documentation

Franz is a Kafka configuration management system that stores cluster and topic definitions in Kafka compacted topics.

## Base URL

```
http://localhost:8080
```

## Overview

Franz provides two types of endpoints:

1. **Configuration Endpoints**: CRUD operations for managing cluster and topic definitions (stored in Kafka)
2. **Live Query Endpoints**: Read-only queries to actual Kafka clusters for real-time data

---

## Health Check

### GET /health

Check if the Franz service is running.

**Response:**
```json
{
  "status": "healthy"
}
```

---

## Cluster Management

### POST /api/clusters

Create or update a cluster definition.

**Request Body:**
```json
{
  "name": "production-cluster",
  "bootstrap_url": "kafka-prod.example.com:9092",
  "metadata": {
    "environment": "production",
    "region": "us-east-1",
    "tier": "high-performance"
  }
}
```

**Response:** 201 Created
```json
{
  "name": "production-cluster",
  "bootstrap_url": "kafka-prod.example.com:9092",
  "metadata": {
    "environment": "production",
    "region": "us-east-1",
    "tier": "high-performance"
  },
  "created_at": "2024-11-09T10:00:00Z",
  "updated_at": "2024-11-09T10:00:00Z"
}
```

### GET /api/clusters

List all cluster definitions.

**Response:** 200 OK
```json
{
  "clusters": [
    {
      "name": "production-cluster",
      "bootstrap_url": "kafka-prod.example.com:9092",
      "metadata": {
        "environment": "production"
      },
      "created_at": "2024-11-09T10:00:00Z",
      "updated_at": "2024-11-09T10:00:00Z"
    }
  ],
  "count": 1
}
```

### GET /api/cluster/{cluster_name}

Get a specific cluster definition.

**Response:** 200 OK
```json
{
  "name": "production-cluster",
  "bootstrap_url": "kafka-prod.example.com:9092",
  "metadata": {
    "environment": "production"
  },
  "created_at": "2024-11-09T10:00:00Z",
  "updated_at": "2024-11-09T10:00:00Z"
}
```

### PUT /api/cluster/{cluster_name}

Update an existing cluster definition.

**Request Body:**
```json
{
  "bootstrap_url": "kafka-prod-new.example.com:9092",
  "metadata": {
    "environment": "production",
    "region": "us-west-2"
  }
}
```

**Response:** 200 OK

### DELETE /api/cluster/{cluster_name}

Delete a cluster definition.

**Response:** 200 OK
```json
{
  "message": "Cluster deleted successfully",
  "name": "production-cluster"
}
```

---

## Topic Definitions

### POST /api/topic_definitions

Create or update a topic definition/template.

**Request Body:**
```json
{
  "name": "user-events-template",
  "metadata": {
    "partitions": "12",
    "replication_factor": "3",
    "retention_ms": "604800000",
    "compression_type": "lz4",
    "description": "Template for user event topics"
  }
}
```

**Response:** 201 Created

### GET /api/topic_definitions

List all topic definitions.

**Response:** 200 OK
```json
{
  "definitions": [
    {
      "name": "user-events-template",
      "metadata": {
        "partitions": "12"
      },
      "created_at": "2024-11-09T10:00:00Z",
      "updated_at": "2024-11-09T10:00:00Z"
    }
  ],
  "count": 1
}
```

### GET /api/topic_definition/{definition_name}

Get a specific topic definition.

**Response:** 200 OK

### PUT /api/topic_definition/{definition_name}

Update a topic definition.

**Request Body:**
```json
{
  "metadata": {
    "partitions": "24",
    "replication_factor": "3"
  }
}
```

**Response:** 200 OK

### DELETE /api/topic_definition/{definition_name}

Delete a topic definition.

**Response:** 200 OK
```json
{
  "message": "Topic definition deleted successfully",
  "name": "user-events-template"
}
```

---

## Cluster Topics

### POST /api/cluster/{cluster_name}/topics

Assign a topic to a cluster (create a mapping).

**Request Body:**
```json
{
  "topic_name": "user-events-prod",
  "topic_template": "user-events-template",
  "metadata": {
    "owner": "data-platform-team",
    "purpose": "user activity tracking"
  }
}
```

**Response:** 201 Created
```json
{
  "cluster_name": "production-cluster",
  "topic_name": "user-events-prod",
  "topic_template": "user-events-template",
  "metadata": {
    "owner": "data-platform-team"
  },
  "created_at": "2024-11-09T10:00:00Z",
  "updated_at": "2024-11-09T10:00:00Z"
}
```

### GET /api/cluster/{cluster_name}/topics

List all topics assigned to a cluster.

**Response:** 200 OK
```json
{
  "cluster": "production-cluster",
  "topics": [
    {
      "cluster_name": "production-cluster",
      "topic_name": "user-events-prod",
      "topic_template": "user-events-template",
      "metadata": {
        "owner": "data-platform-team"
      },
      "created_at": "2024-11-09T10:00:00Z",
      "updated_at": "2024-11-09T10:00:00Z"
    }
  ],
  "count": 1
}
```

### GET /api/cluster/{cluster_name}/topic/{topic_name}

Get a specific topic mapping.

**Response:** 200 OK

### PUT /api/cluster/{cluster_name}/topic/{topic_name}

Update a topic mapping.

**Request Body:**
```json
{
  "topic_template": "updated-template",
  "metadata": {
    "owner": "new-team"
  }
}
```

**Response:** 200 OK

### DELETE /api/cluster/{cluster_name}/topic/{topic_name}

Remove a topic mapping from a cluster.

**Response:** 200 OK
```json
{
  "message": "Topic removed from cluster successfully",
  "cluster": "production-cluster",
  "topic": "user-events-prod"
}
```

---

## Live Query Endpoints

These endpoints query actual Kafka clusters in real-time based on stored cluster definitions.

### GET /api/cluster/{cluster_name}/live/topics

Query actual topics from the Kafka cluster.

**Response:** 200 OK
```json
{
  "cluster": "production-cluster",
  "topics": {
    "user-events-prod": [
      {
        "id": 0,
        "leader": 1
      },
      {
        "id": 1,
        "leader": 2
      }
    ]
  },
  "count": 1
}
```

### GET /api/cluster/{cluster_name}/live/topic/{topic_name}

Query metadata for a specific topic from the Kafka cluster.

**Response:** 200 OK
```json
{
  "cluster": "production-cluster",
  "topic": "user-events-prod",
  "partition": 0,
  "leader": 1
}
```

### GET /api/cluster/{cluster_name}/live/brokers

Query actual brokers from the Kafka cluster.

**Response:** 200 OK
```json
{
  "cluster": "production-cluster",
  "brokers": [
    {
      "id": 1,
      "host": "kafka-broker-1.example.com",
      "port": 9092
    },
    {
      "id": 2,
      "host": "kafka-broker-2.example.com",
      "port": 9092
    }
  ],
  "count": 2
}
```

---

## Example Workflow

### 1. Define a Cluster

```bash
curl -X POST http://localhost:8080/api/clusters \
  -H "Content-Type: application/json" \
  -d '{
    "name": "production-cluster",
    "bootstrap_url": "kafka-prod.example.com:9092",
    "metadata": {
      "environment": "production",
      "region": "us-east-1"
    }
  }'
```

### 2. Create a Topic Definition

```bash
curl -X POST http://localhost:8080/api/topic_definitions \
  -H "Content-Type: application/json" \
  -d '{
    "name": "high-throughput-template",
    "metadata": {
      "partitions": "24",
      "replication_factor": "3",
      "compression_type": "lz4"
    }
  }'
```

### 3. Assign Topic to Cluster

```bash
curl -X POST http://localhost:8080/api/cluster/production-cluster/topics \
  -H "Content-Type: application/json" \
  -d '{
    "topic_name": "user-events",
    "topic_template": "high-throughput-template",
    "metadata": {
      "owner": "platform-team"
    }
  }'
```

### 4. Query Live Cluster Data

```bash
# Get all topics actually running on the cluster
curl http://localhost:8080/api/cluster/production-cluster/live/topics

# Get broker information
curl http://localhost:8080/api/cluster/production-cluster/live/brokers
```

---

## Error Responses

All errors follow this format:

```json
{
  "error": "Error message",
  "status": 400,
  "details": "Detailed error information"
}
```

Common HTTP status codes:
- `400` - Bad Request (invalid JSON, missing required fields)
- `404` - Not Found (cluster, topic, or definition not found)
- `405` - Method Not Allowed
- `500` - Internal Server Error

---

## Storage Backend

Franz uses three Kafka compacted topics to store configuration data:

- `franz.clusters.store` - Cluster definitions
- `franz.topic.definitions.store` - Topic templates
- `franz.cluster.topics.store` - Cluster-to-topic mappings

These topics can be configured via environment variables (see Configuration section in README.md).

