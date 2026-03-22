# Franz — Kafka Fleet Management

Franz is the control plane of the **KafkaMetamorphosis** platform. It allows teams to declaratively manage Kafka clusters, topic configurations, and topic definitions through a REST API.

## 1. What This Strives to Solve

Managing Kafka at scale across multiple clusters is painful. Teams end up with ad-hoc scripts, tribal knowledge, and no single source of truth for what topics exist, how they are configured, and which clusters they belong to.

Franz solves this by providing:

- A **central registry** for Kafka clusters and their topic configurations
- A **declarative API** to define topics and their desired state
- A **reconciliation model** — operators express intent, and the platform drives actual Kafka clusters toward that state

## 2. How It Works

Franz is the **control plane**: it stores and validates the desired state of your Kafka infrastructure. It does not talk to Kafka directly. A companion service, **Gregor Samsa**, acts as the per-cluster reconciler — it reads Franz's state and applies it to the actual Kafka clusters.

```
Client → Franz (REST API + PostgreSQL) ← Gregor Samsa → Kafka Cluster
```

For a detailed explanation of the domain model, entities, state machines, and architecture decisions, refer to the [KafkaMetamorphosis docs](../docs).

## 3. How to Run Locally

Franz uses [Leiningen](https://leiningen.org/) and requires PostgreSQL.

```bash
# Start dependencies (PostgreSQL)
make deps

# Run database migrations
make migrate

# Start the server
make run
```

Other useful targets:

```bash
make seed         # Add some local db data to local run
make unit         # Run unit tests
make integration  # Run integration tests
make test         # Run all tests
make stop-deps    # Stop dependencies
make reset-db     # Drop and recreate the database
```

The server starts on `http://localhost:8080` by default.

## 4. HTTP Routes

### Operations


| Method | Path               | Description                        |
| ------ | ------------------ | ---------------------------------- |
| `GET`  | `/ops/health`      | Basic health check                 |
| `GET`  | `/ops/liveness`    | Liveness probe                     |
| `GET`  | `/ops/readiness`   | Readiness probe                    |
| `GET`  | `/ops/config/dump` | Dump current runtime configuration |


### Clusters


| Method   | Path                             | Description                   |
| -------- | -------------------------------- | ----------------------------- |
| `GET`    | `/api/v0/clusters`               | List all clusters (paginated) |
| `POST`   | `/api/v0/clusters`               | Register a new cluster        |
| `GET`    | `/api/v0/clusters/:cluster-name` | Get a specific cluster        |
| `PUT`    | `/api/v0/clusters/:cluster-name` | Update a cluster              |
| `DELETE` | `/api/v0/clusters/:cluster-name` | Delete a cluster              |


### Topic Configurations

Topic configurations define the default Kafka settings (partitions, replication factor, retention, etc.) that can be attached to clusters or topic definitions.


| Method   | Path                                                   | Description                               |
| -------- | ------------------------------------------------------ | ----------------------------------------- |
| `GET`    | `/api/v0/topic_configurations`                         | List all topic configurations (paginated) |
| `POST`   | `/api/v0/topic_configurations`                         | Create a topic configuration              |
| `GET`    | `/api/v0/topic_configurations/:topic-configuration-id` | Get a specific configuration              |
| `PUT`    | `/api/v0/topic_configurations/:topic-configuration-id` | Update a configuration                    |
| `DELETE` | `/api/v0/topic_configurations/:topic-configuration-id` | Delete a configuration                    |


All list endpoints accept `page` and `size` query parameters.

## 5. How to Contribute

This project is in active development. If you want to contribute, report issues, or discuss ideas, feel free to get in touch:

- **Email**: [ronierison.silva@gmail.com](mailto:ronierison.silva@gmail.com)
- **LinkedIn**: [linkedin.com/in/joseronierison](https://www.linkedin.com/in/joseronierison)

