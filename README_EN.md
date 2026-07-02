# pipeHub

> A lightweight Kafka → HTTP message forwarding middleware that delivers messages from Kafka topics to designated HTTP endpoints in real time.

> P.S. This project is currently a prototype. Support for real-time data streaming between multiple data components may be added in the future.

---

## Overview

pipeHub is a Spring Boot-based integration pipeline service. Its core responsibility is **bridging two endpoints (Kafka Source → HTTP Destination)**. It reads Pipeline rules defined in a JSON configuration file, automatically establishes Kafka Consumers on startup, continuously polls for messages, and forwards them to target endpoints via HTTP POST.

Use case: decoupled integration between the Kafka ecosystem and REST API services — transit data continuously without embedding the Kafka SDK.

---

## Features

- **Configuration-driven Pipelines**: Declaratively define Kafka → HTTP forwarding rules in `data.json`, with support for multiple Pipelines running in parallel.
- **Auto-recovery on Startup**: All Pipelines are automatically loaded and started once the application is ready — no manual intervention required.
- **Flexible Authentication**: Supports both `X-Proxy-Token` pass-through and `Authorization: Bearer <token>` HTTP authentication schemes.
- **HTTP Retry Mechanism**: Each message is retried up to 3 times with a 500ms interval on failure, improving delivery reliability.
- **External Configuration Priority**: Supports loading configuration from `pipeline/data.json` located alongside the JAR, enabling dynamic adjustments in production without repackaging.
- **Connection Pool Management**: Built-in Apache HttpClient connection pool (max 200 connections, 50 per route) for efficient HTTP connection reuse.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        pipeHub                                  │
│                                                                 │
│  ┌───────────────┐        ┌─────────────────────────────────┐   │
│  │PipelineManager│ ────▶  │KafkaHttpEndpointPipelineService │   │
│  │  (data.json)  │        └──────────────┬──────────────────┘   │
│  └───────────────┘                       │                      │
│                                          ▼                      │
│                         ┌─────────────────────────┐             │
│                         │ KafkaHttpEndpointBridge │             │
│                         │  (per pipeline)         │             │
│                         └────────────┬────────────┘             │
│                                      │                          │
│                                      ▼                          │
│                         ┌─────────────────────────┐             │
│                         │KafkaHttpEndpointChannel │             │
│                         │  [Daemon Thread]        │             │
│                         │  poll() & send()        │             │
│                         └────────────┬────────────┘             │
└──────────────────────────────────────┼──────────────────────────┘
                                       │
          ┌────────────────────────────┼──────────────────┐
          ▼                                               ▼
  ┌─────────────┐                              ┌───────────────┐
  │ Kafka Topic │                              │ HTTP Endpoint │
  └─────────────┘                              └───────────────┘
```

---

## Project Structure

```
src/main/java/com/github/pipeHub/
├── PipeHubApplication.java                   # Application entry point
├── controller/
│   └── TestController.java                   # Test receiver endpoint
├── enums/
│   └── AuthKeyEnum.java                      # Authentication header strategy enum
├── manage/
│   ├── HttpClientProvider.java               # HttpClient Bean factory (connection pool)
│   ├── PipelineInfo.java                     # Pipeline list + timestamp wrapper
│   └── PipelineManager.java                  # Configuration loading and management
├── model/
│   ├── Pipeline.java                         # Abstract base class for pipelines
│   ├── KafkaHttpEndpointPipeline.java        # Kafka → HTTP pipeline configuration model
│   ├── KafkaHttpEndpointBridge.java          # Pipeline holder, a pair of Kafka and http endpoint bridge
│   ├── KafkaHttpEndpointChannel.java         # KafkaHttpEndpointChannel is hold by KafkaHttpEndpointBridge, a kernel class of loop consuming transiting logic processing
│   ├── KafkaConfig.java                      # Kafka connection configuration
│   └── HttpEndPointConfig.java               # HTTP endpoint + authentication configuration
├── service/
│   ├── KafkaHttpEndpointPipelineService.java          # Service interface
│   ├── KafkaHttpEndpointPipelineRestorer.java         # Startup listener
│   └── impl/
│       └── KafkaHttpEndpointPipelineServiceImpl.java  # Service implementation
└── utils/
    ├── ApplicationContextUtil.java           # Static Spring context accessor
    └── JsonUtil.java                         # Jackson utility wrapper

src/main/resources/
├── application.yml                           # Main configuration (port 9494)
├── application-dev.yml                       # Development environment configuration
├── application-site.yml                      # Production environment configuration
└── static/
    └── data.json                             # Pipeline definition file (built-in default)
```

---

## Pipeline Configuration

The Pipeline definition file `data.json` has the following structure:

```json
[
  {
    "kafkaConfig": {
      "bootstrapServer": "your-kafka-host:your-kafka-port",
      "groupId": "your-consumer-group",
      "topic": "your-kafka-topic"
    },
    "httpEndPointConfig": {
      "url": "http://your-api-endpoint/path",
      "authKey": "your-http-endpoint-authKey"
    }
  }
]
```

| Field | Description                                                                               |
|---|-------------------------------------------------------------------------------------------|
| `kafkaConfig.bootstrapServer` | Kafka Broker address                                                                      |
| `kafkaConfig.groupId` | Consumer Group ID                                                                         |
| `kafkaConfig.topic` | Subscribed topic name                                                                     |
| `httpEndPointConfig.url` | Target HTTP endpoint URL                                                                  |
| `httpEndPointConfig.authKey` | Authentication header type (`Authorization` or `X-Proxy-Token`) can be expanded as needed |

**External configuration override**: Place `data.json` in a `pipeline/` folder alongside the JAR. The application will prioritize the external file, allowing Pipeline configuration changes without repackaging.

---

## Quick Start

### Prerequisites

- Java 8+
- Maven 3.6+
- An accessible Kafka Broker

### Build and Run

```bash
# Clone the repository
git clone https://github.com/DarrylHuang/pipeHub.git
cd pipeHub

# Build (produces a fat JAR with all dependencies)
mvn clean package

# Run
java -jar target/pipeHub-*.jar --spring.profiles.active=dev
```

### Custom Pipeline Configuration

```bash
# Create the config folder alongside the JAR
mkdir pipeline

# Place your custom data.json
cp your-data.json pipeline/data.json

# Start the application — external configuration is loaded automatically
java -jar pipeHub.jar
```

---

## Tech Stack

| Category | Technology | Version |
|---|---|---|
| Framework | Spring Boot | 2.6.13 |
| Language | Java | 1.8 |
| Build | Maven | 3.x |
| HTTP Client | Apache HttpComponents | 4.5.10 |
| Message Queue | Apache Kafka Clients | 3.4.1 |
| JSON Processing | Jackson | (bundled with Spring Boot) |
| Utilities | Lombok, Commons Lang3, Commons Collections4 | - |

---

## Design Highlights

### 1. Enum Strategy Pattern (`AuthKeyEnum`)

Authentication header handling logic is encapsulated within enum constants. Each constant implements the `handleAuth()` abstract method, realizing a clean strategy pattern:

```java
Authorization {
    @Override
    public String handleAuth(String token) {
        return "Bearer " + token;
    }
}
```

Adding a new authentication type only requires a new enum constant — no changes to calling code needed.

### 2. Dual-source Configuration Loading (`PipelineManager`)

An external-first, built-in-fallback loading strategy that balances deployment flexibility with an out-of-the-box experience:

```
pipeline/data.json (alongside JAR)  ──priority──▶  load external config
        ↓ not found
classpath:/static/data.json         ──fallback──▶  load built-in config
```

### 3. Extensible Pipeline Abstraction

The `Pipeline` abstract base class + `KafkaHttpEndpointPipeline` concrete implementation hierarchy leaves extension points for future pipeline types (e.g., Kafka→Kafka, HTTP→Kafka).

---

## Known Issues and Improvement Suggestions

| Severity | Issue | Recommendation |
|------|---|---|
| High | SSL trusts all certificates (`TrustAll` + `NoopHostnameVerifier`) | Configure a proper certificate trust chain |
| High | Auth tokens stored in plaintext in the configuration file | Use environment variables or a secrets management service |
| Medium | Bare threads used instead of `ExecutorService` | Switch to a thread pool for better lifecycle management and graceful shutdown |
| Medium | `enable.auto.commit=true` coexists with manual `commitSync()` | Disable auto-commit and use manual commit exclusively |
| Medium | `analyseMessage` empty-string check is inconsistent with caller-side `null` checks | Standardize on returning `null` for invalid messages |

---

## License

[MIT](LICENSE)
