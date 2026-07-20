# pipeHub TODO Feature Notes

## Background

pipeHub currently loads Kafka to HTTP pipeline configuration from `data.json`.
This is simple, but it has two limitations:

- configuration maintenance is file based and not convenient for multiple pipelines;
- pipeline lifecycle is bound to the Spring Boot process lifecycle: application start means all configured transmissions start, and application stop means all transmissions stop.

The target design is to store pipeline configuration and lifecycle state in an embedded database, then allow operations staff to start, stop, list, and inspect individual pipelines from a Linux command line.

## Recommended Architecture

Use a client-server model.

```text
pipeHub Server
  Spring Boot process
  Kafka -> HTTP transmission runtime
  Embedded H2 database
  Local management HTTP API

pipehub Client
  Linux command line tool
  Sends management requests to pipeHub Server
```

The CLI client should not run pipeline logic itself. It should only call the running server.

Example user experience:

```bash
pipehub list
pipehub status order-to-crm
pipehub start order-to-crm
pipehub stop order-to-crm
pipehub restart order-to-crm
pipehub shell
```

Optional interactive shell:

```text
pipehub> list
pipehub> stop order-to-crm
pipehub> start user-event-to-api
pipehub> exit
```

## Server-Side Recommendation

Use Spring Boot as a long-running backend service. It should:

- reconcile persisted desired pipeline state on application startup;
- expose local management APIs for pipeline operations;
- persist pipeline configuration and lifecycle state to an embedded database;
- keep currently running pipelines in an in-memory runtime registry.

Recommended embedded database:

- H2 file mode
- JDBC/JdbcTemplate first
- avoid `jdbc:h2:mem:` because pipeline state must survive process restart

Example datasource:

```properties
spring.datasource.url=jdbc:h2:file:./pipeline/pipehub;DB_CLOSE_ON_EXIT=FALSE
spring.datasource.driver-class-name=org.h2.Driver
spring.datasource.username=sa
spring.datasource.password=
```

Recommended server dependencies:

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-jdbc</artifactId>
</dependency>

<dependency>
    <groupId>com.h2database</groupId>
    <artifactId>h2</artifactId>
    <scope>runtime</scope>
</dependency>

<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-actuator</artifactId>
</dependency>
```

`spring-boot-starter-actuator` is optional, but useful for health checks and service status.

## Pipeline Runtime Model

Introduce a runtime manager in the server:

```text
PipelineRepository
        |
        v
PipelineRuntimeManager
        |
        +-- start(id)
        +-- stop(id)
        +-- restart(id)
        +-- list()
        +-- reconcileDesiredState()
```

The runtime manager should maintain a map of active pipelines:

```text
Map<String, KafkaHttpEndpointBridge>
```

Current code already has a useful stop point:

```java
KafkaHttpEndpointChannel.shutdown()
```

The bridge should expose a corresponding `shutdown()` method and delegate to the channel.

Suggested lifecycle behavior:

```text
Application startup
  -> query desired_status from H2
  -> start pipelines that should be RUNNING
  -> stop or keep stopped pipelines that should be STOPPED
  -> register active bridge in memory

pipehub stop <id>
  -> call server management API
  -> server updates desired_status = STOPPED
  -> background reconciler stops the active runtime
  -> update runtime_status = STOPPED after shutdown succeeds

pipehub start <id>
  -> call server management API
  -> server updates desired_status = RUNNING
  -> background reconciler starts the runtime
  -> update runtime_status = RUNNING after startup succeeds
```

## Desired State and Reconciler

The long-term lifecycle design should use a desired-state model instead of making the REST request directly own the whole start or stop operation.

Core idea:

```text
database state: what operators want
runtime state:  what is actually running in memory
reconciler:     compares both sides and moves runtime state toward desired state
```

For example:

```text
desired_status = RUNNING
runtime_status = STOPPED
```

The reconciler should start that pipeline.

Another example:

```text
desired_status = STOPPED
runtime_status = RUNNING
```

The reconciler should stop that pipeline.

Recommended request flow:

```text
CLI -> REST API -> persist desired_status -> return 202 Accepted
Reconciler -> compare desired state and actual runtime -> start or stop runtime -> persist runtime_status
```

Recommended trigger strategy:

```text
Primary path: command-triggered single-pipeline reconcile
Startup path: full reconcile once when the Spring Boot application starts
Fallback path: low-frequency scheduled full reconcile
```

The reconciler does not need to depend on high-frequency polling.
When an operator runs `pipehub start <id>` or `pipehub stop <id>`, the server should persist `desired_status` first, then immediately request reconciliation for that single pipeline after the database transaction commits.

```text
POST /admin/pipelines/{id}/stop
  -> update desired_status = STOPPED
  -> after commit
  -> reconciler.request(id)
  -> return 202 Accepted
```

Sketch:

```java
public void request(String pipelineId) {
    executor.submit(() -> reconcileOne(pipelineId));
}
```

A low-frequency scheduled task should still exist as a recovery mechanism rather than the main driver:

```java
@Scheduled(fixedDelay = 30000)
public void reconcileAll() {
    repository.findAll().forEach(p -> request(p.getId()));
}
```

The scheduled task is useful for:

- recovering after the process crashes during start or stop;
- correcting occasional differences between database state and in-memory runtime state;
- detecting manual database changes if operators bypass the API;
- deciding whether to restart a failed pipeline whose `desired_status` is still `RUNNING`.

For this project, a 30-second or 60-second full reconcile is enough in most cases.
The expected number of pipelines is likely much smaller than the cost of Kafka consumers and HTTP delivery, so the H2 table scan itself should not be the performance bottleneck.

This is better than a synchronous Spring event as the main control flow because:

- operator commands become idempotent;
- HTTP requests do not block on Kafka startup or shutdown;
- process crashes during start or stop can be recovered after restart;
- the database remains the source of truth for operator intent;
- lifecycle logic is explicit in `PipelineLifecycleService` and `PipelineRuntimeManager`.

Suggested classes:

```text
PipelineCommandController
  -> PipelineDesiredStateService
      -> PipelineRepository

PipelineReconciler
  -> PipelineRepository
  -> PipelineRuntimeManager
  -> PipelineLifecycleService
```

`PipelineCommandController` should only validate the request and update `desired_status`.
`PipelineReconciler` should run periodically, compare database desired state with actual in-memory runtime state, and execute start or stop work.

Sketch:

```java
@Scheduled(fixedDelay = 3000)
public void reconcile() {
    List<PipelineRecord> pipelines = repository.findAll();

    for (PipelineRecord pipeline : pipelines) {
        RuntimeState actual = runtimeManager.getActualState(pipeline.getId());

        if (pipeline.wantRunning() && !actual.isRunningOrStarting()) {
            lifecycleService.startRuntime(pipeline);
        }

        if (pipeline.wantStopped() && actual.isRunningOrStarting()) {
            lifecycleService.stopRuntime(pipeline.getId());
        }
    }
}
```

Start and stop operations should still be executed through a bounded `ExecutorService`, so one slow Kafka connection does not block reconciliation for all pipelines.

Spring events can still be useful, but they should be treated as side effects rather than the main lifecycle engine:

```text
PipelineStartedEvent
PipelineStoppedEvent
PipelineFailedEvent
PipelineStatusChangedEvent
```

Good event listener responsibilities:

- audit logs;
- metrics;
- alerts;
- WebSocket status push.

Avoid putting the core start, stop, rollback, and persistence flow inside event listeners.

## Suggested Database Table

Start with one table, then split later if needed.

```sql
CREATE TABLE pipeline_config (
    id VARCHAR(128) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(64) NOT NULL,
    desired_status VARCHAR(32) NOT NULL,
    runtime_status VARCHAR(32) NOT NULL,
    last_error CLOB,
    version BIGINT NOT NULL,
    updated_by VARCHAR(128),
    kafka_config_json CLOB NOT NULL,
    http_config_json CLOB NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    last_started_at TIMESTAMP,
    last_stopped_at TIMESTAMP
);
```

Recommended status values:

```text
desired_status:
RUNNING
STOPPED

runtime_status:
STARTING
RUNNING
STOPPING
STOPPED
FAILED
```

`desired_status` should mean the operator's persistent intent and replaces the simpler `enabled` flag.

`runtime_status` should describe the current or latest runtime state.

`version` should be used for optimistic locking or lost-update protection when multiple operators issue commands at nearly the same time.

## Management API Shape

Expose APIs on a local-only management port or bind address.

Example:

```text
GET  /admin/pipelines
GET  /admin/pipelines/{id}
POST /admin/pipelines/{id}/start
POST /admin/pipelines/{id}/stop
POST /admin/pipelines/{id}/restart
```

Recommended binding:

```properties
server.address=127.0.0.1
```

or use a dedicated management port if needed.

For production, protect the management API with at least one of:

- local-only bind address;
- token authentication;
- mTLS;
- firewall rules.

## CLI Client Language Choice

The planned direction is to implement the Linux CLI client in Go.

Reasoning:

- Go produces a single static-style Linux binary;
- no JRE or Python runtime is required on the target machine;
- deployment is simple: copy one `pipehub` binary to `/usr/local/bin`;
- Go is widely used for infrastructure CLI tools;
- Cobra is a common Go CLI framework for subcommands, flags, help text, and shell completion.

Recommended Go libraries:

```text
github.com/spf13/cobra       command structure
github.com/spf13/viper       optional config file/env support
github.com/chzyer/readline   optional interactive shell
```

For a first Go CLI version, even the Go standard library is enough:

```text
net/http
encoding/json
flag
os
fmt
```

Use Cobra when the command tree becomes larger.

## Suggested Repository Structure

If moving to a multi-module Java server plus Go CLI, one possible structure is:

```text
pipeHub/
  pom.xml
  pipehub-server/
    pom.xml
    src/main/java/...
  pipehub-common/
    pom.xml
    src/main/java/...
  pipehub-cli/
    go.mod
    cmd/pipehub/
    internal/client/
    internal/shell/
```

However, because the CLI is Go, `pipehub-common` cannot be directly reused by the Go client.
The shared contract should instead be an HTTP API contract:

```text
OpenAPI spec or documented JSON request/response DTOs
```

Pragmatic approach:

1. Keep the current Java project as the server first.
2. Add H2 persistence and management REST APIs.
3. Add a `client-go/` or `pipehub-cli/` directory for the Go CLI.
4. Document request/response JSON clearly.
5. Generate clients from OpenAPI later only if the API grows.

## Suggested Implementation Phases

### Phase 1: Server Persistence

- Add H2 and Spring JDBC.
- Create `pipeline_config` table.
- Migrate `data.json` pipelines into the database.
- Add `id`, `desired_status`, `runtime_status`, and `version` fields to the pipeline model.

### Phase 2: Runtime Lifecycle

- Add `PipelineRuntimeManager`.
- Track active pipelines by id.
- Add `start(id)`, `stop(id)`, `restart(id)`, and `list()` behavior.
- Add `shutdown()` to `KafkaHttpEndpointBridge`.
- Ensure stop wakes and closes the Kafka consumer safely.

### Phase 3: Management API

- Add local-only REST endpoints.
- Return stable JSON response objects.
- Add simple authentication if the port is reachable beyond localhost.

### Phase 4: Go CLI

- Create `pipehub-cli/` with Go module.
- Implement commands:
  - `pipehub list`
  - `pipehub status <id>`
  - `pipehub start <id>`
  - `pipehub stop <id>`
  - `pipehub restart <id>`
  - `pipehub shell`
- Add config resolution:
  - command flags;
  - environment variables;
  - optional config file such as `/etc/pipehub/pipehub-cli.yaml`.

### Phase 5: Linux Packaging

- Build a Linux binary named `pipehub`.
- Install it to `/usr/local/bin/pipehub`.
- Run the server as a `systemd` service.
- Keep CLI and server version information visible:

```bash
pipehub version
```

## Final Recommendation

Build the server as a stable Spring Boot backend with H2 persistence and local management APIs.
Build the operational client as a Go-based Linux CLI.

This gives operations staff the shell experience they want while keeping the actual pipeline runtime independent from terminal sessions, SSH state, and service startup mode.

