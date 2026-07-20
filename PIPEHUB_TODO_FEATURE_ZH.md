# pipeHub 待办功能说明

## 背景

pipeHub 当前通过 `data.json` 加载 Kafka 到 HTTP 的传输线配置。
这种方式实现简单，但存在两个明显限制：

- 配置维护依赖文件，不方便管理多条传输线；
- 传输线生命周期和 Spring Boot 进程生命周期绑定：应用启动则所有配置的传输线启动，应用停止则所有传输线停止。

目标设计是将传输线配置和生命周期状态保存到内嵌数据库中，然后让运维人员可以通过 Linux 命令行启动、停止、查看和维护单条传输线。

## 推荐架构

采用客户端-服务端模型。

```text
pipeHub Server
  Spring Boot 进程
  Kafka -> HTTP 传输运行时
  内嵌 H2 数据库
  本地管理 HTTP API

pipehub Client
  Linux 命令行工具
  向 pipeHub Server 发送管理请求
```

CLI 客户端不应该承载传输逻辑。它只负责调用正在运行的服务端。

示例使用方式：

```bash
pipehub list
pipehub status order-to-crm
pipehub start order-to-crm
pipehub stop order-to-crm
pipehub restart order-to-crm
pipehub shell
```

可选的交互式 shell：

```text
pipehub> list
pipehub> stop order-to-crm
pipehub> start user-event-to-api
pipehub> exit
```

## 服务端建议

继续将 Spring Boot 作为常驻后台服务。服务端职责包括：

- 应用启动时根据数据库中的期望状态恢复传输线；
- 暴露本地管理 API，用于传输线运维操作；
- 将传输线配置和生命周期状态持久化到内嵌数据库；
- 在内存中维护当前正在运行的传输线注册表。

推荐内嵌数据库：

- H2 文件模式；
- 优先使用 JDBC/JdbcTemplate；
- 避免使用 `jdbc:h2:mem:`，因为传输线状态需要在进程重启后保留。

示例数据源配置：

```properties
spring.datasource.url=jdbc:h2:file:./pipeline/pipehub;DB_CLOSE_ON_EXIT=FALSE
spring.datasource.driver-class-name=org.h2.Driver
spring.datasource.username=sa
spring.datasource.password=
```

推荐服务端依赖：

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

`spring-boot-starter-actuator` 不是必须项，但适合做健康检查和服务状态暴露。

## 传输线运行时模型

服务端新增运行时管理器：

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

运行时管理器需要维护当前活跃传输线：

```text
Map<String, KafkaHttpEndpointBridge>
```

当前代码中已经有一个可用的停止入口：

```java
KafkaHttpEndpointChannel.shutdown()
```

建议在 `KafkaHttpEndpointBridge` 中也暴露对应的 `shutdown()` 方法，并委托给内部 channel。

建议生命周期行为：

```text
应用启动
  -> 从 H2 查询 desired_status
  -> 启动期望状态为 RUNNING 的传输线
  -> 停止或保持停止期望状态为 STOPPED 的传输线
  -> 将活跃 bridge 注册到内存

pipehub stop <id>
  -> 调用服务端管理 API
  -> 服务端更新 desired_status = STOPPED
  -> 后台 Reconciler 停止活跃运行时
  -> 停止成功后更新 runtime_status = STOPPED

pipehub start <id>
  -> 调用服务端管理 API
  -> 服务端更新 desired_status = RUNNING
  -> 后台 Reconciler 启动运行时
  -> 启动成功后更新 runtime_status = RUNNING
```

## Desired State + Reconciler 方案

长期生命周期管理建议采用期望状态模型，而不是让 REST 请求同步负责完整的启动或停止过程。

核心思想：

```text
数据库状态：运维人员希望传输线处于什么状态
内存状态：  传输线当前实际是否正在运行
Reconciler：比较两边状态，并把实际运行状态拉齐到期望状态
```

例如：

```text
desired_status = RUNNING
runtime_status = STOPPED
```

Reconciler 应该启动这条传输线。

另一个例子：

```text
desired_status = STOPPED
runtime_status = RUNNING
```

Reconciler 应该停止这条传输线。

推荐请求流程：

```text
CLI -> REST API -> 持久化 desired_status -> 返回 202 Accepted
Reconciler -> 对比期望状态和实际运行状态 -> 启动或停止运行时 -> 持久化 runtime_status
```

推荐触发策略：

```text
主路径：命令触发单条 pipeline reconcile
启动路径：Spring Boot 应用启动时全量 reconcile 一次
兜底路径：低频定时全量 reconcile
```

Reconciler 不需要依赖高频轮询。
当运维人员执行 `pipehub start <id>` 或 `pipehub stop <id>` 时，服务端应该先持久化 `desired_status`，然后在数据库事务提交后立即请求协调这一条传输线。

```text
POST /admin/pipelines/{id}/stop
  -> update desired_status = STOPPED
  -> after commit
  -> reconciler.request(id)
  -> return 202 Accepted
```

示意代码：

```java
public void request(String pipelineId) {
    executor.submit(() -> reconcileOne(pipelineId));
}
```

低频定时任务仍然建议保留，但它的定位是恢复兜底，而不是主驱动：

```java
@Scheduled(fixedDelay = 30000)
public void reconcileAll() {
    repository.findAll().forEach(p -> request(p.getId()));
}
```

定时任务的价值在于：

- 启动或停止过程中进程崩溃后，可以继续恢复；
- 数据库状态和内存运行状态偶发不一致时，可以自动修正；
- 如果运维绕过 API 直接修改 H2 数据库，也能被发现；
- 某条传输线异常失败后，如果 `desired_status` 仍然是 `RUNNING`，可以决定是否自动拉起。

对当前项目来说，30 秒或 60 秒做一次全量 reconcile 通常已经足够。
传输线数量大概率远小于 Kafka consumer 和 HTTP 投递本身的运行成本，因此 H2 配置表的低频扫描不应成为性能瓶颈。

这个方案比用同步 SpringEvent 作为主流程更适合，因为：

- 运维命令具备幂等性；
- HTTP 请求不会被 Kafka 启动或停止过程阻塞；
- 启停过程中进程崩溃，重启后仍能根据数据库继续恢复；
- 数据库是运维意图的事实来源；
- 生命周期逻辑集中在 `PipelineLifecycleService` 和 `PipelineRuntimeManager` 中，更容易排查。

建议类结构：

```text
PipelineCommandController
  -> PipelineDesiredStateService
      -> PipelineRepository

PipelineReconciler
  -> PipelineRepository
  -> PipelineRuntimeManager
  -> PipelineLifecycleService
```

`PipelineCommandController` 只负责校验请求并更新 `desired_status`。
`PipelineReconciler` 定时运行，对比数据库期望状态和内存实际状态，然后执行启动或停止。

示意代码：

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

启动和停止仍然建议交给有界 `ExecutorService` 执行，避免某一个 Kafka 连接卡住后影响所有传输线的协调。

SpringEvent 仍然可以使用，但定位应是旁路副作用，而不是主生命周期引擎：

```text
PipelineStartedEvent
PipelineStoppedEvent
PipelineFailedEvent
PipelineStatusChangedEvent
```

适合事件监听器处理的事情：

- 审计日志；
- 指标统计；
- 告警；
- WebSocket 状态推送。

不建议把核心启动、停止、回滚和持久化流程放进事件监听器。

## 建议数据库表

第一阶段可以先使用单表，后续复杂后再拆分。

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

推荐状态值：

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

`desired_status` 表示运维人员持久化下来的期望状态，用它替代更简单的 `enabled` 标记。

`runtime_status` 表示传输线当前或最近一次运行状态。

`version` 用于乐观锁或防止多个运维人员同时操作时发生状态覆盖。

## 管理 API 形态

管理 API 建议只监听本机地址或专用管理端口。

示例接口：

```text
GET  /admin/pipelines
GET  /admin/pipelines/{id}
POST /admin/pipelines/{id}/start
POST /admin/pipelines/{id}/stop
POST /admin/pipelines/{id}/restart
```

推荐绑定：

```properties
server.address=127.0.0.1
```

也可以按需要使用独立管理端口。

生产环境至少使用以下一种方式保护管理 API：

- 只绑定本机地址；
- token 认证；
- mTLS；
- 防火墙规则。

## CLI 客户端语言选择

计划方向是使用 Go 实现 Linux CLI 客户端。

原因：

- Go 可以生成单文件 Linux 二进制；
- 目标机器不需要 JRE 或 Python 运行环境；
- 部署简单，只需要将 `pipehub` 二进制放到 `/usr/local/bin`；
- Go 在基础设施 CLI 工具中使用广泛；
- Cobra 是 Go 生态中常用的 CLI 框架，支持子命令、参数、帮助信息和 shell completion。

推荐 Go 库：

```text
github.com/spf13/cobra       命令结构
github.com/spf13/viper       可选的配置文件和环境变量支持
github.com/chzyer/readline   可选的交互式 shell 支持
```

第一版 Go CLI 甚至可以只使用标准库：

```text
net/http
encoding/json
flag
os
fmt
```

当命令树变复杂后，再引入 Cobra 更合适。

## 建议仓库结构

如果后续改造成 Java 服务端加 Go CLI，可以采用下面结构：

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

不过，因为 CLI 使用 Go，`pipehub-common` 里的 Java DTO 无法直接给 Go 客户端复用。
真正的共享契约应该是 HTTP API 契约：

```text
OpenAPI spec 或清晰文档化的 JSON 请求/响应 DTO
```

务实路线：

1. 先保留当前 Java 项目作为服务端。
2. 添加 H2 持久化和管理 REST API。
3. 新增 `client-go/` 或 `pipehub-cli/` 目录放 Go CLI。
4. 明确记录请求和响应 JSON 格式。
5. API 规模变大后，再考虑从 OpenAPI 生成客户端。

## 建议实施阶段

### 阶段 1：服务端持久化

- 添加 H2 和 Spring JDBC。
- 创建 `pipeline_config` 表。
- 将 `data.json` 中的传输线迁移到数据库。
- 给传输线模型添加 `id`、`desired_status`、`runtime_status` 和 `version` 字段。

### 阶段 2：运行时生命周期

- 添加 `PipelineRuntimeManager`。
- 按 id 跟踪活跃传输线。
- 添加 `start(id)`、`stop(id)`、`restart(id)` 和 `list()` 能力。
- 给 `KafkaHttpEndpointBridge` 添加 `shutdown()`。
- 确保停止时可以安全唤醒并关闭 Kafka consumer。

### 阶段 3：管理 API

- 添加只面向本机的 REST 接口。
- 返回稳定的 JSON 响应对象。
- 如果管理端口可能被远程访问，添加简单认证。

### 阶段 4：Go CLI

- 创建 `pipehub-cli/` Go module。
- 实现命令：
  - `pipehub list`
  - `pipehub status <id>`
  - `pipehub start <id>`
  - `pipehub stop <id>`
  - `pipehub restart <id>`
  - `pipehub shell`
- 添加配置解析：
  - 命令行参数；
  - 环境变量；
  - 可选配置文件，例如 `/etc/pipehub/pipehub-cli.yaml`。

### 阶段 5：Linux 打包

- 构建名为 `pipehub` 的 Linux 二进制文件。
- 安装到 `/usr/local/bin/pipehub`。
- 将服务端作为 `systemd` 服务运行。
- 保留 CLI 和服务端版本查看能力：

```bash
pipehub version
```

## 最终建议

将服务端建设为稳定的 Spring Boot 后台服务，使用 H2 持久化并暴露本地管理 API。
将运维客户端建设为 Go 语言 Linux CLI。

这样既能给运维人员提供命令行操作体验，又能让真实的传输运行时不依赖终端会话、SSH 状态或服务启动方式。
