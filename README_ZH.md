# pipeHub

> 一个轻量级的 Kafka → HTTP 消息转发中间件，将 Kafka Topic 中的消息实时投递到指定的 HTTP 端点。

> P.S.暂时这个工程只是个原型，未来可能会支持多种数据组件间的数据实时流 

---

## 项目简介

pipeHub 是一个基于 Spring Boot 的集成管道服务。它的核心职责是**连接两个端点（Kafka Source → HTTP Destination）**，通过读取 JSON 配置文件中定义的 Pipeline 规则，在应用启动时自动建立 Kafka Consumer，持续轮询消息并以 HTTP POST 方式转发到目标接口。

适用场景：Kafka 生态系统与 REST API 服务之间的解耦集成，无需引入 Kafka SDK，即可实现实时数据流转。

---

## 功能特性

- **Pipeline 配置驱动**：通过 `data.json` 文件声明式定义 Kafka → HTTP 转发规则，支持多条 Pipeline 并行运行。
- **启动自动恢复**：应用就绪后自动加载并启动所有 Pipeline，无需手动干预。
- **灵活的认证策略**：支持 `X-Proxy-Token` 透传和 `Authorization: Bearer <token>` 两种 HTTP 认证方式。
- **HTTP 重试机制**：每条消息最多重试 3 次，失败间隔 500ms，提升投递可靠性。
- **外部配置优先**：支持从 JAR 包同级 `pipeline/data.json` 加载配置，便于生产环境动态调整，无需重新打包。
- **连接池管理**：内置 Apache HttpClient 连接池（最大连接数 200，每路由 50），高效复用 HTTP 连接。

---

## 架构概览

```
┌─────────────────────────────────────────────────────────────┐
│                        pipeHub                              │
│                                                             │
│  ┌───────────────┐     ┌────────────────────────────────┐   │
│  │PipelineManager│────▶│KafkaHttpEndpointPipelineService│   │
│  │  (data.json)  │     └──────────────┬─────────────────┘   │
│  └───────────────┘                    │                     │
│                                       ▼                     │
│                         ┌─────────────────────────┐         │
│                         │ KafkaHttpEndpointBridge │         │
│                         │  (per pipeline)         │         │
│                         └────────────┬────────────┘         │
│                                      │                      │
│                                      ▼                      │
│                         ┌─────────────────────────┐         │
│                         │KafkaHttpEndpointChannel │         │
│                         │  [Daemon Thread]        │         │
│                         │  poll() & send()        │         │
│                         └────────────┬────────────┘         │
└──────────────────────────────────────┼──────────────────────┘
                                       │
          ┌────────────────────────────┼──────────────────┐
          ▼                                               ▼
  ┌─────────────┐                              ┌───────────────┐
  │ Kafka Topic │                              │ HTTP Endpoint │
  └─────────────┘                              └───────────────┘
```

---

## 项目结构

```
src/main/java/com/github/pipeHub/
├── PipeHubApplication.java                   # 启动入口
├── controller/
│   └── TestController.java                   # 测试接收端点
├── enums/
│   └── AuthKeyEnum.java                      # 认证头策略枚举
├── manage/
│   ├── HttpClientProvider.java               # HttpClient Bean 工厂（连接池）
│   ├── PipelineInfo.java                     # Pipeline 列表 + 时间戳包装
│   └── PipelineManager.java                  # 配置加载与管理
├── model/
│   ├── Pipeline.java                         # Pipeline 抽象基类
│   ├── KafkaHttpEndpointPipeline.java        # Kafka→HTTP Pipeline 配置模型
│   ├── KafkaHttpEndpointBridge.java          # Pipeline 工厂/生命周期持有者
│   ├── KafkaHttpEndpointChannel.java         # 核心消费投递循环
│   ├── KafkaConfig.java                      # Kafka 连接配置
│   └── HttpEndPointConfig.java               # HTTP 端点 + 认证配置
├── service/
│   ├── KafkaHttpEndpointPipelineService.java          # 服务接口
│   ├── KafkaHttpEndpointPipelineRestorer.java         # 启动监听器
│   └── impl/
│       └── KafkaHttpEndpointPipelineServiceImpl.java  # 服务实现
└── utils/
    ├── ApplicationContextUtil.java           # 静态 Spring 上下文访问器
    └── JsonUtil.java                         # Jackson 工具封装

src/main/resources/
├── application.yml                           # 主配置（端口 9494）
├── application-dev.yml                       # 开发环境配置
├── application-site.yml                      # 生产环境配置
└── static/
    └── data.json                             # Pipeline 定义文件（内置默认）
```

---

## Pipeline 配置说明

Pipeline 定义文件 `data.json` 结构如下：

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

| 字段 | 说明                                            |
|---|-----------------------------------------------|
| `kafkaConfig.bootstrapServer` | Kafka Broker 地址                               |
| `kafkaConfig.groupId` | Consumer Group ID                             |
| `kafkaConfig.topic` | 订阅的 Topic 名称                                  |
| `httpEndPointConfig.url` | 目标 HTTP 端点 URL                                |
| `httpEndPointConfig.authKey` | 认证头类型（`Authorization` 或 `X-Proxy-Token`）可按需扩展 |

**外部配置覆盖**：将 `data.json` 放置在 JAR 包同级目录的 `pipeline/` 文件夹下，应用优先读取外部文件，无需重新打包即可修改 Pipeline 配置。

---

## 快速开始

### 环境要求

- Java 8+
- Maven 3.6+
- 可访问的 Kafka Broker

### 构建与运行

```bash
# 克隆项目
git clone https://github.com/DarrylHuang/pipeHub.git
cd pipeHub

# 构建（生成包含依赖的 fat JAR）
mvn clean package

# 运行
java -jar target/pipeHub-*.jar --spring.profiles.active=dev
```

### 自定义 Pipeline 配置

```bash
# 在 JAR 同级目录创建配置文件夹
mkdir pipeline

# 放置自定义 data.json
cp your-data.json pipeline/data.json

# 启动应用，会自动读取外部配置
java -jar pipeHub.jar
```

---

## 技术栈

| 类别 | 技术 | 版本 |
|---|---|---|
| 框架 | Spring Boot | 2.6.13 |
| 语言 | Java | 1.8 |
| 构建 | Maven | 3.x |
| HTTP 客户端 | Apache HttpComponents | 4.5.10 |
| 消息队列 | Apache Kafka Clients | 3.4.1 |
| JSON 处理 | Jackson | (Spring Boot 内置) |
| 工具库 | Lombok, Commons Lang3, Commons Collections4 | - |

---

## 设计亮点

### 1. 枚举策略模式（`AuthKeyEnum`）

将认证头的处理逻辑封装在枚举常量中，每个常量实现 `handleAuth()` 抽象方法，实现了简洁的策略模式：

```java
Authorization {
    @Override
    public String handleAuth(String token) {
        return "Bearer " + token;
    }
}
```

新增认证类型只需添加枚举常量，无需修改调用方代码。

### 2. 双源配置加载（`PipelineManager`）

外部文件优先、内置资源兜底的加载策略，兼顾了部署灵活性与开箱即用体验：

```
JAR同级 pipeline/data.json   ──优先──▶  加载外部配置
        ↓ 不存在
classpath:/static/data.json  ──缺省──▶  加载内置配置
```

### 3. 可扩展的 Pipeline 抽象

`Pipeline` 抽象基类 + `KafkaHttpEndpointPipeline` 具体实现的层次结构，为未来支持更多管道类型（如 Kafka→Kafka、HTTP→Kafka）预留了扩展点。

---

## 已知问题与改进建议

| 问题级别 | 问题 | 建议 |
|------|---|---|
| 高    | SSL 信任所有证书 (`TrustAll` + `NoopHostnameVerifier`) | 配置正确的证书信任链 |
| 高    | Auth Token 明文写入配置文件 | 使用环境变量或密钥管理服务 |
| 中    | 使用裸线程而非 `ExecutorService` | 改用线程池便于生命周期管理和优雅关闭 |
| 中    | `enable.auto.commit=true` 与手动 `commitSync()` 并存 | 禁用自动提交，统一使用手动提交 |
| 中    | `analyseMessage` 空字符串检查与调用方 `null` 检查不一致 | 统一返回 `null` 表示无效消息 |

---

## License

[MIT](LICENSE)
