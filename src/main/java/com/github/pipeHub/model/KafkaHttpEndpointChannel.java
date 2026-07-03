package com.github.pipeHub.model;

import com.github.pipeHub.enums.AuthKeyEnum;
import com.github.pipeHub.utils.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Getter
public class KafkaHttpEndpointChannel {
    private static final int POST_MAX_RETRY = 3;
    private static final long POLL_TIMEOUT_MS = 1000L;

    private final String channelName;
    private final KafkaConfig kafkaConfig;
    private final HttpEndPointConfig httpEndpoint;
    private final CloseableHttpClient httpAgent;
    private final Consumer<String, String> consumer;

    public KafkaHttpEndpointChannel(KafkaConfig kafkaConfig, HttpEndPointConfig httpEndpoint, CloseableHttpClient httpAgent) {
        this.kafkaConfig = kafkaConfig;
        this.consumer = new KafkaConsumer<>(kafkaConfig.toConsumerProperties());
        List<String> topicList = Arrays.stream(StringUtils.split(kafkaConfig.getTopics(), ",")).map(String::trim).filter(StringUtils::isNotBlank).distinct().collect(Collectors.toList());
        this.consumer.subscribe(topicList);

        this.httpEndpoint = httpEndpoint;
        this.httpAgent = httpAgent;

        this.channelName = "Channel[kafka => http]:" + kafkaConfig.getServers() + ":" + kafkaConfig.getTopics() + " TO " + httpEndpoint.getUrl();

        link();
    }

    public void link() {
        Thread thread = new Thread(this::dispatchLoop, channelName);
        thread.setDaemon(true);
        thread.start();
    }

    private void dispatchLoop() {
        log.info("{} started consuming from topics {}", channelName, kafkaConfig.getTopics());
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
                if (!records.isEmpty()) {
                    boolean isDispatched = true;
                    for (ConsumerRecord<String, String> record : records) {
                        String message = analyseMessage(record.value());
                        if (message != null) {
                            if (!send(message)) {
                                isDispatched = false;

                                log.error("the Http endpoint is blocked for some reason, Kafka polling offset cursor is paused at {}. ", System.currentTimeMillis());
                                log.error("Http endpoint: {}", httpEndpoint.getUrl());
                                log.error("Kafka server: {}", kafkaConfig.getServers());
                                log.error("Kafka topic: {}", record.topic());
                                log.error("partition: {}", record.partition());
                                log.error("offset: {}", record.offset());
                                log.error("key: {}", record.key());

                                break;
                            }
                        }
                    }

                    if (isDispatched) {
                        consumer.commitSync();
                    }
                }
            }
        } catch (Exception e) {
            log.error("{} encountered error in pollLoop", channelName, e);
        } finally {
            consumer.close();
            log.info("{} consumer closed", channelName);
        }
    }

    protected String analyseMessage(String message) {
        if (Objects.isNull(message) || StringUtils.isBlank(message)) {
            return "";
        }
        return message;
    }

    private boolean send(String message) {
        AuthKeyEnum authKeyEnum = AuthKeyEnum.of(httpEndpoint.getAuthKey());

        int attempt = 0;
        boolean success = false;
        do {
            attempt++;
            try {
                String url = httpEndpoint.getUrl();
                HttpPost post = new HttpPost(url);
                post.setHeader("Content-Type", "application/json");
                if (Objects.nonNull(authKeyEnum)) {
                    String val = authKeyEnum.handleAuth(httpEndpoint.getAuthValue());
                    post.setHeader(httpEndpoint.getAuthKey(), val);
                }
                post.setEntity(new StringEntity(message, "UTF-8"));

                httpAgent.execute(post, response -> {
                    int status = response.getStatusLine().getStatusCode();
                    if (status >= 200 && status < 300) {
                        String res = EntityUtils.toString(response.getEntity());
                        JsonNode responseEntity = JsonUtil.toJson(res);
                        status = responseEntity.get("code").asInt();
                        if (status == 200) {
                            return true;
                        } else {
                            throw new RuntimeException("HTTP request failed with status: " + status);
                        }
                    } else {
                        throw new RuntimeException("HTTP request failed with status: " + status);
                    }
                });
                success = true;
            } catch (Exception e) {
                log.warn("{} HTTP send attempt {} failed", channelName, attempt, e);
                try {
                    TimeUnit.MILLISECONDS.sleep(500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        } while (!success && attempt < POST_MAX_RETRY);

        if (!success) {
            log.error("{} failed to send message after {} attempts: {}", channelName, POST_MAX_RETRY, message);
            return false;
        } else {
            return true;
        }
    }

    public void shutdown() {
        try {
            consumer.wakeup();
        } catch (Exception e) {
            log.warn("{} stop error", channelName, e);
        }
    }
}
