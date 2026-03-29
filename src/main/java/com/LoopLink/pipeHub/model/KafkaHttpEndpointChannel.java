package com.LoopLink.pipeHub.model;

import com.LoopLink.pipeHub.enums.AuthKeyEnum;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

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
        this.consumer.subscribe(Collections.singletonList(kafkaConfig.getTopics()));

        this.httpEndpoint = httpEndpoint;
        this.httpAgent = httpAgent;

        this.channelName = "Channel[kafka => http]:" + kafkaConfig.getServers() + ":" + kafkaConfig.getTopics() + " TO " + httpEndpoint.getUrl();

        link();
    }

    public void link() {
        Thread thread = new Thread(this::pollLoop, channelName);
        thread.setDaemon(true);
        thread.start();
    }

    private void pollLoop() {
        log.info("{} started consuming from topics {}", channelName, kafkaConfig.getTopics());
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
                if (!records.isEmpty()) {
                    records.forEach(record -> {
                        String message = analyseMessage(record.value());
                        if (message != null) {
                            send(message);
                        }
                    });
                    consumer.commitSync();
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

    private void send(String message) {
        AuthKeyEnum authKeyEnum = AuthKeyEnum.of(httpEndpoint.getAuthKey());

        int attempt = 0;
        boolean success = false;
        while (!success && attempt < POST_MAX_RETRY) {
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
                        EntityUtils.toString(response.getEntity());
                        return true;
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
                    return;
                }
            }
        }

        if (!success) {
            log.error("{} failed to send message after {} attempts: {}", channelName, POST_MAX_RETRY, message);
        }
    }

    public void stop() {
        try {
            consumer.wakeup();
        } catch (Exception e) {
            log.warn("{} stop error", channelName, e);
        }
    }
}
