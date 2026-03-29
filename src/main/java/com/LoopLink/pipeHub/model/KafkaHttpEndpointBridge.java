package com.LoopLink.pipeHub.model;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.impl.client.CloseableHttpClient;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

@Data
public class KafkaHttpEndpointBridge {

    private KafkaConfig kafkaConfig;
    private HttpEndPointConfig httpEndPointConfig;

    public static KafkaHttpEndpointBridge establish(KafkaConfig kafkaConfig, HttpEndPointConfig httpEndPointConfig) {
        return new KafkaHttpEndpointBridge(kafkaConfig,  httpEndPointConfig);
    }

    private KafkaHttpEndpointBridge(KafkaConfig kafkaConfig, HttpEndPointConfig httpEndPointConfig) {
        this.kafkaConfig = kafkaConfig;
        this.httpEndPointConfig = httpEndPointConfig;
    }

    private KafkaHttpEndpointChannel kafkaHttpEndpointChannel;

    public void openChannel(CloseableHttpClient httpAgent) throws Exception {
        if (this.kafkaConfig == null || this.httpEndPointConfig == null || httpAgent == null) {
            throw new Exception("kafkaConfig or httpEndPointConfig or httpClient is null, please check.");
        }

        if (this.kafkaConfig.getTopics() == null || StringUtils.isBlank(this.kafkaConfig.getTopics())) {
            throw new Exception("kafka topics is null, please check.");
        }

        Set<String> topics = Arrays.stream(StringUtils.split(this.kafkaConfig.getTopics(), ",")).collect(Collectors.toSet());
        if (topics.isEmpty()) {
            throw new Exception("kafka topics is null, please check.");
        } else {
            this.kafkaConfig.setTopics(StringUtils.join(topics, ","));
        }

        if (this.httpEndPointConfig.getUrl() == null || StringUtils.isBlank(this.httpEndPointConfig.getUrl())) {
            throw new Exception("httpEndPoint url is null, please check.");
        }

        this.kafkaHttpEndpointChannel = new KafkaHttpEndpointChannel(this.kafkaConfig, this.httpEndPointConfig, httpAgent);
    }

}
