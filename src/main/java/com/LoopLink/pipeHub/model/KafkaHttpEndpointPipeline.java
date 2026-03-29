package com.LoopLink.pipeHub.model;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=false)
public class KafkaHttpEndpointPipeline extends Pipeline {
    // private String id;
    private KafkaConfig kafkaConfig;
    private HttpEndPointConfig httpEndPointConfig;
    // private String status;
}
