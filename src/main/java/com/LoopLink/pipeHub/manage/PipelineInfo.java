package com.LoopLink.pipeHub.manage;

import com.LoopLink.pipeHub.model.KafkaHttpEndpointPipeline;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.List;

@Data
@AllArgsConstructor
public class PipelineInfo {
    private List<KafkaHttpEndpointPipeline> data;

    @JsonFormat(pattern = "MMM dd, yyyy HH:mm:ss", locale = "en")
    private LocalDateTime timestamp;

}
