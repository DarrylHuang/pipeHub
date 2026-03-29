package com.LoopLink.pipeHub.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

@Data
@EqualsAndHashCode(callSuper=false)
@Accessors(chain=true, fluent = true)
public class DataAnalysisSettings {
    private Integer structured;


    private String node;
}
