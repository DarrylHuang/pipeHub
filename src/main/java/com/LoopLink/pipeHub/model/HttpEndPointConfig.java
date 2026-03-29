package com.LoopLink.pipeHub.model;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=false)
public class HttpEndPointConfig {
    private String url;
    private String authKey;
    private String authValue;
}
