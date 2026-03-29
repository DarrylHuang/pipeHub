package com.LoopLink.pipeHub.response;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Result {
    private String msg;
    private int code;
}
