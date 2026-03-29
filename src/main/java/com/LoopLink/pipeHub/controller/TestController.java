package com.LoopLink.pipeHub.controller;

import com.LoopLink.pipeHub.response.Result;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class TestController {

    @PostMapping("/data/ev_feedback")
    public Result accept(@RequestBody Object msg) {
        System.out.println(msg);
        return Result.builder().msg("success").code(200).build();
    }

}
