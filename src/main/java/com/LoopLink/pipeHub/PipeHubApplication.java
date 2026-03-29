package com.LoopLink.pipeHub;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class PipeHubApplication {

    public static void main(String[] args) {
        SpringApplication.run(PipeHubApplication.class, args);
    }

}
