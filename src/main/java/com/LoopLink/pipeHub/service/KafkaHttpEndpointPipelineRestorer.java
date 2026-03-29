package com.LoopLink.pipeHub.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaHttpEndpointPipelineRestorer implements ApplicationListener<ApplicationReadyEvent> {

    private final KafkaHttpEndpointPipelineService kafkaHttpEndpointPipelineService;
    private final ObjectMapper objectMapper;

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {

        try {
            kafkaHttpEndpointPipelineService.startAll();
            // List<KafkaHttpEndpointPipeline> records = objectMapper.readValue(input, new TypeReference<List<KafkaHttpEndpointPipeline>>() {});
            // ExecutorService executor = Executors.newFixedThreadPool(5);

            // int size = records.size();
            // AtomicInteger count = new AtomicInteger();
            // records.forEach(task -> executor.submit(() -> {
            //     try {
            //         kafkaHttpEndpointPipelineService.start(task.getId());
            //         count.getAndIncrement();
            //     } catch (Exception e) {
            //         log.error("Failed to start task {}", task.getId(), e);
            //     }
            // }));

            // executor.shutdown();
            // log.info("there is {} KafkaHttpEndpointPipeline need to be restored, KafkaHttpEndpointPipeline finished restoring {} tasks", size, count);

        } catch (Exception e) {
            log.error("KafkaHttpEndpointPipeline failed to restore tasks", e);
        }

    }

}
