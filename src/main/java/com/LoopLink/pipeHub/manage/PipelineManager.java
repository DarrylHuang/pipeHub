package com.LoopLink.pipeHub.manage;

import com.LoopLink.pipeHub.model.KafkaHttpEndpointPipeline;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Data
@Component
@RequiredArgsConstructor
public class PipelineManager {
    private static final String DICTIONARY = "pipelines";
    private static final String DATA = "/static/data.json";
    private static final Path RECORDS = Paths.get(DICTIONARY, DATA);
    private static final ClassPathResource CLASSPATH_RESOURCE = new ClassPathResource(DATA);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final AtomicReference<PipelineInfo> infoRef = new AtomicReference<>(new PipelineInfo(Collections.emptyList(), LocalDateTime.now()));

    private boolean useExternal = false;

    @PostConstruct
    public void init() {
        loadData();
        // watchDataModified();
    }

    private synchronized void loadData() {
        try (InputStream input = getDataInputStream()) {
            if (input != null) {
                List<KafkaHttpEndpointPipeline> data = MAPPER.readValue(input, new TypeReference<List<KafkaHttpEndpointPipeline>>() {});
                PipelineInfo info = new PipelineInfo(data, LocalDateTime.now());
                infoRef.set(info);
                log.info("Load data from {} file", useExternal ? "external" : "classpath");
            } else {
                log.warn("Data file not found in either external path or classpath.");
            }
        } catch (IOException e) {
            log.error("Failed to load configuration: {}", e.getMessage(), e);
        }
    }

    private InputStream getDataInputStream() throws IOException {
        if (Files.exists(RECORDS)) {
            useExternal = true;
            return Files.newInputStream(RECORDS);
        } else if (CLASSPATH_RESOURCE.exists()) {
            useExternal = false;
            return CLASSPATH_RESOURCE.getInputStream();
        } else {
            return null;
        }
    }

    public synchronized void updateConfig(String key, String value) {
        Path path;

        try {
            path = useExternal ? RECORDS : Paths.get(CLASSPATH_RESOURCE.getURI());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try (OutputStream output = Files.newOutputStream(path)) {
            log.info("Updated data [{}]: [{}={}]", useExternal ? RECORDS : Paths.get(CLASSPATH_RESOURCE.getURI()), key, value);
        } catch (IOException e) {
            log.error("Failed to update data: {}", e.getMessage(), e);
        }
    }

    private void watchDataModified() {

        Thread thread = new Thread(() -> {
            try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
                Path dir;
                if (useExternal) {
                    dir = RECORDS.getParent();
                } else {
                    dir = Paths.get(CLASSPATH_RESOURCE.getURI()).getParent();
                }

                dir.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
                log.info("Watching external data file: {}", RECORDS);

                while (true) {
                    WatchKey key = watchService.take();
                    for (WatchEvent<?> event : key.pollEvents()) {
                        if (event.context().toString().equals(RECORDS.getFileName().toString())) {
                            log.info("Modification detected in data file, reloading...");
                            loadData();
                        }
                    }
                    key.reset();
                }
            } catch (IOException | InterruptedException e) {
                log.error("Error watching data file: {}", e.getMessage(), e);
            }
        });

        thread.setDaemon(true);
        thread.start();

    }
}
