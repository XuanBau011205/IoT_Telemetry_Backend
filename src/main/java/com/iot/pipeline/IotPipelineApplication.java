// IotPipelineApplication.java
package com.iot.pipeline;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling  // Required for @Scheduled on MultiNodeWindowProcessor and DatabaseWriter
public class IotPipelineApplication {
    public static void main(String[] args) {
        SpringApplication.run(IotPipelineApplication.class, args);
    }
}