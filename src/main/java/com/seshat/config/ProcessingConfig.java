package com.seshat.config;

import com.seshat.processing.JavaLogProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ProcessingConfig {

    @Bean
    public JavaLogProcessor javaLogProcessor() {
        return new JavaLogProcessor();
    }
}
