package com.seshat.config;

import co.elastic.clients.json.JsonpMapper;
import co.elastic.clients.json.jackson.Jackson3JsonpMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.json.JsonMapper;

@Configuration
public class ElasticsearchJsonConfig {

    @Bean
    public JsonpMapper elasticsearchJsonpMapper(JsonMapper mapper) {
        return new Jackson3JsonpMapper(mapper);
    }
}