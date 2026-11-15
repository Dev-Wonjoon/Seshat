package com.seshat.repair;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.Map;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "seshat.repair", name = "enabled", havingValue = "true")
public class JsonRepairConfiguration {

    @Bean(name = "openAiCompatibleRepair", destroyMethod = "close")
    @ConditionalOnProperty(prefix = "seshat.repair", name = "provider",
            havingValue = "openAiCompatibleRepair")
    OpenAiCompatibleJsonRepairClient openAiCompatibleRepair(
            @Value("${seshat.repair.http.endpoint:}") String endpoint,
            @Value("${seshat.repair.http.model:}") String model,
            @Value("${seshat.repair.http.api-key:}") String apiKey
    ) {
        return new OpenAiCompatibleJsonRepairClient(endpoint, model, apiKey);
    }

    @Bean
    Settings jsonRepairSettings(
            @Value("${seshat.repair.provider:}") String provider,
            @Value("${seshat.repair.timeout-ms:5000}") long timeoutMillis,
            @Value("${seshat.repair.max-attempts:3}") int maxAttempts,
            @Value("${seshat.repair.retry-delay-ms:1000}") long retryDelayMillis,
            Map<String, JsonRepairClient> clients
    ) {
        if (!clients.containsKey(provider)) {
            throw new IllegalStateException("설정에 해당하는 JSON 복구 연결부가 없습니다.");
        }
        if (timeoutMillis <= 0 || timeoutMillis > 300_000
                || maxAttempts <= 0 || retryDelayMillis <= 0) {
            throw new IllegalArgumentException("JSON 복구 작업 설정이 유효하지 않습니다.");
        }
        return new Settings(provider, Map.copyOf(clients), Duration.ofMillis(timeoutMillis),
                maxAttempts, retryDelayMillis);
    }

    record Settings(
            String provider, Map<String, JsonRepairClient> clients, Duration timeout,
            int maxAttempts, long retryDelayMillis
    ) {
    }
}
