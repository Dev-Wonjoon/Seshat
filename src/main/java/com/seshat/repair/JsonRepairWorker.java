package com.seshat.repair;

import com.seshat.processing.JavaLogProcessor;
import com.seshat.repair.JsonRepairConfiguration.Settings;
import com.seshat.repair.JsonRepairRepository.Job;
import com.seshat.repair.JsonRepairRepository.Outcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

@Service
@ConditionalOnProperty(prefix = "seshat.repair", name = "enabled", havingValue = "true")
public class JsonRepairWorker {

    private static final Logger log = LoggerFactory.getLogger(JsonRepairWorker.class);
    private static final long CLAIM_GRACE_MS = 30_000;

    private final JsonRepairRepository repository;
    private final JsonRepairPolicy policy;
    private final JavaLogProcessor processor;
    private final JsonMapper mapper;
    private final Settings settings;

    JsonRepairWorker(
            JsonRepairRepository repository, JsonRepairPolicy policy,
            JavaLogProcessor processor, JsonMapper mapper, Settings settings
    ) {
        this.repository = repository;
        this.policy = policy;
        this.processor = processor;
        this.mapper = mapper;
        this.settings = settings;
    }

    @Transactional(propagation = Propagation.NEVER)
    public boolean runOnce() {
        long now = System.currentTimeMillis();
        var activeClient = settings.clients().get(settings.provider());
        var claimed = repository.claim(now, now + settings.timeout().toMillis() + CLAIM_GRACE_MS,
                settings.maxAttempts(), settings.provider(), activeClient.profile());

        if (claimed.isEmpty()) {
            return false;
        }

        Job job = claimed.get();
        Outcome outcome;
        boolean retry = false;
        try {
            outcome = repair(job);
        } catch (RepairCallException exception) {
            outcome = Outcome.withoutCandidate("CALL_FAILED", exception.reason().name());
            retry = exception.retryable() && job.attempt() < settings.maxAttempts();
        } catch (RuntimeException exception) {
            log.error("JSON 복구 작업 내부 오류. 작업 ID={}, 오류 유형={}",
                    job.id(), exception.getClass().getSimpleName());
            outcome = Outcome.withoutCandidate("CALL_FAILED", "WORKER_ERROR");
        }

        long finishedAt = System.currentTimeMillis();
        repository.finish(job, outcome, finishedAt,
                retry ? finishedAt + settings.retryDelayMillis() : 0);
        return true;
    }

    private Outcome repair(Job job) throws RepairCallException {
        var original = mapper.readTree(job.originalResult());
        String raw = original.path("raw").stringValue();
        var assessment = policy.assess(raw);
        if (assessment.state() != null) {
            return Outcome.withoutCandidate(assessment.state(), assessment.reason());
        }

        // 진행 중이던 작업의 재시도는 처음 할당한 공급자와 모델을 사용한다.
        var client = settings.clients().get(job.provider());
        if (client == null || !job.profile().equals(client.profile())) {
            return Outcome.withoutCandidate("CALL_FAILED", "PROVIDER_PROFILE_UNAVAILABLE");
        }

        var error = original.path("parse_error");
        var response = client.repair(new RepairRequest(raw, error.path("code").stringValue(),
                error.path("message").stringValue(), settings.timeout()));
        if (response == null || response.decision() == null) {
            return new Outcome("CALL_FAILED", "INVALID_RESPONSE", response, null, null);
        }
        String candidate = response.candidateJson();
        if (response.decision() == RepairCandidate.Decision.UNRECOVERABLE) {
            if (candidate != null || response.reason() == null || response.reason().isBlank()) {
                return new Outcome("CALL_FAILED", "INVALID_RESPONSE", response, null, null);
            }
            return new Outcome("UNRECOVERABLE", response.reason(), response, null, null);
        }
        if (candidate == null || candidate.isBlank()) {
            return new Outcome("CALL_FAILED", "INVALID_RESPONSE", response, null, null);
        }

        boolean syntaxValid = "SUCCESS".equals(processor.process(candidate)
                .path("parse_status").stringValue());
        if (!syntaxValid) {
            return new Outcome("CALL_FAILED", "INVALID_CANDIDATE_SYNTAX", response, false, null);
        }

        boolean valuesPreserved = policy.preservesValues(assessment, candidate);
        return new Outcome(valuesPreserved ? "VERIFIED" : "CANDIDATE_REJECTED",
                valuesPreserved ? response.reason() : "VALUES_NOT_PRESERVED",
                response, true, valuesPreserved);
    }
}
