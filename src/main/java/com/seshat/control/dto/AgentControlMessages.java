package com.seshat.control.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.seshat.ingestion.dto.ParsingMode;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.util.Objects;
import java.util.UUID;

public final class AgentControlMessages {

    private AgentControlMessages() {
    }

    public record Target(
            @JsonProperty("run_id") @NotBlank String runId,
            @JsonProperty("source_id") @NotBlank String sourceId,
            @JsonProperty("instance_id") @NotNull UUID instanceId
    ) {
    }

    public record CommandId(
            @JsonProperty("controller_instance_id") UUID controllerInstanceId,
            @JsonProperty("command_version") long commandVersion
    ) {
        public CommandId {
            Objects.requireNonNull(controllerInstanceId, "중앙 인스턴스 ID는 필수입니다.");
            if (commandVersion <= 0) {
                throw new IllegalArgumentException("명령 버전은 양수여야 합니다.");
            }
        }
    }

    public record Command(CommandId id, ParsingMode mode) {
        public Command {
            Objects.requireNonNull(id, "명령 ID는 필수입니다.");
            Objects.requireNonNull(mode, "명령 모드는 필수입니다.");
        }
    }

    public record CreateCommandRequest(
            @NotNull @Valid Target target,
            @JsonProperty("request_id") @NotNull UUID requestId,
            @NotNull ParsingMode mode
    ) {
    }

    @JsonInclude(JsonInclude.Include.ALWAYS)
    public record SyncRequest(
            @NotNull @Valid Target target,
            @JsonProperty("received_command_id") CommandId receivedCommandId,
            @JsonProperty("applied_command_id") CommandId appliedCommandId,
            @JsonProperty("applied_mode") @NotNull ParsingMode appliedMode
    ) {
    }

    @JsonInclude(JsonInclude.Include.ALWAYS)
    public record SyncResponse(
            @NotNull @Valid Target target,
            @JsonProperty("controller_instance_id") @NotNull UUID controllerInstanceId,
            @JsonProperty("valid_for_ms") @Positive long validForMs,
            Command command
    ) {
    }

    public enum CommandPhase {
        PENDING_DELIVERY,
        PENDING_APPLY,
        APPLIED
    }

    @JsonInclude(JsonInclude.Include.ALWAYS)
    public record StatusResponse(
            Target target,
            @JsonProperty("controller_instance_id") UUID controllerInstanceId,
            boolean active,
            @JsonProperty("applied_mode") ParsingMode appliedMode,
            Command command,
            CommandPhase phase,
            @JsonProperty("command_reason") String commandReason
    ) {
    }
}