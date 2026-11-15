package com.seshat.logs.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record LogRunIds(List<String> items, @JsonProperty("next_after") String nextAfter) {}
