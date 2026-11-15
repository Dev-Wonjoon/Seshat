package com.seshat.repair;

import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

@Component
public class JsonRepairPolicy {

    private static final int MAX_INPUT_LENGTH = 16_384;
    private static final int MAX_NESTING_DEPTH = 50;
    private static final Pattern TOKEN = Pattern.compile(
            "\"(?:[^\"\\\\\\x00-\\x1f]|\\\\(?:[\"\\\\/bfnrt]|u[0-9a-fA-F]{4}))*+\""
                    + "|-?(?:0|[1-9][0-9]*)(?:\\.[0-9]+)?(?:[eE][+-]?[0-9]+)?"
                    + "|true|false|null|[{}\\[\\]:,]|[ \\r\\n\\t]+"
    );

    private final JsonMapper mapper;

    public JsonRepairPolicy(JsonMapper mapper) {
        this.mapper = mapper;
    }

    Assessment assess(String raw) {
        if (raw.length() >= MAX_INPUT_LENGTH) {
            return Assessment.reject("SKIPPED", "INPUT_TOO_LARGE");
        }
        if (raw.isBlank()) {
            return Assessment.reject("SKIPPED", "EMPTY_INPUT");
        }
        if (!raw.stripLeading().startsWith("{")) {
            return Assessment.reject("SKIPPED", "NON_JSON_OBJECT");
        }

        try {
            var input = tokens(raw);
            var grammar = new Grammar(input);
            grammar.object(1);
            if (grammar.peek() != null) {
                return Assessment.reject("UNRECOVERABLE", "TRAILING_CONTENT");
            }
            if (grammar.edit == null) {
                return Assessment.reject("SKIPPED", "ALREADY_VALID");
            }
            grammar.edit.apply(input);
            return new Assessment(null, null, List.copyOf(input));
        } catch (InvalidInput exception) {
            return Assessment.reject("UNRECOVERABLE", exception.getMessage());
        }
    }

    boolean preservesValues(Assessment assessment, String candidate) {
        if (candidate.length() > MAX_INPUT_LENGTH) {
            return false;
        }
        try {
            return assessment.expectedTokens().equals(tokens(candidate));
        } catch (InvalidInput exception) {
            return false;
        }
    }

    private static List<String> tokens(String raw) {
        var tokens = new ArrayList<String>();
        var matcher = TOKEN.matcher(raw);
        int offset = 0;

        while (offset < raw.length()) {
            if (!matcher.region(offset, raw.length()).lookingAt()) {
                throw new InvalidInput("AMBIGUOUS_SYNTAX");
            }
            String token = matcher.group();
            if (!token.isBlank()) {
                tokens.add(token);
            }
            offset = matcher.end();
        }
        return tokens;
    }

    record Assessment(String state, String reason, List<String> expectedTokens) {

        private static Assessment reject(String state, String reason) {
            return new Assessment(state, reason, List.of());
        }
    }

    private record Edit(int index, String insertedToken) {

        private void apply(List<String> tokens) {
            if (insertedToken == null) {
                tokens.remove(index);
            } else {
                tokens.add(index, insertedToken);
            }
        }
    }

    private final class Grammar {

        private final List<String> tokens;
        private int index;
        private Edit edit;

        private Grammar(List<String> tokens) {
            this.tokens = tokens;
        }

        private void object(int depth) {
            checkDepth(depth);
            expect("{");
            if (take("}")) {
                return;
            }

            var keys = new HashSet<String>();
            while (true) {
                String key = peek();
                if (key == null || !key.startsWith("\"")) {
                    throw new InvalidInput("AMBIGUOUS_SYNTAX");
                }
                index++;
                if (!keys.add(mapper.readValue(key, String.class))) {
                    throw new InvalidInput("DUPLICATE_KEY");
                }
                expect(":");
                value(depth);

                if (take("}")) {
                    return;
                }
                if (peek() == null && depth == 1) {
                    recordEdit(new Edit(index, "}"));
                    return;
                }
                if (take(",")) {
                    if (take("}")) {
                        recordEdit(new Edit(index - 2, null));
                        return;
                    }
                } else if (peek() != null && peek().startsWith("\"")) {
                    recordEdit(new Edit(index, ","));
                } else {
                    throw new InvalidInput("AMBIGUOUS_SYNTAX");
                }
            }
        }

        private void array(int depth) {
            checkDepth(depth);
            expect("[");
            if (take("]")) {
                return;
            }
            do {
                value(depth);
            } while (take(","));
            expect("]");
        }

        private void value(int depth) {
            String token = peek();
            if (token == null || "}:,]".contains(token)) {
                throw new InvalidInput("AMBIGUOUS_SYNTAX");
            }
            switch (token) {
                case "{" -> object(depth + 1);
                case "[" -> array(depth + 1);
                default -> index++;
            }
        }

        private void recordEdit(Edit edit) {
            if (this.edit != null) {
                throw new InvalidInput("AMBIGUOUS_SYNTAX");
            }
            this.edit = edit;
        }

        private void checkDepth(int depth) {
            if (depth > MAX_NESTING_DEPTH) {
                throw new InvalidInput("NESTING_LIMIT");
            }
        }

        private String peek() {
            return index < tokens.size() ? tokens.get(index) : null;
        }

        private boolean take(String token) {
            if (!token.equals(peek())) {
                return false;
            }
            index++;
            return true;
        }

        private void expect(String token) {
            if (!take(token)) {
                throw new InvalidInput("AMBIGUOUS_SYNTAX");
            }
        }
    }

    private static final class InvalidInput extends RuntimeException {
        private InvalidInput(String reason) {
            super(reason);
        }
    }
}
