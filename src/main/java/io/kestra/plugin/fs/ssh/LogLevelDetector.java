package io.kestra.plugin.fs.ssh;

import org.slf4j.event.Level;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

// The detected level is never below INFO so that a line can never be dropped by the task's `logLevel`.
final class LogLevelDetector {
    private static final int MAX_PREFIX_LENGTH = 128;

    private static final Map<String, Level> LEVELS = Map.ofEntries(
        Map.entry("FATAL", Level.ERROR),
        Map.entry("CRITICAL", Level.ERROR),
        Map.entry("SEVERE", Level.ERROR),
        Map.entry("ERROR", Level.ERROR),
        Map.entry("WARN", Level.WARN),
        Map.entry("WARNING", Level.WARN),
        Map.entry("INFO", Level.INFO),
        Map.entry("NOTICE", Level.INFO),
        Map.entry("DEBUG", Level.INFO),
        Map.entry("FINE", Level.INFO),
        Map.entry("TRACE", Level.INFO),
        Map.entry("FINER", Level.INFO),
        Map.entry("FINEST", Level.INFO)
    );

    // A timestamp is only skipped when it has a date or time separator, so `404 error page` is not one.
    private static final String TIMESTAMP =
        "(?:\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2}(?:[.,]\\d{1,9})?(?:Z|[+-]\\d{2}:?\\d{2})?|\\d{2}:\\d{2}:\\d{2}(?:[.,]\\d{1,9})?)";

    // DEBUG/TRACE-like tokens are only accepted in brackets: `debug: connection refused` is data, not a level.
    private static final Pattern PATTERN = Pattern.compile(
        "^\\s{0,4}(?:" + TIMESTAMP + "\\s{1,4})?(?:"
            + "\\[(FATAL|CRITICAL|SEVERE|ERROR|WARNING|WARN|INFO|NOTICE|DEBUG|TRACE|FINEST|FINER|FINE)]"
            + "|(FATAL|CRITICAL|SEVERE|ERROR|WARNING|WARN|INFO|NOTICE)(?=[\\s:]|$))",
        Pattern.CASE_INSENSITIVE
    );

    private LogLevelDetector() {
    }

    static Optional<Level> detect(String line) {
        if (line == null || line.isEmpty()) {
            return Optional.empty();
        }

        var prefix = line.length() > MAX_PREFIX_LENGTH ? line.substring(0, MAX_PREFIX_LENGTH) : line;
        var matcher = PATTERN.matcher(prefix);
        if (!matcher.find()) {
            return Optional.empty();
        }

        var token = matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
        return Optional.ofNullable(LEVELS.get(token.toUpperCase()));
    }
}
