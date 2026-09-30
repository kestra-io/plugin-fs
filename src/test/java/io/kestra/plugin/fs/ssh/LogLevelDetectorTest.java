package io.kestra.plugin.fs.ssh;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.event.Level;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class LogLevelDetectorTest {
    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
        "WARNING:root:disk almost full|WARN",
        "2026-01-01 10:00:00 ERROR boom|ERROR",
        "2026-01-01 10:00:00,123 [INFO] x|INFO",
        "2026-01-01T10:00:00Z warn: x|WARN",
        "10:00:00 ERROR x|ERROR",
        "[DEBUG] x|INFO",
        "[TRACE] x|INFO",
        "[FINEST] x|INFO",
        "SEVERE: x|ERROR",
        "CRITICAL x|ERROR",
        "FATAL x|ERROR",
        "NOTICE x|INFO",
        "error: x|ERROR",
        "[warn] x|WARN"
    })
    void detect_recognizesDeclaredLevel(String line, Level expected) {
        assertThat(LogLevelDetector.detect(line).orElse(null), is(expected));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "debug: true",
        "debug: connection refused by 10.0.0.5",
        "verbose: false",
        "404 error page served",
        "Trace file written to /tmp/build.trace",
        "Debug symbols stripped from libfoo.so",
        "Errors found: 3",
        "Informational message",
        "::{\"outputs\":{\"a\":1}}::",
        "",
        "Permission denied"
    })
    void detect_ignoresLinesWithoutDeclaredLevel(String line) {
        assertThat(LogLevelDetector.detect(line).isPresent(), is(false));
    }

    @Test
    void detect_neverThrowsOnVeryLongOrNullLines() {
        assertThat(LogLevelDetector.detect("1".repeat(100_000)).isPresent(), is(false));
        assertThat(LogLevelDetector.detect(" ".repeat(100_000) + "ERROR x").isPresent(), is(false));
        assertThat(LogLevelDetector.detect(null).isPresent(), is(false));
    }
}
