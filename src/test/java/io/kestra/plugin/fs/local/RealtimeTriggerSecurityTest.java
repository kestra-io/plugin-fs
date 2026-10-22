package io.kestra.plugin.fs.local;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import com.google.re2j.PatternSyntaxException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Security-focused integration tests for RealtimeTrigger.
 * Tests allowed-path boundaries, symbolic-link rejection, and regex validation.
 */
@KestraTest
class RealtimeTriggerSecurityTest {
    @Inject
    private RunContextFactory runContextFactory;


    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }

        try (var paths = Files.walk(root)) {
            paths.sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    private static boolean causeChainContains(Throwable error, Class<? extends Throwable> expectedType) {
        Throwable current = error;
        while (current != null) {
            if (expectedType.isInstance(current)) {
                return true;
            }
            if (current.getCause() == current) {
                break;
            }
            current = current.getCause();
        }
        return false;
    }

    private static String causeChainMessages(Throwable error) {
        StringBuilder messages = new StringBuilder();
        Throwable current = error;
        while (current != null) {
            if (current.getMessage() != null) {
                if (messages.length() > 0) {
                    messages.append(" | ");
                }
                messages.append(current.getMessage());
            }
            if (current.getCause() == current) {
                break;
            }
            current = current.getCause();
        }
        return messages.toString().toLowerCase(Locale.ROOT);
    }

    private static Path pathOutsideConfiguredAllowedRoots() {
        Path filesystemRoot = Path.of(System.getProperty("user.dir")).toAbsolutePath().getRoot();
        if (filesystemRoot == null) {
            throw new AssertionError("Could not determine the filesystem root for an outside-allowed-path test");
        }
        // RealtimeTrigger validates the normalized path before checking whether it exists, so no
        // privileged directory creation is necessary. The root-level child is outside the test
        // allowlist configured in application.yml (/tmp on Unix-like systems and C:\\Users on Windows).
        return filesystemRoot.resolve("kestra-realtime-trigger-forbidden-" + UUID.randomUUID());
    }

    @Test
    @Timeout(20)
    void testSecurityPathOutsideAllowedRejected() throws Exception {
        Path allowedDir = Files.createTempDirectory("realtime-trigger-allowed-");
        try {
            Path allowedFile = allowedDir.resolve("allowed.txt");
            RealtimeTrigger allowedTrigger = RealtimeTrigger.builder()
                .id("test-path-allowed")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(allowedDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .build();
            var allowedContext = TestsUtils.mockTrigger(runContextFactory, allowedTrigger);
            List<Execution> allowedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> allowedError = new AtomicReference<>();
            CountDownLatch allowedEventLatch = new CountDownLatch(1);
            CountDownLatch probeObserved = new CountDownLatch(1);
            Disposable allowedSubscription = Flux.from(allowedTrigger.evaluate(allowedContext.getKey(), allowedContext.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                            return;
                        }
                        allowedExecutions.add(execution);
                        allowedEventLatch.countDown();
                    },
                    allowedError::set
                );

            try {
                RealtimeTriggerTestSupport.awaitWatcherReady(allowedDir, RealtimeTrigger.EventType.CREATE, ".realtime-trigger-probe-allowed.txt", probeObserved);

                Files.writeString(allowedFile, "allowed", StandardCharsets.UTF_8);
                assertThat("Expected CREATE event inside configured allowed paths",
                    allowedEventLatch.await(5, TimeUnit.SECONDS), is(true));
                assertThat("Allowed-path watcher must not fail", allowedError.get(), nullValue());
                assertThat("Expected one allowed-path execution", allowedExecutions.size(), is(1));

                @SuppressWarnings("unchecked")
                Map<String, Object> fileVariables = (Map<String, Object>) allowedExecutions.get(0)
                    .getTrigger().getVariables().get("file");
                assertThat(fileVariables.get("name"), equalTo("allowed.txt"));
            } finally {
                allowedTrigger.stop();
                allowedSubscription.dispose();
            }

            // The trigger validates allowed-path boundaries before checking whether the path exists,
            // so this root-level path exercises rejection without writing outside the test workspace.
            Path forbiddenPath = pathOutsideConfiguredAllowedRoots();
            RealtimeTrigger forbiddenTrigger = RealtimeTrigger.builder()
                .id("test-path-forbidden")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(forbiddenPath.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .build();
            var forbiddenContext = TestsUtils.mockTrigger(runContextFactory, forbiddenTrigger);
            AtomicReference<Throwable> forbiddenError = new AtomicReference<>();
            CountDownLatch forbiddenErrorLatch = new CountDownLatch(1);
            Disposable forbiddenSubscription = Flux.from(forbiddenTrigger.evaluate(forbiddenContext.getKey(), forbiddenContext.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {},
                    error -> {
                        forbiddenError.set(error);
                        forbiddenErrorLatch.countDown();
                    }
                );

            try {
                assertThat("Expected watcher startup to reject a path outside allowed-paths",
                    forbiddenErrorLatch.await(5, TimeUnit.SECONDS), is(true));
                assertThat("Expected a security exception in the error chain",
                    causeChainContains(forbiddenError.get(), SecurityException.class), is(true));
                assertThat("Error should identify the allowed-paths restriction",
                    causeChainMessages(forbiddenError.get()), containsString("allowed-path"));
            } finally {
                forbiddenTrigger.stop();
                forbiddenSubscription.dispose();
            }
        } finally {
            deleteRecursively(allowedDir);
        }
    }

    @Test
    @Timeout(20)
    void testSecuritySymlinkRejected() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-watch-root-");
        Path externalDir = Files.createTempDirectory("realtime-trigger-external-");
        try {
            Path symlinkInTempDir = tempDir.resolve("evil-link");
            Path externalFile = externalDir.resolve("file.txt");
            Path barrierFile = tempDir.resolve("symlink-registration-barrier.txt");

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-symlink-rejection")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .recursive(Property.ofValue(true))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .build();
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            List<Execution> receivedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            CountDownLatch barrierLatch = new CountDownLatch(1);
            CountDownLatch externalFileLatch = new CountDownLatch(1);
            CountDownLatch terminationLatch = new CountDownLatch(1);
            CountDownLatch probeObserved = new CountDownLatch(1);
            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .doFinally(signalType -> terminationLatch.countDown())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                            return;
                        }
                        receivedExecutions.add(execution);
                        @SuppressWarnings("unchecked")
                        Map<String, Object> fileVariables = (Map<String, Object>) execution.getTrigger()
                            .getVariables().get("file");
                        String name = (String) fileVariables.get("name");
                        if ("symlink-registration-barrier.txt".equals(name)) {
                            barrierLatch.countDown();
                        } else if ("file.txt".equals(name)) {
                            externalFileLatch.countDown();
                        }
                    },
                    watcherError::set
                );

            try {
                RealtimeTriggerTestSupport.awaitWatcherReady(tempDir, RealtimeTrigger.EventType.CREATE, ".realtime-trigger-probe-symlink.txt", probeObserved);

                try {
                    Files.createSymbolicLink(symlinkInTempDir, externalDir);
                } catch (UnsupportedOperationException | IOException | SecurityException e) {
                    Assumptions.assumeTrue(false,
                        "Symbolic links are unavailable in this environment: " + e.getMessage());
                    return;
                }

                // Both paths are entries in the watched root. Processing this barrier proves that
                // the preceding symlink ENTRY_CREATE event has been handled by the event loop.
                Files.createFile(barrierFile);
                assertThat("Watcher must process the event-ordering barrier",
                    barrierLatch.await(5, TimeUnit.SECONDS), is(true));
                assertThat("Watcher must not fail while rejecting the symlink", watcherError.get(), nullValue());
                assertThat("Watcher must still be active after the barrier", terminationLatch.getCount(), is(1L));
                assertThat("The symlink itself must not emit an execution", receivedExecutions.size(), is(1));

                Files.writeString(symlinkInTempDir.resolve("file.txt"), "content", StandardCharsets.UTF_8);
                assertThat("The write through the symlink should reach its external target", Files.exists(externalFile), is(true));
                boolean externalFileDetected = externalFileLatch.await(2, TimeUnit.SECONDS);

                assertThat("File created through the symlink must not be detected", externalFileDetected, is(false));
                assertThat("Watcher must not fail during the observation window", watcherError.get(), nullValue());
                assertThat("Watcher must remain active during the observation window", terminationLatch.getCount(), is(1L));
                assertThat("Only the barrier file should have generated an execution", receivedExecutions.stream()
                    .map(execution -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> fileVariables = (Map<String, Object>) execution.getTrigger()
                            .getVariables().get("file");
                        return (String) fileVariables.get("name");
                    }).toList(), everyItem(equalTo("symlink-registration-barrier.txt")));
            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
            deleteRecursively(externalDir);
        }
    }

    @Test
    @Timeout(15)
    void testSecurityRegexLengthLimitThrowsException() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-long-regex-");
        try {
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-regex-length-limit")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .regExp(Property.ofValue("a".repeat(1001)))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            AtomicReference<Throwable> capturedError = new AtomicReference<>();
            CountDownLatch errorLatch = new CountDownLatch(1);

            Disposable subscription = Flux.from(
                trigger.evaluate(context.getKey(), context.getValue())
            )
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {},
                    error -> {
                        capturedError.set(error);
                        errorLatch.countDown();
                    }
                );

            try {
                assertThat(
                    "Overlong regex must fail before watcher startup",
                    errorLatch.await(5, TimeUnit.SECONDS),
                    is(true)
                );

                Throwable error = capturedError.get();

                assertThat(
                    "Expected an IllegalArgumentException for an overlong regex",
                    causeChainContains(error, IllegalArgumentException.class),
                    is(true)
                );

                assertThat(
                    "Error should identify the regex length limit",
                    causeChainMessages(error),
                    containsString("must not exceed")
                );
            }finally {
                trigger.stop();
                subscription.dispose();
            }
        }finally {
            deleteRecursively(tempDir);
        }
    }





    @Timeout(15)
    void testSecurityRepeatedTopLevelQuantifiersRejected(String regex) throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-repeated-quantifiers-");
        try {
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-repeated-top-level-quantifiers")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .regExp(Property.ofValue(regex))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            var capturedError = new AtomicReference<Throwable>();
            var errorLatch = new CountDownLatch(1);

            Disposable subscription = Flux.from(
                trigger.evaluate(context.getKey(), context.getValue())
            )
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {},
                    error -> {
                        capturedError.set(error);
                        errorLatch.countDown();
                    }
                );

            try {
                assertThat(
                    "Unsafe top-level quantifiers must be rejected: " + regex,
                    errorLatch.await(5, TimeUnit.SECONDS),
                    is(true)
                );
                assertThat(
                    "Expected an IllegalArgumentException for unsafe backtracking",
                    causeChainContains(capturedError.get(), IllegalArgumentException.class),
                    is(true)
                );
                assertThat(
                    "Error should identify unsafe backtracking",
                    causeChainMessages(capturedError.get()),
                    containsString("catastrophic backtracking")
                );
            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }




    @Test
    @Timeout(15)
    void testSecurityValidGroupedQuantifiersAccepted() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-valid-regex-");
        try {
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-valid-grouped-quantifiers")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .regExp(Property.ofValue(".*(?:[a-z]+|[0-9]+).*\\.txt"))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            CountDownLatch probeObserved = new CountDownLatch(1);
            AtomicReference<Throwable> watcherError = new AtomicReference<>();

            Disposable subscription = Flux.from(
                trigger.evaluate(context.getKey(), context.getValue())
            )
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                        }
                    },
                    watcherError::set
                );

            try {
                RealtimeTriggerTestSupport.awaitWatcherReady(
                    tempDir,
                    RealtimeTrigger.EventType.CREATE,
                    ".realtime-trigger-probe-valid-regex.txt",
                    probeObserved
                );

                assertThat(
                    "A valid grouped regex must allow the watcher to start",
                    watcherError.get(),
                    nullValue()
                );
            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    @Timeout(15)
    void testSecuritySimpleQuantifierAccepted() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-simple-regex-");
        try {
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-simple-quantifier")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .regExp(Property.ofValue(".*a+.*"))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            var executions = new CopyOnWriteArrayList<Execution>();
            var probeObserved = new CountDownLatch(1);
            var matchingFileObserved = new CountDownLatch(1);
            var watcherError = new AtomicReference<Throwable>();

            Disposable subscription = Flux.from(
                trigger.evaluate(context.getKey(), context.getValue())
            )
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                            return;
                        }

                        executions.add(execution);
                        matchingFileObserved.countDown();
                    },
                    watcherError::set
                );

            try {
                RealtimeTriggerTestSupport.awaitWatcherReady(
                    tempDir,
                    RealtimeTrigger.EventType.CREATE,
                    ".realtime-trigger-probe-simple-regex.txt",
                    probeObserved
                );

                Path matchingFile = tempDir.resolve("alpha.txt");
                Files.writeString(matchingFile, "test", StandardCharsets.UTF_8);

                assertThat(
                    "A file matching a valid simple quantifier should emit an execution",
                    matchingFileObserved.await(5, TimeUnit.SECONDS),
                    is(true)
                );

                assertThat("Watcher should not fail", watcherError.get(), nullValue());
            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    @Timeout(15)
    void testSecurityBoundedGroupWithInnerQuantifierAccepted() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-repeated-group-");
        try {
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-bounded-group-with-inner-quantifier")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .regExp(Property.ofValue("(.*){1}"))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            var executions = new CopyOnWriteArrayList<Execution>();
            var probeObserved = new CountDownLatch(1);
            var matchingFileObserved = new CountDownLatch(1);
            var watcherError = new AtomicReference<Throwable>();

            Disposable subscription = Flux.from(
                trigger.evaluate(context.getKey(), context.getValue())
            )
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                        return;
                    }

                    executions.add(execution);
                    matchingFileObserved.countDown();
                },
                watcherError::set
            );

            try {
                RealtimeTriggerTestSupport.awaitWatcherReady(
                    tempDir,
                    RealtimeTrigger.EventType.CREATE,
                    ".realtime-trigger-probe-repeated-group.txt",
                    probeObserved
                );

                Files.writeString(
                    tempDir.resolve("abab.txt"),
                    "test",
                    StandardCharsets.UTF_8
                );

                assertThat(
                    "A repeated group without inner quantifiers should match",
                    matchingFileObserved.await(5, TimeUnit.SECONDS),
                    is(true)
                );

                assertThat("Watcher should not fail", watcherError.get(), nullValue());

                assertThat("Expected exactly one matching execution", executions.size(), is(1));

                @SuppressWarnings("unchecked")
                Map<String, Object> fileVariables = (Map<String, Object>) executions.get(0)
                .getTrigger().getVariables().get("file");

                assertThat(fileVariables.get("name"), equalTo("abab.txt"));
            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }


    @Test
    @Timeout(15)
    void testSecurityInvalidRegexThrowsException() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-invalid-regex-");
        try {
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-invalid-regex")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .regExp(Property.ofValue("[invalid(regex"))
                .build();
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            AtomicReference<Throwable> capturedError = new AtomicReference<>();
            CountDownLatch errorLatch = new CountDownLatch(1);
            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {},
                    error -> {
                        capturedError.set(error);
                        errorLatch.countDown();
                    }
                );

            try {
                assertThat("Invalid regex must fail before watcher startup",
                    errorLatch.await(5, TimeUnit.SECONDS), is(true));
                Throwable error = capturedError.get();
                assertThat("Expected an IllegalArgumentException in the error chain",
                    causeChainContains(error, IllegalArgumentException.class), is(true));
                assertThat("Expected PatternSyntaxException in the cause chain",
                    causeChainContains(error, PatternSyntaxException.class), is(true));
                assertThat("Error should mention regex validation",
                    causeChainMessages(error), anyOf(containsString("regex"), containsString("pattern")));
            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    @Timeout(15)
    void testSecurityBoundedQuantifierAccepted() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-bounded-quantifier-");
        try {
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-bounded-quantifier")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .regExp(Property.ofValue(
                    ".*a{2,4}.*|.*\\.realtime-trigger-probe-bounded-quantifier\\.txt"
                ))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            var executions = new CopyOnWriteArrayList<Execution>();
            var probeObserved = new CountDownLatch(1);
            var matchingFileObserved = new CountDownLatch(1);
            var watcherError = new AtomicReference<Throwable>();

            Disposable subscription = Flux.from(
                trigger.evaluate(context.getKey(), context.getValue())
            )
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                            return;
                        }

                        executions.add(execution);
                        matchingFileObserved.countDown();
                    },
                    watcherError::set
                );

            try {
                RealtimeTriggerTestSupport.awaitWatcherReady(
                    tempDir,
                    RealtimeTrigger.EventType.CREATE,
                    ".realtime-trigger-probe-bounded-quantifier.txt",
                    probeObserved
                );

                Files.writeString(
                    tempDir.resolve("aaa.txt"),
                    "test",
                    StandardCharsets.UTF_8
                );

                assertThat(
                    "A bounded quantifier should match aaa.txt",
                    matchingFileObserved.await(5, TimeUnit.SECONDS),
                    is(true)
                );

                assertThat("Watcher should not fail", watcherError.get(), nullValue());
                assertThat("Expected exactly one matching execution", executions.size(), is(1));

                @SuppressWarnings("unchecked")
                Map<String, Object> fileVariables = (Map<String, Object>) executions.get(0)
                    .getTrigger().getVariables().get("file");

                assertThat(fileVariables.get("name"), equalTo("aaa.txt"));
            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    @Timeout(15)
    void testSecurityEscapedQuantifierCharactersAccepted() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-escaped-quantifier-");
        try {
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-escaped-quantifier-characters")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .regExp(Property.ofValue(".*literal\\+star\\*\\.txt|.*\\.realtime-trigger-probe-escaped-quantifier\\.txt"))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            var executions = new CopyOnWriteArrayList<Execution>();
            var probeObserved = new CountDownLatch(1);
            var matchingFileObserved = new CountDownLatch(1);
            var watcherError = new AtomicReference<Throwable>();

            Disposable subscription = Flux.from(
                trigger.evaluate(context.getKey(), context.getValue())
            )
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                            return;
                        }

                        executions.add(execution);
                        matchingFileObserved.countDown();
                    },
                    watcherError::set
                );

            try {
                RealtimeTriggerTestSupport.awaitWatcherReady(
                    tempDir,
                    RealtimeTrigger.EventType.CREATE,
                    ".realtime-trigger-probe-escaped-quantifier.txt",
                    probeObserved
                );

                Files.writeString(
                    tempDir.resolve("literal+star*.txt"),
                    "test",
                    StandardCharsets.UTF_8
                );

                assertThat(
                    "Escaped quantifier characters should match as literals",
                    matchingFileObserved.await(5, TimeUnit.SECONDS),
                    is(true)
                );
                assertThat("Watcher should not fail", watcherError.get(), nullValue());
                assertThat("Expected exactly one matching execution", executions.size(), is(1));

                @SuppressWarnings("unchecked")
                Map<String, Object> fileVariables = (Map<String, Object>) executions.get(0)
                    .getTrigger().getVariables().get("file");

                assertThat(fileVariables.get("name"), equalTo("literal+star*.txt"));
            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    @Timeout(15)
    void testSecurityQuantifierCharactersInsideCharacterClassAccepted() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-quantifier-character-class-");
        try {
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-quantifiers-inside-character-class")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .regExp(Property.ofValue(
                    ".*[+*?]\\.txt|.*\\.realtime-trigger-probe-quantifier-class\\.txt"
                ))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            var executions = new CopyOnWriteArrayList<Execution>();
            var probeObserved = new CountDownLatch(1);
            var matchingFileObserved = new CountDownLatch(1);
            var watcherError = new AtomicReference<Throwable>();

            Disposable subscription = Flux.from(
                trigger.evaluate(context.getKey(), context.getValue())
            )
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                            return;
                        }

                        executions.add(execution);
                        matchingFileObserved.countDown();
                    },
                    watcherError::set
                );

            try {
                RealtimeTriggerTestSupport.awaitWatcherReady(
                tempDir,
                    RealtimeTrigger.EventType.CREATE,
                    ".realtime-trigger-probe-quantifier-class.txt",
                    probeObserved
                );

                Files.writeString(
                    tempDir.resolve("file+.txt"),
                    "test",
                    StandardCharsets.UTF_8
                );

                assertThat(
                    "Quantifier symbols inside a character class should match literally",
                    matchingFileObserved.await(5, TimeUnit.SECONDS),
                    is(true)
                );
                assertThat("Watcher should not fail", watcherError.get(), nullValue());
                assertThat("Expected exactly one matching execution", executions.size(), is(1));

                @SuppressWarnings("unchecked")
                Map<String, Object> fileVariables = (Map<String, Object>) executions.get(0)
                    .getTrigger().getVariables().get("file");

                assertThat(fileVariables.get("name"), equalTo("file+.txt"));
            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }
}
