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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Integration tests for RealtimeTrigger.
 */
@KestraTest
class RealtimeTriggerTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static final Duration WATCHER_READY_TIMEOUT = Duration.ofSeconds(5);

    /**
     * Wait for the trigger to become ready (watcher initialized and active).
     */

    /**
     * Recursively delete a directory and all its contents.
     */
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

    @Test
    @Timeout(15)
    void testCreateEvent() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path testFile = tempDir.resolve("test.txt");

        try {
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-create")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            List<Execution> receivedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            CountDownLatch eventLatch = new CountDownLatch(1);

            CountDownLatch probeObserved = new CountDownLatch(1);
            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                            return;
                        }
                        receivedExecutions.add(execution);
                        eventLatch.countDown();
                    },
                    watcherError::set
                );

            try {

                RealtimeTriggerTestSupport.awaitWatcherReady(tempDir, RealtimeTrigger.EventType.CREATE, ".realtime-trigger-probe-create.txt", probeObserved);

                Files.writeString(testFile, "test content", StandardCharsets.UTF_8);

                assertThat(
                    "Expected CREATE event",
                    eventLatch.await(5, TimeUnit.SECONDS),
                    is(true)
                );

                assertThat("Watcher must not fail", watcherError.get(), nullValue());
                assertThat("Should have received execution", receivedExecutions.size(), is(1));

                Execution execution = receivedExecutions.get(0);
                Map<String, Object> variables = execution.getTrigger().getVariables();
                assertThat(variables.get("changeType"), equalTo("CREATE"));

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
    void testCreateEventOutputContract() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path testFile = tempDir.resolve("test.txt");

        try {
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-output-contract")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            List<Execution> receivedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            CountDownLatch eventLatch = new CountDownLatch(1);

            CountDownLatch probeObserved = new CountDownLatch(1);
            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                            return;
                        }
                        receivedExecutions.add(execution);
                        eventLatch.countDown();
                    },
                    watcherError::set
                );

            try {

                RealtimeTriggerTestSupport.awaitWatcherReady(tempDir, RealtimeTrigger.EventType.CREATE, ".realtime-trigger-probe-output.txt", probeObserved);

                Files.writeString(testFile, "test content", StandardCharsets.UTF_8);

                assertThat(
                    "Expected CREATE event",
                    eventLatch.await(5, TimeUnit.SECONDS),
                    is(true)
                );

                assertThat("Watcher must not fail", watcherError.get(), nullValue());
                assertThat("Should have received execution", receivedExecutions.size(), is(1));

                Execution execution = receivedExecutions.get(0);
                Map<String, Object> variables = execution.getTrigger().getVariables();

                assertThat(variables, hasKey("file"));
                assertThat(variables, hasKey("changeType"));
                assertThat(variables.get("changeType"), equalTo("CREATE"));

                @SuppressWarnings("unchecked")
                Map<String, Object> fileVariables = (Map<String, Object>) variables.get("file");

                assertThat(fileVariables.get("name"), equalTo("test.txt"));
                assertThat(
                    fileVariables.get("localPath").toString(),
                    equalTo(testFile.toAbsolutePath().normalize().toUri().toString())
                );
                assertThat(fileVariables, hasKey("uri"));

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
    void testModifyEvent() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path testFile = tempDir.resolve("test.txt");

        try {
            Files.writeString(testFile, "initial content", StandardCharsets.UTF_8);

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-modify")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.UPDATE))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            List<Execution> receivedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            CountDownLatch eventLatch = new CountDownLatch(1);

            CountDownLatch probeObserved = new CountDownLatch(1);
            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                            return;
                        }
                        receivedExecutions.add(execution);
                        eventLatch.countDown();
                    },
                    watcherError::set
                );

            try {
                RealtimeTriggerTestSupport.awaitWatcherReady(tempDir, RealtimeTrigger.EventType.UPDATE, ".realtime-trigger-probe-modify.txt", probeObserved);

                Files.writeString(testFile, "modified content", StandardCharsets.UTF_8);

                assertThat(
                    "Expected UPDATE event",
                    eventLatch.await(5, TimeUnit.SECONDS),
                    is(true)
                );

                assertThat("Watcher must not fail", watcherError.get(), nullValue());
                assertThat("Should have received execution", receivedExecutions.size(), is(1));

                Execution execution = receivedExecutions.get(0);
                Map<String, Object> variables = execution.getTrigger().getVariables();
                assertThat(variables.get("changeType"), equalTo("UPDATE"));

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
    void testDeleteEvent() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path testFile = tempDir.resolve("test.txt");

        try {
            Files.writeString(testFile, "content", StandardCharsets.UTF_8);

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-delete")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.DELETE))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            List<Execution> receivedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            CountDownLatch eventLatch = new CountDownLatch(1);

            CountDownLatch probeObserved = new CountDownLatch(1);
            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                            return;
                        }
                        receivedExecutions.add(execution);
                        eventLatch.countDown();
                    },
                    watcherError::set
                );
                RealtimeTriggerTestSupport.awaitWatcherReady(tempDir, RealtimeTrigger.EventType.DELETE, ".realtime-trigger-probe-delete.txt", probeObserved);

            try {

                Files.deleteIfExists(testFile);

                assertThat(
                    "Expected DELETE event",
                    eventLatch.await(5, TimeUnit.SECONDS),
                    is(true)
                );

                assertThat("Watcher must not fail", watcherError.get(), nullValue());
                assertThat("Should have received execution", receivedExecutions.size(), is(1));

                Execution execution = receivedExecutions.get(0);
                Map<String, Object> variables = execution.getTrigger().getVariables();
                assertThat(variables.get("changeType"), equalTo("DELETE"));

                @SuppressWarnings("unchecked")
                Map<String, Object> fileVariables = (Map<String, Object>) variables.get("file");
                assertThat(fileVariables.get("name"), equalTo("test.txt"));

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
    void testDeleteDirectoryDoesNotEmitExecution() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path childDir = tempDir.resolve("subdir");

        try {
            Files.createDirectory(childDir);

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-delete-dir-suppression")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.DELETE))
                .recursive(Property.ofValue(false))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            List<Execution> receivedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            CountDownLatch eventLatch = new CountDownLatch(1);
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
                        eventLatch.countDown();
                    },
                    watcherError::set
                );

            try {
                RealtimeTriggerTestSupport.awaitWatcherReady(
                    tempDir,
                    RealtimeTrigger.EventType.DELETE,
                    ".realtime-trigger-probe-delete-directory.txt",
                    probeObserved
                );

                Files.delete(childDir);

                boolean eventArrived = eventLatch.await(2, TimeUnit.SECONDS);

                assertThat("Watcher must not fail", watcherError.get(), nullValue());
                assertThat("Watcher must remain active during observation", terminationLatch.getCount(), is(1L));
                assertThat("Directory deletion should not emit execution", eventArrived, is(false));
                assertThat("No executions should be received", receivedExecutions, empty());
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
    void testDeleteNestedDirectoryDoesNotEmitExecution() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path newDir = tempDir.resolve("new");
        Path nestedDir = newDir.resolve("nested");

        try {
            // Pre-create the nested hierarchy so startup registration is the behavior under test.
            Files.createDirectories(nestedDir);

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-delete-nested-dir-suppression")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.DELETE))
                .recursive(Property.ofValue(true))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            List<Execution> receivedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            CountDownLatch eventLatch = new CountDownLatch(1);
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
                        eventLatch.countDown();
                    },
                    watcherError::set
                );

            try {
                RealtimeTriggerTestSupport.awaitWatcherReady(
                    tempDir,
                    RealtimeTrigger.EventType.DELETE,
                    ".realtime-trigger-probe-delete-nested-directory.txt",
                    probeObserved
                );

                Files.delete(nestedDir);

                boolean eventArrived = eventLatch.await(2, TimeUnit.SECONDS);

                assertThat("Watcher must not fail", watcherError.get(), nullValue());
                assertThat("Watcher must remain active during observation", terminationLatch.getCount(), is(1L));
                assertThat("Nested directory deletion should not emit execution", eventArrived, is(false));
                assertThat("No executions should be received", receivedExecutions, empty());
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
    void testRegexFilter() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path csvFile = tempDir.resolve("data.csv");
        Path txtFile = tempDir.resolve("readme.txt");

        try {
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-regex-filter")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .regExp(Property.ofValue(".*\\.csv$"))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            List<Execution> receivedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            CountDownLatch eventLatch = new CountDownLatch(1);

            CountDownLatch probeObserved = new CountDownLatch(1);
            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                            return;
                        }
                        receivedExecutions.add(execution);
                        eventLatch.countDown();
                    },
                    watcherError::set
                );

            try {

                RealtimeTriggerTestSupport.awaitWatcherReady(tempDir, RealtimeTrigger.EventType.CREATE, ".realtime-trigger-probe-filter.csv", probeObserved);

                Files.writeString(txtFile, "readme", StandardCharsets.UTF_8);
                Files.writeString(csvFile, "header1,header2", StandardCharsets.UTF_8);

                assertThat(
                    "Expected CSV event",
                    eventLatch.await(5, TimeUnit.SECONDS),
                    is(true)
                );

                assertThat("Watcher must not fail", watcherError.get(), nullValue());
                assertThat("Should have received one execution", receivedExecutions.size(), is(1));

                Execution execution = receivedExecutions.get(0);
                @SuppressWarnings("unchecked")
                Map<String, Object> fileVariables = (Map<String, Object>) execution.getTrigger().getVariables().get("file");
                assertThat(fileVariables.get("name"), equalTo("data.csv"));

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
    void testStopAndCleanup() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");

        try {
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-stop-cleanup")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            CountDownLatch terminationLatch = new CountDownLatch(1);

            CountDownLatch probeObserved = new CountDownLatch(1);
            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .doFinally(signalType -> terminationLatch.countDown())
                .subscribe(execution -> {
                    if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                        probeObserved.countDown();
                    }
                }, watcherError::set);

            try {
                RealtimeTriggerTestSupport.awaitWatcherReady(tempDir, RealtimeTrigger.EventType.CREATE, ".realtime-trigger-probe-stop.txt", probeObserved);

                assertThat("Stream should not have terminated before stop()", terminationLatch.getCount(), is(1L));
                assertThat("No error should have occurred before stop()", watcherError.get(), nullValue());

                trigger.stop();

                assertThat("Watcher should terminate after stop()", terminationLatch.await(3, TimeUnit.SECONDS), is(true));

                Throwable error = watcherError.get();
                if (error != null && !(error instanceof CancellationException)) {
                    throw new AssertionError("Unexpected error after stop()", error);
                }
            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    @Timeout(20)
    void testRecursiveDirectoryRegistration() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path subDir = tempDir.resolve("subdir");
        Path fileInSubDir = subDir.resolve("file.txt");
        Path barrierFile = tempDir.resolve("registration-barrier.txt");

        try {
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-recursive-registration")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .recursive(Property.ofValue(true))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            List<Execution> receivedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            CountDownLatch registrationBarrierLatch = new CountDownLatch(1);
            CountDownLatch fileLatch = new CountDownLatch(1);

            CountDownLatch probeObserved = new CountDownLatch(1);
            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
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
                        String fileName = (String) fileVariables.get("name");
                        if ("registration-barrier.txt".equals(fileName)) {
                            registrationBarrierLatch.countDown();
                        } else if ("file.txt".equals(fileName)) {
                            fileLatch.countDown();
                        }
                    },
                    watcherError::set
                );
                RealtimeTriggerTestSupport.awaitWatcherReady(tempDir, RealtimeTrigger.EventType.CREATE, ".realtime-trigger-probe-recursive-reg.txt", probeObserved);

            try {

                Files.createDirectory(subDir);
                // ENTRY_CREATE events for the root are processed in order. Processing the directory
                // event registers subDir synchronously before the subsequent barrier-file event.
                Files.createFile(barrierFile);
                assertThat(
                    "Watcher must process the registration barrier",
                    registrationBarrierLatch.await(5, TimeUnit.SECONDS),
                    is(true)
                );

                Files.writeString(fileInSubDir, "content", StandardCharsets.UTF_8);
                assertThat(
                    "Expected CREATE event in the dynamically registered subdirectory",
                    fileLatch.await(5, TimeUnit.SECONDS),
                    is(true)
                );
                assertThat("Watcher must not fail", watcherError.get(), nullValue());

                Execution fileExecution = receivedExecutions.stream()
                    .filter(execution -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> fileVariables = (Map<String, Object>) execution.getTrigger()
                            .getVariables().get("file");
                        return "file.txt".equals(fileVariables.get("name"));
                    })
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("File event not found"));

                @SuppressWarnings("unchecked")
                Map<String, Object> fileVariables = (Map<String, Object>) fileExecution.getTrigger()
                    .getVariables().get("file");
                assertThat(fileVariables.get("name"), equalTo("file.txt"));
                assertThat(
                    fileVariables.get("localPath").toString(),
                    equalTo(fileInSubDir.toAbsolutePath().normalize().toUri().toString())
                );
            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }



    // ========== BEHAVIOR TESTS (3) ==========

    @Test
    @Timeout(25)
    void testCreateOrUpdateDefaultIncludesBoth() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path testFile = tempDir.resolve("test.txt");
        Path barrierFile = tempDir.resolve("registration-barrier.txt");

        try {
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-default-create-or-update")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                // .on() intentionally omitted: the default is CREATE_OR_UPDATE.
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            List<Execution> receivedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            CountDownLatch createLatch = new CountDownLatch(1);
            CountDownLatch registrationBarrierLatch = new CountDownLatch(1);
            CountDownLatch updateLatch = new CountDownLatch(1);
            AtomicBoolean acceptUpdates = new AtomicBoolean(false);

            CountDownLatch probeObserved = new CountDownLatch(1);
            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                            return;
                        }
                        Map<String, Object> variables = execution.getTrigger().getVariables();
                        @SuppressWarnings("unchecked")
                        Map<String, Object> fileVariables = (Map<String, Object>) variables.get("file");
                        String fileName = (String) fileVariables.get("name");
                        String changeType = (String) variables.get("changeType");

                        if ("registration-barrier.txt".equals(fileName)) {
                            registrationBarrierLatch.countDown();
                            return;
                        }
                        if (!"test.txt".equals(fileName)) {
                            return;
                        }

                        receivedExecutions.add(execution);
                        if ("CREATE".equals(changeType)) {
                            createLatch.countDown();
                        } else if ("UPDATE".equals(changeType) && acceptUpdates.get()) {
                            updateLatch.countDown();
                        }
                    },
                    watcherError::set
                );

            try {
                RealtimeTriggerTestSupport.awaitWatcherReady(tempDir, RealtimeTrigger.EventType.CREATE_OR_UPDATE, ".realtime-trigger-probe-both.txt", probeObserved);

                Files.createFile(testFile);
                assertThat("Expected CREATE event", createLatch.await(5, TimeUnit.SECONDS), is(true));

                // Process a later event from the same root key before enabling UPDATE observation.
                // This drains earlier events (including any incidental CREATE-side filesystem event).
                Files.createFile(barrierFile);
                assertThat("Watcher must process the registration barrier",
                    registrationBarrierLatch.await(5, TimeUnit.SECONDS), is(true));

                acceptUpdates.set(true);
                Files.writeString(testFile, "modified", StandardCharsets.UTF_8);
                assertThat("Expected explicit UPDATE event", updateLatch.await(5, TimeUnit.SECONDS), is(true));
                assertThat("Watcher must not fail", watcherError.get(), nullValue());

                List<String> changeTypes = receivedExecutions.stream()
                    .map(execution -> (String) execution.getTrigger().getVariables().get("changeType"))
                    .collect(Collectors.toList());
                assertThat(changeTypes, hasItem("CREATE"));
                assertThat(changeTypes, hasItem("UPDATE"));
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
    void testMultipleEventsInSequence() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path file1 = tempDir.resolve("file1.txt");
        Path file2 = tempDir.resolve("file2.txt");

        try {
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-multiple-events-single-watcher")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            List<Execution> receivedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            CountDownLatch eventLatch = new CountDownLatch(2);

            CountDownLatch probeObserved = new CountDownLatch(1);
            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                            return;
                        }
                        receivedExecutions.add(execution);
                        eventLatch.countDown();
                    },
                    watcherError::set
                );
                RealtimeTriggerTestSupport.awaitWatcherReady(tempDir, RealtimeTrigger.EventType.CREATE, ".realtime-trigger-probe-multi.txt", probeObserved);

            try {

                Files.writeString(file1, "file1", StandardCharsets.UTF_8);
                Files.writeString(file2, "file2", StandardCharsets.UTF_8);

                assertThat(
                    "Expected two events from one trigger instance",
                    eventLatch.await(5, TimeUnit.SECONDS),
                    is(true)
                );

                assertThat("Watcher must not fail", watcherError.get(), nullValue());
                assertThat("Should have received two executions", receivedExecutions.size(), is(2));

                List<String> fileNames = receivedExecutions.stream()
                    .map(exec -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> fileVars = (Map<String, Object>) exec.getTrigger().getVariables().get("file");
                        return (String) fileVars.get("name");
                    })
                    .collect(Collectors.toList());

                assertThat(
                    "Both files should be detected (unordered)",
                    fileNames,
                    containsInAnyOrder("file1.txt", "file2.txt")
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
    @Timeout(25)
    void testRegexFilterAcrossEventTypes() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path csvFile = tempDir.resolve("data.csv");
        Path txtFile = tempDir.resolve("readme.txt");
        Path barrierFile = tempDir.resolve("registration-barrier.csv");

        try {
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-regex-filter-across-events")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE_OR_UPDATE))
                .regExp(Property.ofValue(".*\\.csv$"))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            List<Execution> receivedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            CountDownLatch csvCreateLatch = new CountDownLatch(1);
            CountDownLatch registrationBarrierLatch = new CountDownLatch(1);
            CountDownLatch csvUpdateLatch = new CountDownLatch(1);
            AtomicBoolean acceptCsvUpdates = new AtomicBoolean(false);

            CountDownLatch probeObserved = new CountDownLatch(1);
            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                            return;
                        }
                        Map<String, Object> variables = execution.getTrigger().getVariables();
                        @SuppressWarnings("unchecked")
                        Map<String, Object> fileVariables = (Map<String, Object>) variables.get("file");
                        String fileName = (String) fileVariables.get("name");
                        String changeType = (String) variables.get("changeType");

                        if ("registration-barrier.csv".equals(fileName)) {
                            registrationBarrierLatch.countDown();
                            return;
                        }
                        if (!"data.csv".equals(fileName)) {
                            return;
                        }

                        receivedExecutions.add(execution);
                        if ("CREATE".equals(changeType)) {
                            csvCreateLatch.countDown();
                        } else if ("UPDATE".equals(changeType) && acceptCsvUpdates.get()) {
                            csvUpdateLatch.countDown();
                        }
                    },
                    watcherError::set
                );
                RealtimeTriggerTestSupport.awaitWatcherReady(tempDir, RealtimeTrigger.EventType.CREATE_OR_UPDATE, ".realtime-trigger-probe-regex.csv", probeObserved);

            try {
                Files.writeString(txtFile, "readme", StandardCharsets.UTF_8);
                Files.createFile(csvFile);
                assertThat("Expected CSV CREATE event", csvCreateLatch.await(5, TimeUnit.SECONDS), is(true));

                // The barrier is also a matching CSV event but is intentionally excluded from the
                // assertions. Its callback confirms the earlier root-directory CREATE was processed.
                Files.createFile(barrierFile);
                assertThat("Watcher must process the regex-filtered registration barrier",
                    registrationBarrierLatch.await(5, TimeUnit.SECONDS), is(true));

                acceptCsvUpdates.set(true);
                Files.writeString(csvFile, "col1,col2,col3", StandardCharsets.UTF_8);
                assertThat("Expected CSV UPDATE event", csvUpdateLatch.await(5, TimeUnit.SECONDS), is(true));
                assertThat("Watcher must not fail", watcherError.get(), nullValue());

                assertThat("Only data.csv should contribute to the test assertions",
                    receivedExecutions.stream().map(execution -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> fileVariables = (Map<String, Object>) execution.getTrigger()
                            .getVariables().get("file");
                        return (String) fileVariables.get("name");
                    }).toList(), everyItem(equalTo("data.csv")));

                List<String> changeTypes = receivedExecutions.stream()
                    .map(execution -> (String) execution.getTrigger().getVariables().get("changeType"))
                    .collect(Collectors.toList());
                assertThat("Should have observed CREATE", changeTypes, hasItem("CREATE"));
                assertThat("Should have observed UPDATE", changeTypes, hasItem("UPDATE"));
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
    void testDeleteEventFileMetadata() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path testFile = tempDir.resolve("test-delete.txt");

        try {
            Files.writeString(testFile, "content", StandardCharsets.UTF_8);

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-delete-metadata")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.DELETE))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            List<Execution> receivedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            CountDownLatch eventLatch = new CountDownLatch(1);

            CountDownLatch probeObserved = new CountDownLatch(1);
            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                            return;
                        }
                        receivedExecutions.add(execution);
                        eventLatch.countDown();
                    },
                    watcherError::set
                );
                RealtimeTriggerTestSupport.awaitWatcherReady(tempDir, RealtimeTrigger.EventType.DELETE, ".realtime-trigger-probe-delete2.txt", probeObserved);

            try {
                Files.delete(testFile);

                assertThat("Expected DELETE event", eventLatch.await(5, TimeUnit.SECONDS), is(true));
                assertThat("Watcher must not fail", watcherError.get(), nullValue());
                assertThat("Should have received one execution", receivedExecutions.size(), is(1));

                Map<String, Object> variables = receivedExecutions.get(0).getTrigger().getVariables();
                assertThat(variables.get("changeType"), equalTo("DELETE"));
                @SuppressWarnings("unchecked")
                Map<String, Object> fileVariables = (Map<String, Object>) variables.get("file");
                assertThat(fileVariables.get("name"), equalTo("test-delete.txt"));
                assertThat(
                    fileVariables.get("localPath").toString(),
                    equalTo(testFile.toAbsolutePath().normalize().toUri().toString())
                );
            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }

    // ========== LIFECYCLE TESTS (2) ==========

    @Test
    @Timeout(15)
    void testKillMethodCleanup() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");

        try {
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-kill-cleanup")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            CountDownLatch terminationLatch = new CountDownLatch(1);

            CountDownLatch probeObserved = new CountDownLatch(1);
            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .doFinally(signalType -> terminationLatch.countDown())
                .subscribe(execution -> {
                    if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                        probeObserved.countDown();
                    }
                }, watcherError::set);

            try {
                RealtimeTriggerTestSupport.awaitWatcherReady(tempDir, RealtimeTrigger.EventType.CREATE, ".realtime-trigger-probe-kill.txt", probeObserved);

                assertThat("Stream should not have terminated before kill()", terminationLatch.getCount(), is(1L));
                assertThat("No error should have occurred before kill()", watcherError.get(), nullValue());

                trigger.kill();

                assertThat("Watcher should terminate after kill()", terminationLatch.await(3, TimeUnit.SECONDS), is(true));

                Throwable error = watcherError.get();
                if (error != null && !(error instanceof CancellationException)) {
                    throw new AssertionError("Unexpected error after kill()", error);
                }
            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }
    @Test
    @Timeout(20)
    void testMovedDirectoryDoesNotLeaveStalePathsThatSuppressFileDeletions() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path outsideDir = Files.createTempDirectory("realtime-trigger-outside");
        Path subDir = tempDir.resolve("a");
        Path nestedDir = subDir.resolve("b");

        try {
            Files.createDirectories(nestedDir);

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-stale-directory-paths")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.DELETE))
                .recursive(Property.ofValue(true))
                // Exclude the removed directory path, but include barrier files and the final file "b".
                .regExp(Property.ofValue(".*(barrier[12]\\.txt|/b)$"))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            List<Execution> receivedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            CountDownLatch barrier1Latch = new CountDownLatch(1);
            CountDownLatch barrier2Latch = new CountDownLatch(1);
            CountDownLatch fileBLatch = new CountDownLatch(1);

            CountDownLatch probeObserved = new CountDownLatch(1);
            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                            return;
                        }
                        receivedExecutions.add(execution);
                        @SuppressWarnings("unchecked")
                        Map<String, Object> fileVars = (Map<String, Object>)
                            execution.getTrigger().getVariables().get("file");
                        String fileName = (String) fileVars.get("name");

                        if ("barrier1.txt".equals(fileName)) {
                            barrier1Latch.countDown();
                        } else if ("barrier2.txt".equals(fileName)) {
                            barrier2Latch.countDown();
                        } else if ("b".equals(fileName)) {
                            fileBLatch.countDown();
                        }
                    },
                    watcherError::set
                );

            try {
                RealtimeTriggerTestSupport.awaitWatcherReady(tempDir, RealtimeTrigger.EventType.DELETE, ".realtime-trigger-probe-barrier1.txt", probeObserved);


                Path movedDir = outsideDir.resolve("a");
                try {
                    Files.move(subDir, movedDir);
                } catch (java.nio.file.AccessDeniedException e) {
                    if (System.getProperty("os.name")
                        .toLowerCase(java.util.Locale.ROOT)
                        .contains("win")) {
                        org.junit.jupiter.api.Assumptions.abort(
                            "This Windows filesystem provider denied moving a watched directory: "
                                + e.getMessage()
                        );
                    }
                    throw e;
                }

                Path barrier1 = tempDir.resolve("barrier1.txt");
                Files.createFile(barrier1);
                Files.deleteIfExists(barrier1);

                assertThat(
                    "Barrier 1 DELETE confirms directory move processed",
                    barrier1Latch.await(5, TimeUnit.SECONDS),
                    is(true)
                );

                Files.createDirectory(subDir);

                Path barrier2 = tempDir.resolve("barrier2.txt");
                Files.createFile(barrier2);
                Files.deleteIfExists(barrier2);

                assertThat(
                    "Barrier 2 DELETE confirms directory recreation processed",
                    barrier2Latch.await(5, TimeUnit.SECONDS),
                    is(true)
                );

                Path fileB = subDir.resolve("b");
                Files.writeString(fileB, "content", StandardCharsets.UTF_8);
                Files.deleteIfExists(fileB);

                assertThat(
                    "File 'b' DELETE must emit execution (not suppressed by stale directory path)",
                    fileBLatch.await(5, TimeUnit.SECONDS),
                    is(true)
                );

                assertThat("Watcher must not fail", watcherError.get(), nullValue());

                // Verify the file 'b' execution specifically
                Execution fileBExecution = receivedExecutions.stream()
                    .filter(exec -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> fileVars = (Map<String, Object>)
                            exec.getTrigger().getVariables().get("file");
                        return "b".equals(fileVars.get("name"));
                    })
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("File 'b' DELETE execution    not found"));

                @SuppressWarnings("unchecked")
                Map<String, Object> fileVariables = (Map<String, Object>)
                    fileBExecution.getTrigger().getVariables().get("file");

                assertThat(fileVariables.get("name"), equalTo("b"));
                assertThat(
                    fileVariables.get("localPath").toString(),
                    equalTo(fileB.toAbsolutePath().normalize().toUri().toString())
                );

            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
            deleteRecursively(outsideDir);
        }
    }


    @Test
    @Timeout(20)
    void testPopulatedDirectoryMovedIntoWatchedRoot() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path outsideDir = Files.createTempDirectory("realtime-trigger-outside");
        Path populatedDir = outsideDir.resolve("populated");
        try {
            // Create a populated directory outside the watched root
            Files.createDirectories(populatedDir);
            Path file1 = populatedDir.resolve("file1.txt");
            Path file2 = populatedDir.resolve("file2.txt");
            Path subdir = populatedDir.resolve("subdir");
            Files.createDirectories(subdir);
            Path file3 = subdir.resolve("file3.txt");

            Files.writeString(file1, "content1", StandardCharsets.UTF_8);
            Files.writeString(file2, "content2", StandardCharsets.UTF_8);
            Files.writeString(file3, "content3", StandardCharsets.UTF_8);

            Path externalTarget = outsideDir.resolve("external-target.txt");
            Files.writeString(externalTarget, "must not be emitted through symlink");

            try {
                Files.createSymbolicLink(
                    populatedDir.resolve("external-link.txt"),
                    externalTarget
                );
            } catch (UnsupportedOperationException | IOException | SecurityException e) {
                Assumptions.assumeTrue(
                    false,
                    "Symbolic links are unavailable in this environment: " + e.getMessage()
                );
            }

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-populated-dir-move")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .recursive(Property.ofValue(true))
                .build();
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            List<Execution> receivedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            CountDownLatch eventLatch = new CountDownLatch(3); // Expect 3 file CREATE events
            CountDownLatch probeObserved = new CountDownLatch(1);

            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                            return;
                        }
                        receivedExecutions.add(execution);
                        eventLatch.countDown();
                    },
                    watcherError::set
                );
            try {
                RealtimeTriggerTestSupport.awaitWatcherReady(tempDir, RealtimeTrigger.EventType.CREATE, ".realtime-trigger-probe-populated.txt", probeObserved);

                // Move the populated directory into the watched root
                Path movedDir = tempDir.resolve("populated");
                Files.move(populatedDir, movedDir);

                // Wait for all file events
                assertThat(
                    "Expected 3 CREATE events for files in populated directory",
                    eventLatch.await(10, TimeUnit.SECONDS),
                    is(true)
                );

                assertThat("Watcher must not fail", watcherError.get(), nullValue());
                assertThat("Should have received 3 executions", receivedExecutions.size(), is(3));

                // Verify all files were discovered
                Set<String> fileNames = receivedExecutions.stream()
                    .map(execution -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> fileVars = (Map<String, Object>) execution.getTrigger().getVariables().get("file");
                        return (String) fileVars.get("name");
                    })
                    .collect(Collectors.toSet());

                assertThat(
                    fileNames,
                    is(Set.of("file1.txt", "file2.txt", "file3.txt"))
                );

                // Verify all are CREATE events
                for (Execution execution : receivedExecutions) {
                    assertThat(execution.getTrigger().getVariables().get("changeType"), equalTo("CREATE"));
                }
            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
            deleteRecursively(outsideDir);
        }
    }

    @Test
    @Timeout(20)
    void testRecreatedFileIsNotSuppressedByStaleScanMarker() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path outsideDir = Files.createTempDirectory("realtime-trigger-outside");
        Path populatedDir = outsideDir.resolve("populated");
        Path originalFile = populatedDir.resolve("existing.txt");

        try {
            Files.createDirectories(populatedDir);
            Files.writeString(originalFile, "initial content", StandardCharsets.UTF_8);

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-recreated-file-after-scan")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .recursive(Property.ofValue(true))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            AtomicInteger targetCreateCount = new AtomicInteger();
            CountDownLatch firstTargetCreate = new CountDownLatch(1);
            CountDownLatch secondTargetCreate = new CountDownLatch(1);
            CountDownLatch barrierCreate = new CountDownLatch(1);
            CountDownLatch probeObserved = new CountDownLatch(1);

            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                            return;
                        }

                        @SuppressWarnings("unchecked")
                        Map<String, Object> fileVars = (Map<String, Object>)
                            execution.getTrigger().getVariables().get("file");
                        String fileName = (String) fileVars.get("name");

                        if ("existing.txt".equals(fileName)) {
                            int count = targetCreateCount.incrementAndGet();
                            if (count == 1) {
                                firstTargetCreate.countDown();
                            } else if (count == 2) {
                                secondTargetCreate.countDown();
                            }
                        } else if ("barrier.txt".equals(fileName)) {
                            barrierCreate.countDown();
                        }
                    },
                    watcherError::set
                );

            try {
                RealtimeTriggerTestSupport.awaitWatcherReady(
                    tempDir,
                    RealtimeTrigger.EventType.CREATE,
                    ".realtime-trigger-probe-recreated-file.txt",
                    probeObserved
                );

                Path movedDir = tempDir.resolve("populated");
                Files.move(populatedDir, movedDir);
                Path watchedFile = movedDir.resolve("existing.txt");

                assertThat(
                    "Expected CREATE for the file discovered during directory scanning",
                    firstTargetCreate.await(10, TimeUnit.SECONDS),
                    is(true)
                );

                Files.delete(watchedFile);
                Files.createFile(movedDir.resolve("barrier.txt"));

                assertThat(
                    "Expected the barrier CREATE after the file deletion",
                    barrierCreate.await(10, TimeUnit.SECONDS),
                    is(true)
                );

                Files.writeString(watchedFile, "recreated content", StandardCharsets.UTF_8);

                assertThat(
                    "Recreating the same path must produce another CREATE event",
                    secondTargetCreate.await(10, TimeUnit.SECONDS),
                    is(true)
                );

                assertThat("Watcher must not fail", watcherError.get(), nullValue());
                assertThat("Expected exactly two CREATE events for existing.txt",
                    targetCreateCount.get(), is(2));
            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
            deleteRecursively(outsideDir);
        }
    }

    @Test
    @Timeout(20)
    void testPopulatedDirectoryNoDuplicateEvents() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path outsideDir = Files.createTempDirectory("realtime-trigger-outside");
        Path populatedDir = outsideDir.resolve("populated");
        try {
            // Create a directory with a file
            Files.createDirectories(populatedDir);
            Path existingFile = populatedDir.resolve("existing.txt");
            Files.writeString(existingFile, "content", StandardCharsets.UTF_8);

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-no-duplicates")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .recursive(Property.ofValue(true))
                .build();
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            List<Execution> receivedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            CountDownLatch eventLatch = new CountDownLatch(1);
            CountDownLatch probeObserved = new CountDownLatch(1);
            AtomicInteger eventCount = new AtomicInteger(0);

            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                            return;
                        }
                        @SuppressWarnings("unchecked")
                        Map<String, Object> fileVars = (Map<String, Object>) execution.getTrigger().getVariables().get("file");
                        String fileName = (String) fileVars.get("name");
                        if ("existing.txt".equals(fileName)) {
                            eventCount.incrementAndGet();
                            receivedExecutions.add(execution);
                            eventLatch.countDown();
                        }
                    },
                    watcherError::set
                );
            try {
                RealtimeTriggerTestSupport.awaitWatcherReady(tempDir, RealtimeTrigger.EventType.CREATE, ".realtime-trigger-probe-nodup.txt", probeObserved);

                // Move the populated directory into the watched root
                Path movedDir = tempDir.resolve("populated");
                Files.move(populatedDir, movedDir);

                // Wait for the file event
                assertThat(
                    "Expected 1 CREATE event for existing.txt",
                    eventLatch.await(10, TimeUnit.SECONDS),
                    is(true)
                );

                // Wait a bit longer to ensure no duplicate events arrive
                Thread.sleep(2000);

                assertThat("Watcher must not fail", watcherError.get(), nullValue());
                assertThat("Should receive exactly 1 event (no duplicates)", eventCount.get(), is(1));
                assertThat("Should have exactly 1 execution", receivedExecutions.size(), is(1));
            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
            deleteRecursively(outsideDir);
        }
    }

    @Test
    @Timeout(20)
    void testPopulatedDirectoryWithRegexFilter() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path outsideDir = Files.createTempDirectory("realtime-trigger-outside");
        Path populatedDir = outsideDir.resolve("populated");
        try {
            // Create directory with various files
            Files.createDirectories(populatedDir);
            Files.writeString(populatedDir.resolve("data.csv"), "csv content", StandardCharsets.UTF_8);
            Files.writeString(populatedDir.resolve("readme.txt"), "txt content", StandardCharsets.UTF_8);
            Files.writeString(populatedDir.resolve("config.csv"), "csv config", StandardCharsets.UTF_8);

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-scan-regex-filter")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .recursive(Property.ofValue(true))
                .regExp(Property.ofValue(".*\\.csv$"))
                .build();
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            List<Execution> receivedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            CountDownLatch eventLatch = new CountDownLatch(2); // Only CSV files
            CountDownLatch probeObserved = new CountDownLatch(1);

            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                            return;
                        }
                        receivedExecutions.add(execution);
                        eventLatch.countDown();
                    },
                    watcherError::set
                );
            try {
                RealtimeTriggerTestSupport.awaitWatcherReady(tempDir, RealtimeTrigger.EventType.CREATE, ".realtime-trigger-probe-regex.csv", probeObserved);

                // Move populated directory
                Files.move(populatedDir, tempDir.resolve("populated"));

                assertThat(
                    "Expected 2 CREATE events for CSV files only",
                    eventLatch.await(10, TimeUnit.SECONDS),
                    is(true)
                );

                assertThat("Watcher must not fail", watcherError.get(), nullValue());
                assertThat("Should have 2 CSV file events", receivedExecutions.size(), is(2));

                Set<String> fileNames = receivedExecutions.stream()
                    .map(execution -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> fileVars = (Map<String, Object>) execution.getTrigger().getVariables().get("file");
                        return (String) fileVars.get("name");
                    })
                    .collect(Collectors.toSet());

                assertThat(fileNames, hasItems("data.csv", "config.csv"));
                assertThat(fileNames, not(hasItem("readme.txt")));
            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
            deleteRecursively(outsideDir);
        }
    }

    @Test
    @Timeout(20)
    void testNonRecursiveDoesNotScanNestedFiles() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        try {
            // In non-recursive mode, files in subdirectories should not trigger events
            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-non-recursive-no-scan")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .recursive(Property.ofValue(false))
                .build();
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            List<Execution> receivedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            CountDownLatch rootFileLatch = new CountDownLatch(1);
            CountDownLatch probeObserved = new CountDownLatch(1);

            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                            return;
                        }
                        receivedExecutions.add(execution);
                        @SuppressWarnings("unchecked")
                        Map<String, Object> fileVars = (Map<String, Object>) execution.getTrigger().getVariables().get("file");
                        String fileName = (String) fileVars.get("name");
                        if ("root.txt".equals(fileName)) {
                            rootFileLatch.countDown();
                        }
                    },
                    watcherError::set
                );
            try {
                RealtimeTriggerTestSupport.awaitWatcherReady(tempDir, RealtimeTrigger.EventType.CREATE, ".realtime-trigger-probe-nonrec.txt", probeObserved);

                // Create a file at root level - should trigger event
                Files.writeString(tempDir.resolve("root.txt"), "root", StandardCharsets.UTF_8);

                assertThat(
                    "Expected event for root-level file",
                    rootFileLatch.await(5, TimeUnit.SECONDS),
                    is(true)
                );

                // Create a subdirectory with a file - should NOT trigger event for the file
                Path subdir = tempDir.resolve("subdir");
                Files.createDirectories(subdir);
                Files.writeString(subdir.resolve("nested.txt"), "nested", StandardCharsets.UTF_8);

                // Wait to ensure no nested file events arrive
                Thread.sleep(2000);

                assertThat("Watcher must not fail", watcherError.get(), nullValue());
                assertThat("Should have only 1 execution (root file only)", receivedExecutions.size(), is(1));

                @SuppressWarnings("unchecked")
                Map<String, Object> fileVars = (Map<String, Object>) receivedExecutions.get(0).getTrigger().getVariables().get("file");
                assertThat(fileVars.get("name"), equalTo("root.txt"));
            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    @Timeout(20)
    void testNoStartupScanForPreExistingFiles() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        try {
            // Create files BEFORE starting the trigger
            Path file1 = tempDir.resolve("preexisting1.txt");
            Path file2 = tempDir.resolve("preexisting2.txt");
            Files.writeString(file1, "content1", StandardCharsets.UTF_8);
            Files.writeString(file2, "content2", StandardCharsets.UTF_8);

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-no-startup-scan")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE))
                .build();
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            List<Execution> receivedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            CountDownLatch probeObserved = new CountDownLatch(1);

            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                            return;
                        }
                        receivedExecutions.add(execution);
                    },
                    watcherError::set
                );
            try {
                RealtimeTriggerTestSupport.awaitWatcherReady(tempDir, RealtimeTrigger.EventType.CREATE, ".realtime-trigger-probe-nostartup.txt", probeObserved);

                // Wait a bit to ensure no startup scan events
                Thread.sleep(2000);

                assertThat("Watcher must not fail", watcherError.get(), nullValue());
                assertThat("Should have 0 executions (no startup scan)", receivedExecutions.size(), is(0));

                // Now create a new file - should get an event for this
                Path newFile = tempDir.resolve("created-after-start.txt");
                Files.writeString(newFile, "new content", StandardCharsets.UTF_8);

                // Give time for the event
                Thread.sleep(1000);

                assertThat("Should have 1 execution for newly created file", receivedExecutions.size(), is(1));
                @SuppressWarnings("unchecked")
                Map<String, Object> fileVars = (Map<String, Object>) receivedExecutions.get(0).getTrigger().getVariables().get("file");
                assertThat(fileVars.get("name"), equalTo("created-after-start.txt"));
            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    @Timeout(20)
    void testNormalEventsAfterDynamicRegistration() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-test");
        Path outsideDir = Files.createTempDirectory("realtime-trigger-outside");
        Path populatedDir = outsideDir.resolve("populated");
        try {
            Files.createDirectories(populatedDir);
            Files.writeString(populatedDir.resolve("existing.txt"), "content", StandardCharsets.UTF_8);

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-events-after-registration")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.CREATE_OR_UPDATE))
                .recursive(Property.ofValue(true))
                .build();
            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            List<Execution> receivedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            CountDownLatch scanEventLatch = new CountDownLatch(1);
            CountDownLatch normalEventLatch = new CountDownLatch(1);
            CountDownLatch probeObserved = new CountDownLatch(1);

            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                            return;
                        }
                        receivedExecutions.add(execution);
                        @SuppressWarnings("unchecked")
                        Map<String, Object> fileVars = (Map<String, Object>) execution.getTrigger().getVariables().get("file");
                        String fileName = (String) fileVars.get("name");
                        if ("existing.txt".equals(fileName)) {
                            scanEventLatch.countDown();
                        } else if ("newfile.txt".equals(fileName)) {
                            normalEventLatch.countDown();
                        }
                    },
                    watcherError::set
                );
            try {
                RealtimeTriggerTestSupport.awaitWatcherReady(tempDir, RealtimeTrigger.EventType.CREATE_OR_UPDATE, ".realtime-trigger-probe-normal.txt", probeObserved);

                // Move populated directory - triggers scan
                Path movedDir = tempDir.resolve("populated");
                Files.move(populatedDir, movedDir);

                assertThat("Expected scan event", scanEventLatch.await(10, TimeUnit.SECONDS), is(true));

                // Now create a new file in the moved directory - should trigger normal watch event
                Files.writeString(movedDir.resolve("newfile.txt"), "new", StandardCharsets.UTF_8);

                assertThat("Expected normal watch event", normalEventLatch.await(5, TimeUnit.SECONDS), is(true));
                assertThat("Watcher must not fail", watcherError.get(), nullValue());
                assertThat("Should have 2 executions", receivedExecutions.size(), is(2));
            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
            deleteRecursively(outsideDir);
        }
    }
}
