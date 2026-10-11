package io.kestra.plugin.fs.local;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private static void waitForWatcherReady(RealtimeTrigger trigger) throws InterruptedException {
        long deadline = System.nanoTime() + WATCHER_READY_TIMEOUT.toNanos();

        while (!trigger.isReady()) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("Watcher did not become ready within " + WATCHER_READY_TIMEOUT);
            }
            Thread.sleep(25);
        }
    }

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

            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        receivedExecutions.add(execution);
                        eventLatch.countDown();
                    },
                    watcherError::set
                );

            try {
                waitForWatcherReady(trigger);

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

            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        receivedExecutions.add(execution);
                        eventLatch.countDown();
                    },
                    watcherError::set
                );

            try {
                waitForWatcherReady(trigger);

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

            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        receivedExecutions.add(execution);
                        eventLatch.countDown();
                    },
                    watcherError::set
                );

            try {
                waitForWatcherReady(trigger);

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

            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        receivedExecutions.add(execution);
                        eventLatch.countDown();
                    },
                    watcherError::set
                );

            try {
                waitForWatcherReady(trigger);

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

            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .doFinally(signalType -> terminationLatch.countDown())
                .subscribe(
                    execution -> {
                        receivedExecutions.add(execution);
                        eventLatch.countDown();
                    },
                    watcherError::set
                );

            try {
                waitForWatcherReady(trigger);
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

            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .doFinally(signalType -> terminationLatch.countDown())
                .subscribe(
                    execution -> {
                        receivedExecutions.add(execution);
                        eventLatch.countDown();
                    },
                    watcherError::set
                );

            try {
                waitForWatcherReady(trigger);
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

            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        receivedExecutions.add(execution);
                        eventLatch.countDown();
                    },
                    watcherError::set
                );

            try {
                waitForWatcherReady(trigger);

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

            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .doFinally(signalType -> terminationLatch.countDown())
                .subscribe(ignored -> {}, watcherError::set);

            try {
                waitForWatcherReady(trigger);
                assertThat("Watcher should be ready", trigger.isReady(), is(true));
                assertThat("Stream should not have terminated before stop()", terminationLatch.getCount(), is(1L));
                assertThat("No error should have occurred before stop()", watcherError.get(), nullValue());

                trigger.stop();

                assertThat("Watcher should terminate after stop()", terminationLatch.await(3, TimeUnit.SECONDS), is(true));
                assertThat("Watcher should not be ready after stop", trigger.isReady(), is(false));

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

            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
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

            try {
                waitForWatcherReady(trigger);

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

            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
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
                waitForWatcherReady(trigger);
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

            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        receivedExecutions.add(execution);
                        eventLatch.countDown();
                    },
                    watcherError::set
                );

            try {
                waitForWatcherReady(trigger);

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

            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
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

            try {
                waitForWatcherReady(trigger);
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

            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        receivedExecutions.add(execution);
                        eventLatch.countDown();
                    },
                    watcherError::set
                );

            try {
                waitForWatcherReady(trigger);
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

            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .doFinally(signalType -> terminationLatch.countDown())
                .subscribe(ignored -> {}, watcherError::set);

            try {
                waitForWatcherReady(trigger);
                assertThat("Watcher should be ready", trigger.isReady(), is(true));
                assertThat("Stream should not have terminated before kill()", terminationLatch.getCount(), is(1L));
                assertThat("No error should have occurred before kill()", watcherError.get(), nullValue());

                trigger.kill();

                assertThat("Watcher should terminate after kill()", terminationLatch.await(3, TimeUnit.SECONDS), is(true));
                assertThat("Watcher should not be ready after kill", trigger.isReady(), is(false));

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

            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
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
                waitForWatcherReady(trigger);

                // Step 1: Move 'a' directory outside watched root
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

                // Step 2: Create and delete barrier file #1 to prove directory DELETE processed
                Path barrier1 = tempDir.resolve("barrier1.txt");
                Files.createFile(barrier1);
                Files.deleteIfExists(barrier1);

                assertThat(
                    "Barrier 1 DELETE confirms directory move processed",
                    barrier1Latch.await(5, TimeUnit.SECONDS),
                    is(true)
                );

                // Step 3: Recreate 'a' as a directory
                Files.createDirectory(subDir);

                // Step 4: Create and delete barrier file #2 to prove directory CREATE processed
                Path barrier2 = tempDir.resolve("barrier2.txt");
                Files.createFile(barrier2);
                Files.deleteIfExists(barrier2);

                assertThat(
                    "Barrier 2 DELETE confirms directory recreation processed",
                    barrier2Latch.await(5, TimeUnit.SECONDS),
                    is(true)
                );

                // Step 5: Create 'b' as a FILE (not directory), then delete it
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

}
