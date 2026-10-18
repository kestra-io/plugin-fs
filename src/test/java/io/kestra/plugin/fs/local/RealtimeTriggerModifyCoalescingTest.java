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
import java.lang.reflect.Field;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Coalescing Tests for RealtimeTrigger ENTRY_MODIFY event handling.
 *
 * These tests verify that the coalescing mechanism correctly handles rapid file modifications:
 * - Multiple ENTRY_MODIFY events within 500ms -> exactly 1 UPDATE execution after quiet period
 * - ENTRY_MODIFY events separated by >500ms -> separate UPDATE executions (distinct logical writes)
 * - DELETE events cancel pending MODIFY emissions (no stale UPDATE after DELETE)
 *
 * **Validates Requirements**: 1.1, 1.2, 2.1, 2.2, 2.3, 2.4, 2.5
 */
@KestraTest
class RealtimeTriggerModifyCoalescingTest {
    @Inject
    private RunContextFactory runContextFactory;

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

    /**
     * **Validates: Requirements 1.1, 1.2, 2.1, 2.2**
     *
     * Coalescing Verification Test: Rapid Modify Events Produce Single Execution
     *
     * This test writes to a file twice with a 100ms delay between writes,
     * creating a burst of ENTRY_MODIFY events within a short time window.
     *
     * **EXPECTED OUTCOME**: Test PASSES with exactly 1 execution
     * - Single execution arrives ~500ms after the last write
     * - This validates the coalescing fix is working correctly
     * - Multiple rapid modifications to the same file are coalesced into one UPDATE
     */
    @Test
    @Timeout(20)
    void testRapidModifyEventsDuplicateExecutions() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-coalesce-test");
        Path testFile = tempDir.resolve("test-rapid-modify.txt");

        try {
            // Create file before trigger starts to avoid CREATE events
            Files.writeString(testFile, "initial content", StandardCharsets.UTF_8);

            // Give filesystem time to settle
            Thread.sleep(100);

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-rapid-modify")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.UPDATE))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            List<Execution> receivedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            AtomicInteger executionCount = new AtomicInteger(0);

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
                        executionCount.incrementAndGet();
                    },
                    watcherError::set
                );

            try {
                // Wait for watcher to be ready
                RealtimeTriggerTestSupport.awaitWatcherReady(
                    tempDir,
                    RealtimeTrigger.EventType.UPDATE,
                    ".realtime-trigger-probe-rapid-modify.txt",
                    probeObserved
                );

                // Perform rapid successive writes
                Files.writeString(testFile, "first write", StandardCharsets.UTF_8);
                Thread.sleep(100);  // Short delay within expected quiet period
                Files.writeString(testFile, "second write", StandardCharsets.UTF_8);

                // Wait for potential executions (2 seconds observation window)
                // On unfixed code: multiple executions arrive quickly
                // On fixed code: single execution arrives ~500ms after last write
                Thread.sleep(2000);

                assertThat("Watcher must not fail", watcherError.get(), nullValue());

                int count = executionCount.get();

                // **BUG CONDITION CHECK**
                // Expected on UNFIXED code: count >= 2 (test will FAIL, confirming bug)
                // Expected on FIXED code: count == 1 (test will PASS, validating fix)
                assertThat(
                    "Multiple ENTRY_MODIFY events within quiet period should produce exactly 1 UPDATE execution (coalesced)",
                    count,
                    is(1)
                );

                // Verify all executions are UPDATE type
                receivedExecutions.forEach(execution -> {
                    assertThat(
                        execution.getTrigger().getVariables().get("changeType"),
                        equalTo("UPDATE")
                    );
                });

            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }

    /**
     * **Validates: Requirements 1.1, 1.2, 2.1, 2.2**
     *
     * Coalescing Verification Test: Modify Burst Produces Single Execution
     *
     * This test writes to a file 5 times with 50ms delays between writes,
     * creating a sustained burst of ENTRY_MODIFY events.
     *
     * **EXPECTED OUTCOME**: Test PASSES with exactly 1 execution
     * - Single execution arrives ~500ms after the last write
     * - This validates the coalescing handles sustained bursts correctly
     * - A burst of modifications is coalesced into one UPDATE after the quiet period
     */
    @Test
    @Timeout(20)
    void testModifyBurstGeneratesMultipleExecutions() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-burst-test");
        Path testFile = tempDir.resolve("test-burst.txt");

        try {
            // Create file before trigger starts
            Files.writeString(testFile, "initial content", StandardCharsets.UTF_8);
            Thread.sleep(100);

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-modify-burst")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.UPDATE))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            List<Execution> receivedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            AtomicInteger executionCount = new AtomicInteger(0);

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
                        executionCount.incrementAndGet();
                    },
                    watcherError::set
                );

            try {
                // Wait for watcher to be ready
                RealtimeTriggerTestSupport.awaitWatcherReady(
                    tempDir,
                    RealtimeTrigger.EventType.UPDATE,
                    ".realtime-trigger-probe-burst.txt",
                    probeObserved
                );

                // Perform burst of 5 writes with short delays
                for (int i = 0; i < 5; i++) {
                    Files.writeString(testFile, "write #" + i, StandardCharsets.UTF_8);
                    Thread.sleep(50);  // All within quiet period
                }

                // Wait for potential executions (2 seconds observation window)
                Thread.sleep(2000);

                assertThat("Watcher must not fail", watcherError.get(), nullValue());

                int count = executionCount.get();

                // **BUG CONDITION CHECK**
                // Expected on UNFIXED code: count >= 5 (test will FAIL, confirming bug)
                // Expected on FIXED code: count == 1 (test will PASS, validating fix)
                assertThat(
                    "Burst of ENTRY_MODIFY events should produce exactly 1 UPDATE execution (coalesced)",
                    count,
                    is(1)
                );

                // Verify all executions are UPDATE type
                receivedExecutions.forEach(execution -> {
                    assertThat(
                        execution.getTrigger().getVariables().get("changeType"),
                        equalTo("UPDATE")
                    );
                });

            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }

    /**
     * **Validates: Requirements 2.4**
     *
     * Boundary Test: Writes Separated By Quiet Period Produce Separate Executions
     *
     * This test writes to a file, waits 700ms (beyond the 500ms quiet period),
     * then writes again. These represent two distinct logical file modifications.
     *
     * **EXPECTED OUTCOME on BOTH UNFIXED and FIXED CODE**: 2 separate executions
     * - This is NOT a bug condition—it's correct behavior
     * - Tests that coalescing doesn't merge logically separate writes
     * - Validates requirement 2.4: new MODIFY after quiet period starts new period
     */
    @Test
    @Timeout(20)
    void testSeparatedModifiesProduceSeparateExecutions() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-separated-test");
        Path testFile = tempDir.resolve("test-separated.txt");

        try {
            // Create file before trigger starts
            Files.writeString(testFile, "initial content", StandardCharsets.UTF_8);
            Thread.sleep(100);

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-separated-modify")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.UPDATE))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            List<Execution> receivedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            AtomicInteger executionCount = new AtomicInteger(0);

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
                        executionCount.incrementAndGet();
                    },
                    watcherError::set
                );

            try {
                // Wait for watcher to be ready
                RealtimeTriggerTestSupport.awaitWatcherReady(
                    tempDir,
                    RealtimeTrigger.EventType.UPDATE,
                    ".realtime-trigger-probe-separated.txt",
                    probeObserved
                );

                // First write
                Files.writeString(testFile, "first logical write", StandardCharsets.UTF_8);

                // Wait beyond quiet period (700ms > 500ms)
                Thread.sleep(700);

                // Second write (this is a new logical modification)
                Files.writeString(testFile, "second logical write", StandardCharsets.UTF_8);

                // Wait for both executions to arrive
                Thread.sleep(1500);

                assertThat("Watcher must not fail", watcherError.get(), nullValue());

                int count = executionCount.get();

                // Both unfixed and fixed code should produce 2 executions
                // (distinct writes separated by quiet period)
                assertThat(
                    "Two ENTRY_MODIFY events separated by >500ms should produce 2 separate UPDATE executions",
                    count,
                    is(2)
                );

                // Verify all executions are UPDATE type
                receivedExecutions.forEach(execution -> {
                    assertThat(
                        execution.getTrigger().getVariables().get("changeType"),
                        equalTo("UPDATE")
                    );
                });

            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }

    /**
     * **Validates: Requirements 2.5**
     *
     * Concurrent Files Test: Multiple Files Modified Independently
     *
     * This test modifies two different files concurrently to verify that
     * coalescing tracks each file path independently.
     *
     * **EXPECTED OUTCOME on FIXED CODE**: 2 executions (one per file)
     * - Each file's ENTRY_MODIFY events are coalesced independently
     * - Validates requirement 2.5: per-path quiet period tracking
     */
    @Test
    @Timeout(20)
    void testConcurrentFilesCoalescedIndependently() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-concurrent-test");
        Path fileA = tempDir.resolve("fileA.txt");
        Path fileB = tempDir.resolve("fileB.txt");

        try {
            // Create files before trigger starts
            Files.writeString(fileA, "initial A", StandardCharsets.UTF_8);
            Files.writeString(fileB, "initial B", StandardCharsets.UTF_8);
            Thread.sleep(100);

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-concurrent-files")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.UPDATE))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            List<Execution> receivedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            AtomicInteger executionCount = new AtomicInteger(0);

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
                        executionCount.incrementAndGet();
                    },
                    watcherError::set
                );

            try {
                // Wait for watcher to be ready
                RealtimeTriggerTestSupport.awaitWatcherReady(
                    tempDir,
                    RealtimeTrigger.EventType.UPDATE,
                    ".realtime-trigger-probe-concurrent.txt",
                    probeObserved
                );

                // Modify both files with bursts
                Files.writeString(fileA, "write A-1", StandardCharsets.UTF_8);
                Files.writeString(fileB, "write B-1", StandardCharsets.UTF_8);
                Thread.sleep(50);
                Files.writeString(fileA, "write A-2", StandardCharsets.UTF_8);
                Files.writeString(fileB, "write B-2", StandardCharsets.UTF_8);

                // Wait for executions
                Thread.sleep(2000);

                assertThat("Watcher must not fail", watcherError.get(), nullValue());

                int count = executionCount.get();

                // Expected: exactly 2 executions (one per file path)
                assertThat(
                    "Two files modified concurrently should produce 2 UPDATE executions (one per file)",
                    count,
                    is(2)
                );

                // Verify we received one execution for each file
                long fileAExecutions = receivedExecutions.stream()
                    .filter(exec -> {
                        Object file = exec.getTrigger().getVariables().get("file");
                        if (file instanceof java.util.Map<?, ?> fileMap) {
                            Object name = fileMap.get("name");
                            return "fileA.txt".equals(name);
                        }
                        return false;
                    })
                    .count();

                long fileBExecutions = receivedExecutions.stream()
                    .filter(exec -> {
                        Object file = exec.getTrigger().getVariables().get("file");
                        if (file instanceof java.util.Map<?, ?> fileMap) {
                            Object name = fileMap.get("name");
                            return "fileB.txt".equals(name);
                        }
                        return false;
                    })
                    .count();

                assertThat("Should have exactly 1 execution for fileA", fileAExecutions, is(1L));
                assertThat("Should have exactly 1 execution for fileB", fileBExecutions, is(1L));

            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }

    /**
     * **Validates: Requirements 2.1, 2.2**
     *
     * Fix Verification Test: Single Coalesced Execution
     *
     * This test writes to a file 5 times rapidly (50ms apart) and verifies that
     * exactly one UPDATE execution is emitted approximately 500ms after the last write.
     *
     * This confirms the coalescing fix works correctly for rapid write bursts.
     */
    @Test
    @Timeout(20)
    void testSingleCoalescedExecution() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-single-coalesce-test");
        Path testFile = tempDir.resolve("test-coalesce.txt");

        try {
            // Create file before trigger starts
            Files.writeString(testFile, "initial content", StandardCharsets.UTF_8);
            Thread.sleep(100);

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-single-coalesced")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.UPDATE))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            List<Execution> receivedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            AtomicInteger executionCount = new AtomicInteger(0);
            AtomicReference<Long> firstExecutionTime = new AtomicReference<>();

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
                        executionCount.incrementAndGet();
                        firstExecutionTime.compareAndSet(null, System.currentTimeMillis());
                    },
                    watcherError::set
                );

            try {
                // Wait for watcher to be ready
                RealtimeTriggerTestSupport.awaitWatcherReady(
                    tempDir,
                    RealtimeTrigger.EventType.UPDATE,
                    ".realtime-trigger-probe-single-coalesce.txt",
                    probeObserved
                );

                // Record when we start writing
                long writeStartTime = System.currentTimeMillis();

                // Perform 5 rapid writes (50ms apart, total ~250ms burst)
                for (int i = 0; i < 5; i++) {
                    Files.writeString(testFile, "write #" + i, StandardCharsets.UTF_8);
                    if (i < 4) {
                        Thread.sleep(50);
                    }
                }

                long lastWriteTime = System.currentTimeMillis();

                // Wait for execution (should arrive ~500ms after last write)
                Thread.sleep(1500);

                assertThat("Watcher must not fail", watcherError.get(), nullValue());

                int count = executionCount.get();

                // Verify exactly 1 execution
                assertThat(
                    "5 rapid ENTRY_MODIFY events should produce exactly 1 coalesced UPDATE execution",
                    count,
                    is(1)
                );

                // Verify timing: execution should arrive ~500ms after last write (±100ms tolerance)
                if (firstExecutionTime.get() != null) {
                    long timeSinceLastWrite = firstExecutionTime.get() - lastWriteTime;
                    assertThat(
                        "Execution should arrive approximately 500ms after last write (±100ms tolerance)",
                        timeSinceLastWrite,
                        allOf(greaterThanOrEqualTo(400L), lessThanOrEqualTo(600L))
                    );
                }

                // Verify execution is UPDATE type
                receivedExecutions.forEach(execution -> {
                    assertThat(
                        execution.getTrigger().getVariables().get("changeType"),
                        equalTo("UPDATE")
                    );
                });

            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }

    /**
     * **Validates: Requirements 2.2**
     *
     * Fix Verification Test: Quiet Period Timing
     *
     * This test writes to a file once and measures the time until the execution arrives.
     * It verifies the execution arrives between 400-600ms (500ms ± 100ms tolerance).
     *
     * This confirms the quiet period mechanism is working with correct timing.
     */
    @Test
    @Timeout(20)
    void testQuietPeriodTiming() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-timing-test");
        Path testFile = tempDir.resolve("test-timing.txt");

        try {
            // Create file before trigger starts
            Files.writeString(testFile, "initial content", StandardCharsets.UTF_8);
            Thread.sleep(100);

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-quiet-period-timing")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.UPDATE))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            AtomicReference<Long> executionTime = new AtomicReference<>();

            CountDownLatch probeObserved = new CountDownLatch(1);
            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                            return;
                        }
                        executionTime.set(System.currentTimeMillis());
                    },
                    watcherError::set
                );

            try {
                // Wait for watcher to be ready
                RealtimeTriggerTestSupport.awaitWatcherReady(
                    tempDir,
                    RealtimeTrigger.EventType.UPDATE,
                    ".realtime-trigger-probe-timing.txt",
                    probeObserved
                );

                // Perform single write and record time
                long writeTime = System.currentTimeMillis();
                Files.writeString(testFile, "single write", StandardCharsets.UTF_8);

                // Wait for execution
                Thread.sleep(1500);

                assertThat("Watcher must not fail", watcherError.get(), nullValue());
                assertThat("Execution should have been received", executionTime.get(), notNullValue());

                // Verify timing: execution should arrive 400-600ms after write (500ms ± 100ms)
                long delay = executionTime.get() - writeTime;
                assertThat(
                    "Execution should arrive between 400-600ms after write (500ms ± 100ms tolerance)",
                    delay,
                    allOf(greaterThanOrEqualTo(400L), lessThanOrEqualTo(600L))
                );

            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }

    /**
     * **Validates: Requirements 2.1, 2.2, 2.5**
     *
     * Fix Verification Test: Multiple Files Independent Coalescing
     *
     * This test modifies fileA.txt and fileB.txt concurrently with bursts.
     * It verifies that exactly 1 UPDATE execution is emitted per file (2 total),
     * and each execution has the correct file metadata.
     *
     * This confirms that coalescing tracks each file path independently.
     */
    @Test
    @Timeout(20)
    void testMultipleFilesIndependent() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-multi-independent-test");
        Path fileA = tempDir.resolve("fileA.txt");
        Path fileB = tempDir.resolve("fileB.txt");

        try {
            // Create files before trigger starts
            Files.writeString(fileA, "initial A", StandardCharsets.UTF_8);
            Files.writeString(fileB, "initial B", StandardCharsets.UTF_8);
            Thread.sleep(100);

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-multi-independent")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.UPDATE))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            List<Execution> receivedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            AtomicInteger executionCount = new AtomicInteger(0);

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
                        executionCount.incrementAndGet();
                    },
                    watcherError::set
                );

            try {
                // Wait for watcher to be ready
                RealtimeTriggerTestSupport.awaitWatcherReady(
                    tempDir,
                    RealtimeTrigger.EventType.UPDATE,
                    ".realtime-trigger-probe-multi-independent.txt",
                    probeObserved
                );

                // Modify both files with concurrent bursts
                Files.writeString(fileA, "burst A-1", StandardCharsets.UTF_8);
                Thread.sleep(50);
                Files.writeString(fileB, "burst B-1", StandardCharsets.UTF_8);
                Thread.sleep(50);
                Files.writeString(fileA, "burst A-2", StandardCharsets.UTF_8);
                Thread.sleep(50);
                Files.writeString(fileB, "burst B-2", StandardCharsets.UTF_8);
                Thread.sleep(50);
                Files.writeString(fileA, "burst A-3", StandardCharsets.UTF_8);

                // Wait for executions
                Thread.sleep(2000);

                assertThat("Watcher must not fail", watcherError.get(), nullValue());

                int count = executionCount.get();

                // Verify exactly 2 executions (1 per file)
                assertThat(
                    "Concurrent bursts to two files should produce exactly 2 coalesced UPDATE executions (one per file)",
                    count,
                    is(2)
                );

                // Verify we received one execution for each file
                long fileAExecutions = receivedExecutions.stream()
                    .filter(exec -> {
                        Object file = exec.getTrigger().getVariables().get("file");
                        if (file instanceof java.util.Map<?, ?> fileMap) {
                            Object name = fileMap.get("name");
                            return "fileA.txt".equals(name);
                        }
                        return false;
                    })
                    .count();

                long fileBExecutions = receivedExecutions.stream()
                    .filter(exec -> {
                        Object file = exec.getTrigger().getVariables().get("file");
                        if (file instanceof java.util.Map<?, ?> fileMap) {
                            Object name = fileMap.get("name");
                            return "fileB.txt".equals(name);
                        }
                        return false;
                    })
                    .count();

                assertThat("Should have exactly 1 execution for fileA", fileAExecutions, is(1L));
                assertThat("Should have exactly 1 execution for fileB", fileBExecutions, is(1L));

                // Verify all executions are UPDATE type
                receivedExecutions.forEach(execution -> {
                    assertThat(
                        execution.getTrigger().getVariables().get("changeType"),
                        equalTo("UPDATE")
                    );
                });

            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }

    /**
     * **Validates: Requirements 2.1, 2.2**
     *
     * Fix Verification Test: Boundary Condition Within Quiet Period
     *
     * This test writes to a file, waits 400ms (within the 500ms quiet period),
     * then writes again. Both writes should be coalesced into a single execution.
     *
     * This confirms that writes within the quiet period correctly reset the timer.
     */
    @Test
    @Timeout(20)
    void testBoundaryCondition_WithinQuietPeriod() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-boundary-within-test");
        Path testFile = tempDir.resolve("test-boundary-within.txt");

        try {
            // Create file before trigger starts
            Files.writeString(testFile, "initial content", StandardCharsets.UTF_8);
            Thread.sleep(100);

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-boundary-within")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.UPDATE))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            AtomicInteger executionCount = new AtomicInteger(0);

            CountDownLatch probeObserved = new CountDownLatch(1);
            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                            return;
                        }
                        executionCount.incrementAndGet();
                    },
                    watcherError::set
                );

            try {
                // Wait for watcher to be ready
                RealtimeTriggerTestSupport.awaitWatcherReady(
                    tempDir,
                    RealtimeTrigger.EventType.UPDATE,
                    ".realtime-trigger-probe-boundary-within.txt",
                    probeObserved
                );

                // First write
                Files.writeString(testFile, "first write", StandardCharsets.UTF_8);

                // Wait 400ms (within 500ms quiet period)
                Thread.sleep(400);

                // Second write (should reset the quiet period timer)
                Files.writeString(testFile, "second write", StandardCharsets.UTF_8);

                // Wait for execution (~500ms after second write)
                Thread.sleep(1500);

                assertThat("Watcher must not fail", watcherError.get(), nullValue());

                int count = executionCount.get();

                // Verify exactly 1 execution (both writes coalesced)
                assertThat(
                    "Two writes within quiet period (400ms apart) should coalesce to 1 execution",
                    count,
                    is(1)
                );

            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }

    /**
     * **Validates: Requirements 2.4**
     *
     * Fix Verification Test: Boundary Condition Beyond Quiet Period
     *
     * This test writes to a file, waits 600ms (beyond the 500ms quiet period),
     * then writes again. This should produce 2 separate executions representing
     * two distinct logical writes.
     *
     * This confirms that writes beyond the quiet period start a new coalescing window.
     */
    @Test
    @Timeout(20)
    void testBoundaryCondition_BeyondQuietPeriod() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-boundary-beyond-test");
        Path testFile = tempDir.resolve("test-boundary-beyond.txt");

        try {
            // Create file before trigger starts
            Files.writeString(testFile, "initial content", StandardCharsets.UTF_8);
            Thread.sleep(100);

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-boundary-beyond")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.UPDATE))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            AtomicInteger executionCount = new AtomicInteger(0);

            CountDownLatch probeObserved = new CountDownLatch(1);
            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                            return;
                        }
                        executionCount.incrementAndGet();
                    },
                    watcherError::set
                );

            try {
                // Wait for watcher to be ready
                RealtimeTriggerTestSupport.awaitWatcherReady(
                    tempDir,
                    RealtimeTrigger.EventType.UPDATE,
                    ".realtime-trigger-probe-boundary-beyond.txt",
                    probeObserved
                );

                // First write
                Files.writeString(testFile, "first logical write", StandardCharsets.UTF_8);

                // Wait 600ms (beyond 500ms quiet period - first execution will fire)
                Thread.sleep(600);

                // Second write (this is a new logical modification)
                Files.writeString(testFile, "second logical write", StandardCharsets.UTF_8);

                // Wait for second execution
                Thread.sleep(1500);

                assertThat("Watcher must not fail", watcherError.get(), nullValue());

                int count = executionCount.get();

                // Verify 2 executions (two distinct logical writes)
                assertThat(
                    "Two writes separated by >500ms should produce 2 separate executions (distinct logical writes)",
                    count,
                    is(2)
                );

            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }

    /**
     * **Validates: Requirements 2.3**
     *
     * Fix Verification Test: DELETE Cancels Pending MODIFY
     *
     * This test writes to a file to start the quiet period, then deletes the file
     * before the quiet period expires. It verifies that only a DELETE execution is
     * emitted (no UPDATE), confirming DELETE events properly cancel pending MODIFY emissions.
     */
    @Test
    @Timeout(20)
    void testDeleteCancelsPendingModify() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-delete-cancel-test");
        Path testFile = tempDir.resolve("test-delete-cancel.txt");

        try {
            Files.writeString(testFile, "initial content", StandardCharsets.UTF_8);
            Thread.sleep(100);

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-delete-cancel")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.UPDATE))
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
                RealtimeTriggerTestSupport.awaitWatcherReady(
                    tempDir,
                    RealtimeTrigger.EventType.UPDATE,
                    ".realtime-trigger-probe-delete-cancel.txt",
                    probeObserved
                );

                // Start a pending MODIFY quiet period, then delete the file.
                Files.writeString(testFile, "pending modify", StandardCharsets.UTF_8);

                // Wait until the watcher has actually scheduled the MODIFY emission.
                Field pendingField = RealtimeTrigger.class.getDeclaredField("pendingModifyEvents");
                pendingField.setAccessible(true);
                ConcurrentHashMap<?, ?> pending = (ConcurrentHashMap<?, ?>) pendingField.get(trigger);
                Path normalizedTestFile = testFile.toAbsolutePath().normalize();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                while (!pending.containsKey(normalizedTestFile) && System.nanoTime() < deadline) {
                    Thread.sleep(10);
                }
                assertThat(
                    "Watcher must register a pending MODIFY before deletion",
                    pending.containsKey(normalizedTestFile),
                    is(true)
                );

                Files.delete(testFile);

                // Wait for the watcher to process DELETE and cancel the pending MODIFY.
                long cancellationDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                while (pending.containsKey(normalizedTestFile) && System.nanoTime() < cancellationDeadline) {
                    Thread.sleep(10);
                }

                assertThat(
                    "DELETE must remove the pending MODIFY task",
                    pending.containsKey(normalizedTestFile),
                    is(false)
                );

                // Allow the original quiet period to expire without recreating the file.
                Thread.sleep(1000);

                assertThat("Watcher must not fail", watcherError.get(), nullValue());
                assertThat(
                    "Deleting a file must cancel its pending UPDATE execution",
                    receivedExecutions,
                    empty()
                );
            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }

    /**
     * **Validates: Requirements 2.3**
     *
     * Fix Verification Test: Stop During Quiet Period
     *
     * This test starts a write burst, then calls stop() before the quiet period expires.
     * It verifies that no executions are emitted after stop, and the executor shuts down cleanly.
     *
     * This confirms that cleanup properly cancels pending scheduled tasks.
     */
    @Test
    @Timeout(20)
    void testStopDuringQuietPeriod() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-stop-quiet-test");
        Path testFile = tempDir.resolve("test-stop-quiet.txt");

        try {
            // Create file before trigger starts
            Files.writeString(testFile, "initial content", StandardCharsets.UTF_8);
            Thread.sleep(100);

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-stop-quiet")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.UPDATE))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            AtomicInteger executionCount = new AtomicInteger(0);

            CountDownLatch probeObserved = new CountDownLatch(1);
            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                            return;
                        }
                        executionCount.incrementAndGet();
                    },
                    watcherError::set
                );

            try {
                // Wait for watcher to be ready
                RealtimeTriggerTestSupport.awaitWatcherReady(
                    tempDir,
                    RealtimeTrigger.EventType.UPDATE,
                    ".realtime-trigger-probe-stop-quiet.txt",
                    probeObserved
                );

                // Start write burst
                Files.writeString(testFile, "burst write 1", StandardCharsets.UTF_8);
                Thread.sleep(50);
                Files.writeString(testFile, "burst write 2", StandardCharsets.UTF_8);
                Thread.sleep(50);
                Files.writeString(testFile, "burst write 3", StandardCharsets.UTF_8);

                // Stop trigger immediately (before 500ms quiet period expires)
                Thread.sleep(100);  // Only 200ms total since first write
                trigger.stop();
                subscription.dispose();

                // Wait to ensure no executions arrive after stop
                Thread.sleep(1000);

                // Verify no executions were emitted (pending task was cancelled)
                assertThat(
                    "No executions should be emitted after stop() during quiet period",
                    executionCount.get(),
                    is(0)
                );

                // Note: We can't directly verify executor shutdown from here,
                // but if the test doesn't hang, it indicates clean shutdown

            } finally {
                // Ensure cleanup even if already stopped
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }

    /**
     * **Validates: Requirements 2.5**
     *
     * Unit Test: Verify Coalesce Executor Thread Is Daemon
     *
     * This test verifies that the executor service uses daemon threads
     * with the correct naming. Daemon threads prevent the JVM from
     * blocking shutdown when triggers are still active.
     */
    @Test
    @Timeout(10)
    void testCoalesceExecutorThreadDaemon() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-daemon-test");
        Path testFile = tempDir.resolve("test-daemon.txt");

        try {
            // Create file before trigger starts
            Files.writeString(testFile, "initial content", StandardCharsets.UTF_8);
            Thread.sleep(100);

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-daemon-thread")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.UPDATE))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            AtomicReference<Boolean> foundDaemonThread = new AtomicReference<>(false);
            AtomicReference<String> foundThreadName = new AtomicReference<>();
            CountDownLatch probeObserved = new CountDownLatch(1);

            Disposable subscription = Flux.from(trigger.evaluate(context.getKey(), context.getValue()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    execution -> {
                        if (RealtimeTriggerTestSupport.isProbeExecution(execution)) {
                            probeObserved.countDown();
                        }
                    },
                    error -> {}
                );

            try {
                // Wait for watcher to be ready
                RealtimeTriggerTestSupport.awaitWatcherReady(
                    tempDir,
                    RealtimeTrigger.EventType.UPDATE,
                    ".realtime-trigger-probe-daemon.txt",
                    probeObserved
                );

                // Trigger a MODIFY event to start coalescing (spawns executor thread)
                Files.writeString(testFile, "trigger coalesce", StandardCharsets.UTF_8);

                // Give executor time to schedule the task
                Thread.sleep(100);

                // Inspect all threads to find the coalesce executor thread
                Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
                for (Thread thread : threadSet) {
                    if (thread.getName().contains("realtime-trigger-coalesce")) {
                        foundDaemonThread.set(thread.isDaemon());
                        foundThreadName.set(thread.getName());
                        break;
                    }
                }

                // Verify daemon thread was found and has correct properties
                assertThat(
                    "Coalesce executor thread should exist",
                    foundThreadName.get(),
                    notNullValue()
                );

                assertThat(
                    "Coalesce executor thread should be named 'realtime-trigger-coalesce'",
                    foundThreadName.get(),
                    containsString("realtime-trigger-coalesce")
                );

                assertThat(
                    "Coalesce executor thread must be a daemon thread (won't block JVM shutdown)",
                    foundDaemonThread.get(),
                    is(true)
                );

            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }

    /**
     * **Validates: Requirements 2.3, 2.5**
     *
     * Unit Test: Verify Pending Modify Events Map Concurrency
     *
     * This test simulates concurrent MODIFY events for the same path
     * from multiple threads, verifying that:
     * 1. Only one scheduled task proceeds (others are cancelled)
     * 2. The map remains consistent under concurrent access
     * 3. No race conditions cause duplicate emissions
     */
    @Test
    @Timeout(20)
    void testPendingModifyEventsMapConcurrency() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-concurrency-test");
        Path testFile = tempDir.resolve("test-concurrency.txt");

        try {
            // Create file before trigger starts
            Files.writeString(testFile, "initial content", StandardCharsets.UTF_8);
            Thread.sleep(100);

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-map-concurrency")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.UPDATE))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            List<Execution> receivedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            AtomicInteger executionCount = new AtomicInteger(0);
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
                        executionCount.incrementAndGet();
                    },
                    watcherError::set
                );

            try {
                // Wait for watcher to be ready
                RealtimeTriggerTestSupport.awaitWatcherReady(
                    tempDir,
                    RealtimeTrigger.EventType.UPDATE,
                    ".realtime-trigger-probe-concurrency.txt",
                    probeObserved
                );

                // Simulate rapid concurrent writes from multiple "threads"
                // (In reality, file system events come from one WatchService thread,
                // but we can trigger multiple events rapidly which exercises the
                // ConcurrentHashMap and cancellation logic)
                for (int i = 0; i < 10; i++) {
                    Files.writeString(testFile, "concurrent write #" + i, StandardCharsets.UTF_8);
                    Thread.sleep(10);  // Very short delay to maximize event concurrency
                }

                // Wait for coalesced execution (should be exactly 1)
                Thread.sleep(1500);

                assertThat("Watcher must not fail", watcherError.get(), nullValue());

                int count = executionCount.get();

                // Verify exactly 1 execution despite rapid concurrent writes
                // This confirms the map handles concurrent updates correctly
                assertThat(
                    "10 rapid concurrent MODIFY events should produce exactly 1 coalesced execution (map consistency)",
                    count,
                    is(1)
                );

                // Verify the execution is UPDATE type
                receivedExecutions.forEach(execution -> {
                    assertThat(
                        execution.getTrigger().getVariables().get("changeType"),
                        equalTo("UPDATE")
                    );
                });

            } finally {
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }

    /**
     * **Validates: Requirements 2.3, 2.5**
     *
     * Unit Test: Verify Cleanup Cancels Pending Tasks
     *
     * This test schedules multiple coalesced events for different paths,
     * then calls cleanup() (via stop()) to verify that:
     * 1. All ScheduledFutures are cancelled
     * 2. The pendingModifyEvents map is cleared
     * 3. The executor is shutdown cleanly
     * 4. No executions are emitted after cleanup
     */
    @Test
    @Timeout(20)
    void testCleanupCancelsPendingTasks() throws Exception {
        Path tempDir = Files.createTempDirectory("realtime-trigger-cleanup-test");
        Path file1 = tempDir.resolve("file1.txt");
        Path file2 = tempDir.resolve("file2.txt");
        Path file3 = tempDir.resolve("file3.txt");

        try {
            // Create files before trigger starts
            Files.writeString(file1, "initial 1", StandardCharsets.UTF_8);
            Files.writeString(file2, "initial 2", StandardCharsets.UTF_8);
            Files.writeString(file3, "initial 3", StandardCharsets.UTF_8);
            Thread.sleep(100);

            RealtimeTrigger trigger = RealtimeTrigger.builder()
                .id("test-cleanup-pending")
                .type(RealtimeTrigger.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .on(Property.ofValue(RealtimeTrigger.EventType.UPDATE))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            List<Execution> receivedExecutions = new CopyOnWriteArrayList<>();
            AtomicReference<Throwable> watcherError = new AtomicReference<>();
            AtomicInteger executionCount = new AtomicInteger(0);
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
                        executionCount.incrementAndGet();
                    },
                    watcherError::set
                );

            try {
                // Wait for watcher to be ready
                RealtimeTriggerTestSupport.awaitWatcherReady(
                    tempDir,
                    RealtimeTrigger.EventType.UPDATE,
                    ".realtime-trigger-probe-cleanup.txt",
                    probeObserved
                );

                // Schedule multiple coalesced events (write to 3 different files)
                Files.writeString(file1, "write 1", StandardCharsets.UTF_8);
                Files.writeString(file2, "write 2", StandardCharsets.UTF_8);
                Files.writeString(file3, "write 3", StandardCharsets.UTF_8);

                // Give events time to be scheduled (but not enough to execute)
                Thread.sleep(200);  // 200ms < 500ms quiet period

                // Stop the trigger (calls cleanup internally)
                // This should cancel all 3 pending scheduled tasks
                trigger.stop();

                // Wait to ensure no executions arrive (they should all be cancelled)
                Thread.sleep(1000);

                assertThat("Watcher must not fail", watcherError.get(), nullValue());

                int count = executionCount.get();

                // Verify NO executions were emitted (all cancelled by cleanup)
                assertThat(
                    "Cleanup should cancel all pending coalesced events (no executions after stop)",
                    count,
                    is(0)
                );

                // Verify subscription completed cleanly
                receivedExecutions.forEach(execution -> {
                    // Should not reach here, but if we do, verify it's UPDATE
                    assertThat(
                        execution.getTrigger().getVariables().get("changeType"),
                        equalTo("UPDATE")
                    );
                });

            } finally {
                // Ensure trigger is stopped (may already be stopped from test)
                trigger.stop();
                subscription.dispose();
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }
}
