package io.kestra.plugin.fs.local;

import io.kestra.core.models.executions.Execution;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

final class RealtimeTriggerTestSupport {
    static final String PROBE_PREFIX = ".realtime-trigger-probe-";

    private RealtimeTriggerTestSupport() {
    }

    static boolean isProbeExecution(Execution execution) {
        Object fileValue = execution.getTrigger().getVariables().get("file");

        if (!(fileValue instanceof Map<?, ?> file)) {
            return false;
        }

        Object name = file.get("name");
        return name instanceof String fileName && fileName.startsWith(PROBE_PREFIX);
    }

    static void awaitWatcherReady(
        Path watchedDirectory,
        RealtimeTrigger.EventType eventType,
        String probeFileName,
        CountDownLatch probeObserved
    ) throws Exception {
        if (!probeFileName.startsWith(PROBE_PREFIX)
            || probeFileName.contains("/")
            || probeFileName.contains("\\")) {
            throw new IllegalArgumentException("Invalid probe filename: " + probeFileName);
        }

        Path probe = watchedDirectory.resolve(probeFileName);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        int attempt = 0;

        while (probeObserved.getCount() != 0 && System.nanoTime() < deadline) {
            String content = "probe-" + attempt++;

            switch (eventType) {
                case CREATE, CREATE_OR_UPDATE -> {
                    Files.deleteIfExists(probe);
                    Files.writeString(probe, content);
                }
                case UPDATE -> {
                    if (!Files.exists(probe)) {
                        Files.writeString(probe, content);
                    }
                    Files.writeString(probe, content + "-updated");
                }
                case DELETE -> {
                    Files.writeString(probe, content);
                    Thread.sleep(50);
                    Files.deleteIfExists(probe);
                }
            }

            // For UPDATE events, wait longer to account for 500ms coalescing delay
            // For other events, use shorter polling interval
            long waitMs = (eventType == RealtimeTrigger.EventType.UPDATE) ? 600 : 25;
            probeObserved.await(waitMs, TimeUnit.MILLISECONDS);
        }

        if (probeObserved.getCount() != 0) {
            throw new AssertionError(
                "Watcher did not observe the probe event within 5 seconds"
            );
        }

        Files.deleteIfExists(probe);
    }
}