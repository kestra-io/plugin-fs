package io.kestra.plugin.fs.local;

import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.RealtimeTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.fs.local.models.File;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger on local filesystem events (realtime)",
    description = """
        Watches a local directory for filesystem events (create, modify, delete) using Java NIO WatchService.
        Emits executions when matching filesystem events are reported, without polling.

        **Configuration:** This trigger requires `allowed-paths` to be configured for
        `io.kestra.plugin.fs.local.RealtimeTrigger` under `kestra.plugins.configurations`.

        Example:
        ```yaml
        kestra:
          plugins:
            configurations:
              - type: io.kestra.plugin.fs.local.RealtimeTrigger
                values:
                  allowed-paths:
                    - /data/incoming
        ```

        **Event behavior:** UPDATE events are coalesced per path after a 500 ms quiet period.
        CREATE events are emitted immediately and can observe a partial file; with
        CREATE_OR_UPDATE, a newly written file commonly emits CREATE followed by UPDATE.

        **Platform note:** On macOS, WatchService uses a polling-based fallback and may not
        detect events as quickly as on Linux or Windows.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "React immediately when a file is created or updated",
            full = true,
            code = """
                id: local_realtime_trigger
                namespace: company.team

                triggers:
                  - id: watch
                    type: io.kestra.plugin.fs.local.RealtimeTrigger
                    from: /data/incoming
                    on: CREATE_OR_UPDATE

                tasks:
                  - id: log_file
                    type: io.kestra.plugin.core.log.Log
                    message: "New file: {{ trigger.file.localPath }}"
                """
        ),
        @Example(
            title = "Watch recursively and filter by file extension",
            full = true,
            code = """
                id: local_realtime_recursive
                namespace: company.team

                triggers:
                  - id: watch_csv
                    type: io.kestra.plugin.fs.local.RealtimeTrigger
                    from: /data/incoming
                    recursive: true
                    regExp: ".*\\\\.csv$"

                tasks:
                  - id: process
                    type: io.kestra.plugin.core.log.Log
                    message: "CSV ready: {{ trigger.file.localPath }}"
                """
        ),
        @Example(
            title = "React to file deletions for cleanup tracking",
            full = true,
            code = """
                id: local_realtime_delete
                namespace: company.team

                triggers:
                  - id: watch_deletes
                    type: io.kestra.plugin.fs.local.RealtimeTrigger
                    from: /data/incoming
                    on: DELETE

                tasks:
                  - id: log_removed
                    type: io.kestra.plugin.core.log.Log
                    message: "File removed: {{ trigger.file.localPath }}"
                """
        )
    }
)
public class RealtimeTrigger extends AbstractTrigger
    implements RealtimeTriggerInterface, TriggerOutput<RealtimeTrigger.Output> {

    private static final int MAX_REGEX_LENGTH = 1000;

    public enum EventType {
        CREATE,
        UPDATE,
        CREATE_OR_UPDATE,
        DELETE
    }

    public enum ChangeType {
        CREATE,
        UPDATE,
        DELETE
    }

    @Schema(title = "Directory to watch")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> from;

    @Schema(title = "Include files in subdirectories")
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> recursive = Property.ofValue(false);

    @Schema(title = "Regex pattern to match against the full file path")
    @PluginProperty(group = "advanced")
    private Property<String> regExp;

    @Schema(title = "Event types to trigger on")
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<EventType> on = Property.ofValue(EventType.CREATE_OR_UPDATE);

    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    @Getter(AccessLevel.NONE)
    private transient final AtomicBoolean active = new AtomicBoolean(false);


    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    @Getter(AccessLevel.NONE)
    private transient final AtomicReference<WatchService> watchService = new AtomicReference<>();

    private static final long SCAN_DEDUP_WINDOW_NANOS = 10_000_000_000L; // 10 seconds
    private static final long COALESCE_DELAY_MS = 500L;
    private void validatePath(Path path, List<Path> normalizedAllowedPaths) {
        Path realPath;
        try {
            if (path.toFile().exists()) {
                realPath = path.toRealPath();
            } else {
                Path absolute = path.toAbsolutePath().normalize();
                Path ancestor = absolute;
                Path suffix = Path.of("");
                while (ancestor != null && !ancestor.toFile().exists()) {
                    suffix = ancestor.getFileName() == null ? suffix : ancestor.getFileName().resolve(suffix);
                    ancestor = ancestor.getParent();
                }
                if (ancestor != null) {
                    realPath = ancestor.toRealPath().resolve(suffix);
                } else {
                    realPath = absolute;
                }
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid path: " + path + ". Error: " + e.getMessage(), e);
        }

        boolean isAllowed = normalizedAllowedPaths.stream()
            .anyMatch(realPath::startsWith);

        if (!isAllowed) {
            String formattedAllowedPaths = normalizedAllowedPaths.stream()
                .map(Path::toString)
                .collect(Collectors.joining("', '", "'", "'"));
            throw new SecurityException(
                "Access to path '" + realPath + "' is denied. " +
                "The specified path must be within one of the configured 'allowed-paths': " + formattedAllowedPaths + ". " +
                "Refer to: https://kestra.io/docs/configuration#set-default-values"
            );
        }
    }

    @Override
    public Publisher<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        final RunContext runContext = conditionContext.getRunContext();
        final Logger logger = runContext.logger();

        return Flux.<Execution>create(emitter -> {
            var watchKeyMap = new ConcurrentHashMap<WatchKey, Path>();
            var registeredDirectories = Collections.newSetFromMap(new ConcurrentHashMap<Path, Boolean>());
            var knownDirectories = Collections.newSetFromMap(new ConcurrentHashMap<Path, Boolean>());
            var scannedFiles = new ConcurrentHashMap<Path, Long>();
            var pendingEvents = new ConcurrentHashMap<Path, ScheduledFuture<?>>();
            var coalesceExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
                var thread = new Thread(runnable, "realtime-trigger-coalesce");
                thread.setDaemon(true);
                return thread;
            });
            emitter.onDispose(() -> {
                logger.info("RealtimeTrigger disposed");
                stop();
            });

            try {
                active.set(true);

                if (emitter.isCancelled()) {
                    stop();
                    return;
                }

                var renderedFrom = runContext.render(this.from).as(String.class)
                    .orElseThrow(() -> new IllegalArgumentException("`from` is required"));
                var rootDirectory = Paths.get(renderedFrom).toAbsolutePath().normalize();

                logger.info("RealtimeTrigger starting for directory: {}", rootDirectory);
                var allowedPathConfig = runContext.<List<String>>pluginConfiguration(AbstractLocalTask.ALLOWED_PATHS);
                if (allowedPathConfig.isEmpty() || allowedPathConfig.get().isEmpty()) {
                    throw new SecurityException(
                        "The 'allowed-paths' configuration is required to enable access to the local filesystem. " +
                        "You must define at least one allowed path in the plugin configuration, " +
                        "`kestra.plugins.configurations.io.kestra.plugin.fs.local.RealtimeTrigger.allowed-paths`. " +
                        "Refer to the example in the plugin documentation."
                    );
                }

                var normalizedAllowedPaths = allowedPathConfig.get().stream()
                    .map(allowed -> {
                        try {
                            return Paths.get(allowed).toRealPath();
                        } catch (IOException e) {
                            return Paths.get(allowed).toAbsolutePath().normalize();
                        }
                    })
                    .toList();


                var renderedOn = runContext.render(this.on).as(EventType.class)
                    .orElse(EventType.CREATE_OR_UPDATE);
                Pattern compiledPattern = null;

                if (this.regExp != null) {
                    var renderedRegExp = runContext.render(this.regExp)
                        .as(String.class)
                        .orElse(null);

                    if (renderedRegExp != null) {
                        if (renderedRegExp.length() > MAX_REGEX_LENGTH) {
                            throw new IllegalArgumentException(
                                "'regExp' must not exceed " + MAX_REGEX_LENGTH + " characters"
                            );
                        }


                        try {
                            compiledPattern = Pattern.compile(renderedRegExp);
                        } catch (PatternSyntaxException e) {
                            throw new IllegalArgumentException(
                                "Invalid regex pattern in 'regExp': "
                                    + renderedRegExp + " - " + e.getMessage(),
                                e
                            );
                        }
                    }
                }

                var rRecursive = runContext.render(this.recursive)
                    .as(Boolean.class)
                    .orElse(false);

                validatePath(rootDirectory, normalizedAllowedPaths);

                if (!Files.exists(rootDirectory)) {
                    throw new IllegalArgumentException("Directory does not exist: " + rootDirectory);
                }
                if (!Files.isDirectory(rootDirectory)) {
                    throw new IllegalArgumentException("Path is not a directory: " + rootDirectory);
                }

                if (!active.get()) {
                    emitter.complete();
                    return;
                }

                logger.info("Watching directory: {}", rootDirectory);

                createWatchService();

                // Check cancellation after resource creation
                if (!active.get()) {
                    emitter.complete();
                    return;
                }

                registerDirectory(rootDirectory, watchKeyMap, registeredDirectories);
                knownDirectories.add(rootDirectory.toAbsolutePath().normalize());

                if (rRecursive) {
                    registerDirectoryTree(
                        rootDirectory,
                        watchKeyMap,
                        registeredDirectories,
                        knownDirectories,
                        normalizedAllowedPaths,
                        logger
                    );
                } else {
                    // For non-recursive mode, track immediate child directories (pre-existing)
                    // so DELETE events for them can be suppressed
                    try (var stream = Files.list(rootDirectory)) {
                        stream.filter(p -> {
                            try {
                                return Files.isDirectory(p) && !Files.isSymbolicLink(p);
                            } catch (Exception e) {
                                return false;
                            }
                        }).forEach(childDir -> knownDirectories.add(childDir.toAbsolutePath().normalize()));
                    } catch (IOException e) {
                        logger.debug("Could not list child directories for tracking: {}", e.getMessage());
                    }
                }

                if (!active.get()) {
                    emitter.complete();
                    return;
                }


                // Check cancellation before entering event loop
                if (!active.get()) {
                    emitter.complete();
                    return;
                }

                logger.info("RealtimeTrigger initialized with {} watch keys. Waiting for filesystem events", watchKeyMap.size());

                // Capture the WatchService reference once at the beginning to avoid null-dereference race with stop()
                WatchService service = watchService.get();
                if (service == null) {
                    throw new IllegalStateException("WatchService not initialized");
                }

                while (active.get()) {
                    WatchKey key;
                    try {
                        // Poll with timeout to allow periodic cleanup of expired deduplication entries
                        // even when no filesystem events occur
                        key = service.poll(2, TimeUnit.SECONDS);
                    } catch (ClosedWatchServiceException e) {
                        // Normal shutdown: stop() closed the WatchService, so poll() unblocked.
                        break;
                    } catch (InterruptedException e) {
                        // Thread interrupted, but check active flag before assuming shutdown.
                        if (!active.get()) {
                            break;
                        }
                        // If still active, it's an unexpected interrupt; continue watching.
                        continue;
                    }


                    if (key == null) {
                        cleanupExpiredScanEntries(scannedFiles, System.nanoTime());
                        continue;
                    }

                    var directory = watchKeyMap.get(key);
                    if (directory == null) {
                        // Race condition: the key was removed or map was cleared during shutdown.
                        // Safely discard this key without trying to reset it.
                        continue;
                    }

                    // Process all events for this key
                    for (WatchEvent<?> event : key.pollEvents()) {
                        try {
                            var kind = event.kind();

                            if (kind == StandardWatchEventKinds.OVERFLOW) {
                                logger.warn("WatchService overflow event; some filesystem events may have been missed");
                                continue;
                            }

                            var eventPath = directory.resolve((Path) event.context());
                            var normalizedEventPath = eventPath.toAbsolutePath().normalize();

                            // Clear scan deduplication state even when DELETE is filtered out.
                            if (kind == StandardWatchEventKinds.ENTRY_DELETE) {
                                scannedFiles.remove(normalizedEventPath);
                            }

                            // Directory cleanup is internal bookkeeping and must not depend
                            // on the configured event type or regex filter.
                            if (kind == StandardWatchEventKinds.ENTRY_DELETE
                                && knownDirectories.contains(normalizedEventPath)) {
                                unregisterDirectoryTree(
                                    normalizedEventPath,
                                    watchKeyMap,
                                    registeredDirectories,
                                    knownDirectories
                                );
                                continue;
                            }

                            if (kind == StandardWatchEventKinds.ENTRY_CREATE
                                && rRecursive
                                && Files.isDirectory(eventPath)
                                && !Files.isSymbolicLink(eventPath)) {
                                try {
                                    validatePath(eventPath, normalizedAllowedPaths);

                                    registerDirectory(eventPath, watchKeyMap, registeredDirectories);
                                    knownDirectories.add(eventPath.toAbsolutePath().normalize());

                                    registerDirectoryTree(
                                        eventPath,
                                        watchKeyMap,
                                        registeredDirectories,
                                        knownDirectories,
                                        normalizedAllowedPaths,
                                        logger
                                    );
                                    // Scan and emit CREATE events for existing files in the newly registered tree
                                    scanAndEmitExistingFiles(
                                        eventPath,
                                        renderedOn,
                                        compiledPattern,
                                        normalizedAllowedPaths,
                                        conditionContext,
                                        context,
                                        emitter,
                                        logger,
                                        scannedFiles
                                    );
                                } catch (SecurityException e) {
                                    logger.warn(
                                        "Security: Rejecting dynamically created directory outside allowed-paths: {} - {}",
                                        eventPath,
                                        e.getMessage()
                                    );
                                } catch (IllegalArgumentException e) {
                                    logger.warn(
                                        "Could not validate dynamically created directory {}: {}",
                                        eventPath,
                                        e.getMessage()
                                    );
                                } catch (IOException e) {
                                    logger.debug(
                                        "Could not register dynamically created directory {}: {}",
                                        eventPath,
                                        e.getMessage()
                                    );
                                }
                                continue;
                            }

                            // Track directories for non-recursive mode (ENTRY_CREATE for child directories)
                            if (kind == StandardWatchEventKinds.ENTRY_CREATE
                                && !rRecursive
                                && Files.isDirectory(eventPath)
                                && !Files.isSymbolicLink(eventPath)) {
                                knownDirectories.add(eventPath.toAbsolutePath().normalize());
                                continue;
                            }

                            if (Files.isSymbolicLink(eventPath)) {
                                logger.warn("Security: Rejecting symbolic link event: {}", eventPath);
                                continue;
                            }

                            // Security: Validate event path against allowed-paths before acting on it
                            try {
                                validatePath(eventPath, normalizedAllowedPaths);
                            } catch (SecurityException | IllegalArgumentException e) {
                                logger.warn(
                                    "Security: Rejecting event for path outside allowed-paths: {} - {}",
                                    eventPath,
                                    e.getMessage()
                                );
                                continue;
                            }

                            var normalizedEventPath2 = eventPath.toAbsolutePath().normalize();

                            // A DELETE must cancel a pending MODIFY even when output filters exclude DELETE.
                            if (kind == StandardWatchEventKinds.ENTRY_DELETE) {
                                cancelPendingEvent(normalizedEventPath2, pendingEvents, logger);
                            }

                            if (!shouldTrigger(kind, renderedOn)) {
                                continue;
                            }

                            if (compiledPattern != null && !matchesRegExp(eventPath, compiledPattern)) {
                                continue;
                            }

                            // Deduplication: Check if this CREATE event is a duplicate of a recent scan
                            if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                                var currentTime = System.nanoTime();
                                if (isDuplicateOfRecentScan(normalizedEventPath2, currentTime, scannedFiles)) {
                                    logger.debug("Suppressing duplicate CREATE event for recently scanned file: {}", eventPath);
                                    continue;
                                }
                            }
                            // Process event based on type
                            if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                                emitEventExecution(
                                    eventPath,
                                    ChangeType.CREATE,
                                    conditionContext,
                                    context,
                                    emitter,
                                    logger
                                );
                            } else if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {
                                scheduleCoalescedEvent(
                                    normalizedEventPath2,
                                    eventPath,
                                    ChangeType.UPDATE,
                                    conditionContext,
                                    context,
                                    emitter,
                                    logger,
                                    coalesceExecutor,
                                    pendingEvents
                                );
                            } else if (kind == StandardWatchEventKinds.ENTRY_DELETE) {
                                emitEventExecution(
                                    eventPath,
                                    ChangeType.DELETE,
                                    conditionContext,
                                    context,
                                    emitter,
                                    logger
                                );
                            }

                        } catch (Exception e) {
                            // An error processing this individual event should not terminate the entire watcher.
                            logger.warn("Error processing filesystem event: {}", e.getMessage(), e);
                            // Continue to the next event
                        }
                    }

                    var valid = key.reset();
                    cleanupExpiredScanEntries(scannedFiles, System.nanoTime());
                    if (!valid) {
                        // Key is no longer valid (directory deleted, etc.)
                        var registeredDirectory = watchKeyMap.remove(key);
                        if (registeredDirectory != null) {
                            var normalizedDirectory = registeredDirectory.toAbsolutePath().normalize();

                            registeredDirectories.remove(normalizedDirectory);

                            // Keep knownDirectories until the parent ENTRY_DELETE event
                            // consumes the directory path. The child WatchKey becoming
                            // invalid may be processed before that parent event.
                        }
                        // If all directories are gone, stop watching
                        if (watchKeyMap.isEmpty()) {
                            logger.info("All registered directories are no longer valid; terminating watcher");
                            break;
                        }
                    }
                }

                logger.info("RealtimeTrigger event loop completed");

                emitter.complete();
            } catch (Exception e) {
                if (active.get()) {
                    logger.error("RealtimeTrigger error: {}", e.getMessage(), e);
                    emitter.error(e);
                } else {
                    logger.debug("RealtimeTrigger stopped during initialization or event processing: {}", e.getMessage());
                    emitter.complete();
                }
            } finally {
                cleanup(watchKeyMap, registeredDirectories, knownDirectories, pendingEvents, coalesceExecutor, logger);
            }
        });
    }

    /**
     * Emits an execution for a file event.
     * Handles CREATE, UPDATE (MODIFY), and DELETE events.
     * Skips directories—only file events produce executions.
     *
     * @param eventPath the file path that changed
     * @param changeType the event type: CREATE, UPDATE, or DELETE
     * @param conditionContext the trigger condition context
     * @param context the trigger context
     * @param emitter the flux emitter to emit executions to
     */
    private void emitEventExecution(
        Path eventPath,
        ChangeType changeType,
        ConditionContext conditionContext,
        TriggerContext context,
        FluxSink<Execution> emitter,
        Logger logger
    ) {
        try {
            // For DELETE, the file no longer exists, so we can't read attributes
            // For CREATE/UPDATE, attempt to read to detect directory vs file
            BasicFileAttributes attrs = null;

            if (changeType != ChangeType.DELETE) {
                try {
                    attrs = Files.readAttributes(eventPath, BasicFileAttributes.class);
                } catch (NoSuchFileException e) {
                    logger.debug("File disappeared during {} event processing: {}",             changeType, eventPath);
                    return;
                }

                if (attrs.isDirectory()) {
                    return;
                }
            }

            // Create file metadata for the execution
            File file;

            if (changeType == ChangeType.DELETE) {
                file = File.builder()
                    .uri(eventPath.toUri())
                    .localPath(eventPath.toAbsolutePath().normalize())
                    .name(eventPath.getFileName().toString())
                    .parent(eventPath.getParent().toString())
                    .size(null)
                    .createdDate(null)
                    .modifiedDate(null)
                    .accessedDate(null)
                    .isDirectory(false)
                    .build();
            } else {
                file = File.from(eventPath, attrs);
            }

            var output = Output.builder()
                .file(file)
                .changeType(changeType)
                .build();

            var execution = TriggerService.generateRealtimeExecution(
                this,
                conditionContext,
                context,
                output
            );

            emitter.next(execution);

        } catch (Exception e) {
            logger.warn("Error processing {} event for {}: {}", changeType, eventPath, e.getMessage(), e);
        }
    }

    private void scheduleCoalescedEvent(
        Path normalizedPath,
        Path originalPath,
        ChangeType changeType,
        ConditionContext conditionContext,
        TriggerContext context,
        FluxSink<Execution> emitter,
        Logger logger,
        ScheduledExecutorService coalesceExecutor,
        Map<Path, ScheduledFuture<?>> pendingEvents
    ) {
        synchronized (pendingEvents) {
            var existingFuture = pendingEvents.remove(normalizedPath);
            if (existingFuture != null && !existingFuture.isDone()) {
                existingFuture.cancel(false);
            }

            var futureRef = new AtomicReference<ScheduledFuture<?>>();

            var newFuture = coalesceExecutor.schedule(() -> {
                synchronized (pendingEvents) {
                    var expectedFuture = futureRef.get();

                    if (expectedFuture == null
                        || !pendingEvents.remove(normalizedPath, expectedFuture)) {
                        return;
                    }

                    if (!active.get()) {
                        logger.debug(
                            "Skipping coalesced {} emission for {} (trigger stopped)",
                            changeType,
                            normalizedPath
                        );
                        return;
                    }

                    try {
                        emitEventExecution(
                            originalPath,
                            changeType,
                            conditionContext,
                            context,
                            emitter,
                            logger
                        );
                    } catch (Exception e) {
                        logger.warn(
                            "Error emitting coalesced {} event for {}: {}",
                            changeType,
                            normalizedPath,
                            e.getMessage(),
                            e
                        );
                    }
                }
            }, COALESCE_DELAY_MS, TimeUnit.MILLISECONDS);

            futureRef.set(newFuture);
            pendingEvents.put(normalizedPath, newFuture);
        }
    }

    private void cancelPendingEvent(Path normalizedPath, Map<Path, ScheduledFuture<?>> pendingEvents, Logger logger) {
        synchronized (pendingEvents) {
            var future = pendingEvents.remove(normalizedPath);

            if (future != null && !future.isDone()) {
                future.cancel(false);
                logger.debug(
                    "Cancelled pending event due to DELETE: {}",
                    normalizedPath
                );
            }
        }
    }

    /**
     * Determines whether a WatchEvent.Kind matches the configured EventType filter.
     * Maps WatchService StandardWatchEventKinds to EventType values.
     *
     * @param kind the WatchEvent.Kind from the WatchService
     * @param eventType the configured EventType filter
     * @return true if the event should be processed, false otherwise
     */
    private boolean shouldTrigger(WatchEvent.Kind<?> kind, EventType eventType) {
        if (eventType == EventType.CREATE) {
            return kind == StandardWatchEventKinds.ENTRY_CREATE;
        } else if (eventType == EventType.UPDATE) {
            return kind == StandardWatchEventKinds.ENTRY_MODIFY;
        } else if (eventType == EventType.CREATE_OR_UPDATE) {
            return kind == StandardWatchEventKinds.ENTRY_CREATE || kind == StandardWatchEventKinds.ENTRY_MODIFY;
        } else if (eventType == EventType.DELETE) {
            return kind == StandardWatchEventKinds.ENTRY_DELETE;
        }
        return false;
    }

    private boolean matchesRegExp(Path path, Pattern pattern) {
        if (pattern == null) {
            return true;
        }
        String pathStr = path.toString();
        return pattern.matcher(pathStr).matches();
    }

    /**
     * Creates the WatchService for filesystem monitoring.
     */
    private void createWatchService() throws Exception {
        if (watchService.get() != null) {
            throw new IllegalStateException("WatchService already created");
        }
        watchService.set(FileSystems.getDefault().newWatchService());
    }
    /**
     * Cancels watch registrations for a removed directory and its descendants,
     * and removes the corresponding tracking paths.
     *
     * @param directory the removed directory
     * @param watchKeyMap the map of watch keys to directories
     * @param registeredDirectories the set of registered directories
     * @param knownDirectories the set of known directories
     */
    private void unregisterDirectoryTree(
        Path directory,
        Map<WatchKey, Path> watchKeyMap,
        Set<Path> registeredDirectories,
        Set<Path> knownDirectories
    ) {
        Path normalizedDirectory = directory.toAbsolutePath().normalize();

        // Cancel and remove registrations for this directory and its descendants.
        synchronized (registeredDirectories) {
            for (Map.Entry<WatchKey, Path> entry : watchKeyMap.entrySet()) {
                Path registeredPath = entry.getValue().toAbsolutePath().normalize();

                if (registeredPath.startsWith(normalizedDirectory)) {
                    var key = entry.getKey();
                    key.cancel();

                    if (watchKeyMap.remove(key, entry.getValue())) {
                        registeredDirectories.remove(registeredPath);
                    }
                }
            }
        }

        // Remove the directory and all known descendant paths.
        knownDirectories.removeIf(path -> path.startsWith(normalizedDirectory));
    }

    /**
     * Registers a single directory to be watched for events.
     * Events monitored: ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE
     *
     * @param directory the directory to register
     * @param map the map to store WatchKey -> Path associations
     * @param dirs the set to track registered directories
     * @throws Exception if registration fails
     */
    private void registerDirectory(
        Path directory,
        Map<WatchKey, Path> map,
        Set<Path> dirs
    ) throws Exception {
        Path normalizedDirectory = directory.toAbsolutePath().normalize();

        synchronized (dirs) {
            if (dirs.contains(normalizedDirectory)) {
                return;
            }

            WatchService svc = watchService.get();
            if (svc == null) {
                throw new IllegalStateException("WatchService not initialized");
            }

            var key = normalizedDirectory.register(
                svc,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_MODIFY,
                StandardWatchEventKinds.ENTRY_DELETE
            );

            if (!active.get()) {
                key.cancel();
                throw new IllegalStateException("Trigger stopped during registration");
            }

            dirs.add(normalizedDirectory);
            map.put(key, normalizedDirectory);
        }
    }

    /**
     * Recursively walks the directory tree and registers all subdirectories.
     * Each directory is validated against the configured allowed-paths before
     * registration. Symbolic links are excluded to prevent following linked paths.
     *
     * If a child directory disappears or becomes inaccessible during traversal,
     * it is skipped and registration continues.
     *
     * @param root the root directory to walk
     * @param map the map to store WatchKey -> Path associations
     * @param dirs the set to track registered directories
     * @param knownDirs the set to track all known directories for DELETE suppression
     * @param normalizedAllowedPaths the normalized paths permitted for access
     * @param logger the logger used for registration warnings
     * @throws Exception if an unrecoverable registration error occurs
     */
    private void registerDirectoryTree(
        Path root,
        Map<WatchKey, Path> map,
        Set<Path> dirs,
        Set<Path> knownDirs,
        List<Path> normalizedAllowedPaths,
        Logger logger
    ) throws Exception {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(root)) {
            for (Path entry : stream) {
                if (!Files.isDirectory(entry) || Files.isSymbolicLink(entry)) {
                    continue;
                }

                try {
                    validatePath(entry, normalizedAllowedPaths);

                    registerDirectory(entry, map, dirs);
                    knownDirs.add(entry.toAbsolutePath().normalize());

                    registerDirectoryTree(
                        entry,
                        map,
                        dirs,
                        knownDirs,
                        normalizedAllowedPaths,
                        logger
                    );
                } catch (SecurityException e) {
                    logger.warn(
                        "Security: Rejecting directory outside allowed-paths during recursive registration: {} - {}",
                        entry,
                        e.getMessage()
                    );
                } catch (IllegalArgumentException e) {
                    logger.debug(
                        "Could not validate directory {} during recursive registration: {}",
                        entry,
                        e.getMessage()
                    );
                } catch (IOException e) {
                    // The directory may have disappeared or become inaccessible
                    // between discovery and registration. Continue with siblings.
                    logger.debug(
                        "Could not register directory {} during recursive registration: {}",
                        entry,
                        e.getMessage()
                    );
                }
            }
        }
    }

    /**
     * Scans a newly created directory tree and emits CREATE executions for eligible regular files.
     * This handles the case where a populated directory is moved or created while the trigger is running.
     *
     * Files are tracked in scannedFiles to prevent duplicate events when both the scan
     * and a queued WatchService event discover the same file.
     *
     * @param root the root directory to scan
     * @param renderedOn the configured event type filter
     * @param compiledPattern the compiled regex pattern for filename filtering (null if none)
     * @param normalizedAllowedPaths the list of normalized allowed paths for security validation
     * @param conditionContext the condition context for execution creation
     * @param context the trigger context
     * @param emitter the flux emitter
     * @param logger the logger
     */
    private void scanAndEmitExistingFiles(
        Path root,
        EventType renderedOn,
        Pattern compiledPattern,
        List<Path> normalizedAllowedPaths,
        ConditionContext conditionContext,
        TriggerContext context,
        FluxSink<Execution> emitter,
        Logger logger,
        Map<Path, Long> scannedFiles
    ) {
        Deque<Path> dirsToScan = new ArrayDeque<>();
        dirsToScan.add(root);

        while (!dirsToScan.isEmpty() && active.get()) {
            Path dir = dirsToScan.removeFirst();

            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
                for (Path entry : stream) {
                    if (!active.get()) {
                        return;
                    }

                    Path normalizedPath = entry.toAbsolutePath().normalize();

                    try {
                        if (Files.isSymbolicLink(entry)) {
                            logger.debug("Scan: Skipping symbolic link: {}", entry);
                            continue;
                        }

                        validatePath(entry, normalizedAllowedPaths);

                        if (Files.isDirectory(entry)) {
                            dirsToScan.addLast(entry);
                           continue;
                        }

                        if (!Files.isRegularFile(entry)
                            || !shouldTrigger(StandardWatchEventKinds.ENTRY_CREATE, renderedOn)
                            || (compiledPattern != null && !matchesRegExp(entry, compiledPattern))) {
                            continue;
                        }

                        long scanTime = System.nanoTime();
                        Long previousScanTime = scannedFiles.putIfAbsent(normalizedPath, scanTime);

                        if (previousScanTime != null
                            && scanTime - previousScanTime <= SCAN_DEDUP_WINDOW_NANOS) {
                            continue;
                        }

                        if (previousScanTime != null) {
                            scannedFiles.replace(normalizedPath, previousScanTime, scanTime);
                        }

                        try {
                            emitEventExecution(
                                entry,
                                ChangeType.CREATE,
                                conditionContext,
                                context,
                                emitter,
                                logger
                            );
                        } catch (RuntimeException e) {
                            scannedFiles.remove(normalizedPath, scanTime);
                            throw e;
                        }
                    } catch (SecurityException | IllegalArgumentException e) {
                        logger.warn(
                            "Security: Rejecting scanned path {}: {}",
                            entry,
                            e.getMessage()
                        );
                    } catch (Exception e) {
                        logger.warn("Error scanning entry {}: {}", entry, e.getMessage(), e);
                    }
                }
            } catch (IOException e) {
                logger.warn("Could not scan directory {}: {}", dir, e.getMessage());
            }
        }
    }


    /**
     * Checks if a file event should be suppressed due to recent scan discovery.
     * Returns true if the file was scanned recently (within SCAN_DEDUP_WINDOW_NANOS).
     *
     * Also performs cleanup of expired entries from scannedFiles map.
     *
     * @param filePath the normalized file path
     * @param currentTime the current time in nanoseconds
     * @return true if the event should be suppressed
     */

    private boolean isDuplicateOfRecentScan(Path filePath, long currentTime, Map<Path, Long> scannedFiles) {
        var scanTime = scannedFiles.get(filePath);
        if (scanTime == null) {
            return false;
        }

        long age = currentTime - scanTime;
        if (age > SCAN_DEDUP_WINDOW_NANOS) {
            // Expired, remove it
            scannedFiles.remove(filePath, scanTime);
            return false;
        }
        return true;
    }

    /**
     * Removes all expired entries from scannedFiles map.
     * An entry is expired if its age exceeds SCAN_DEDUP_WINDOW_NANOS.
     *
     * This method should be called periodically during event processing to prevent
     * unbounded memory growth when many unique files are scanned.
     *
     * @param currentTime the current time in nanoseconds
     */
    private void cleanupExpiredScanEntries(Map<Path, Long> scannedFiles, long currentTime) {
        scannedFiles.entrySet().removeIf(entry -> {
            long age = currentTime - entry.getValue();
            return age > SCAN_DEDUP_WINDOW_NANOS;
        });
    }


    @Override
    public void stop() {
        if (active.compareAndSet(true, false)) {
            // Unblock the event loop by closing the WatchService.
            // The evaluate() thread will perform final cleanup in its finally block.
            closeWatchService(null);
        }
    }

    @Override
    public void kill() {
        stop();
    }

    /**
     * Closes the WatchService to unblock the event loop.
     * Uses getAndSet(null) to atomically claim ownership.
     * Only the thread that successfully gets a non-null reference will close it.
     */
    private void closeWatchService(Logger logger) {
        WatchService service = watchService.getAndSet(null);
        if (service == null) {
            return;
        }

        try {
            service.close();
            if (logger != null) {
                logger.info("RealtimeTrigger WatchService closed");
            }
        } catch (Exception e) {
            if (logger != null) {
                logger.warn("Error closing WatchService: {}", e.getMessage());
            }
        }
    }

    private void cleanup(
        Map<WatchKey, Path> watchKeyMap,
        Set<Path> registeredDirectories,
        Set<Path> knownDirectories,
        Map<Path, ScheduledFuture<?>> pendingEvents,
        ScheduledExecutorService coalesceExecutor,
        Logger logger
    ) {
        // Atomically claim ownership of the WatchService.
        // getAndSet(null) ensures only one thread gets a non-null reference.
        WatchService svc = watchService.getAndSet(null);

        try {
            if (svc != null) {
                svc.close();
                logger.info("RealtimeTrigger WatchService closed");
            }
        } catch (Exception e) {
            logger.warn("Error closing WatchService during cleanup: {}", e.getMessage());
        } finally {
            // Establish invariant: trigger is fully inactive
            active.set(false);

            var cancelledCount = 0;
            synchronized (pendingEvents) {
                for (var entry : pendingEvents.entrySet()) {
                    var future = entry.getValue();
                    if (future != null && !future.isDone()) {
                        future.cancel(false);
                        cancelledCount++;
                    }
                }
                pendingEvents.clear();
            }
            if (cancelledCount > 0) {
                logger.info("Cancelled {} pending coalesced events during cleanup", cancelledCount);
            }

            // Shutdown the coalesce executor
            try {
                coalesceExecutor.shutdown();
                if (!coalesceExecutor.awaitTermination(2, TimeUnit.SECONDS)) {
                    List<Runnable> droppedTasks = coalesceExecutor.shutdownNow();
                    if (!droppedTasks.isEmpty()) {
                        logger.warn("Forcibly shut down coalesce executor with {} pending tasks", droppedTasks.size());
                    }
                }
            } catch (InterruptedException e) {
                coalesceExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }

            if (watchKeyMap != null) {
                watchKeyMap.clear();
            }
            if (registeredDirectories != null) {
                registeredDirectories.clear();
            }
            if (knownDirectories != null) {
                knownDirectories.clear();
            }
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The file that triggered the event")
        private final File file;

        @Schema(title = "The type of change that occurred (CREATE, UPDATE, DELETE)")
        private final ChangeType changeType;
    }
}
