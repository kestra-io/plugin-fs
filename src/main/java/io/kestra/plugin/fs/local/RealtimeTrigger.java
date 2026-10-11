package io.kestra.plugin.fs.local;

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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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

        Note: On macOS, WatchService uses a polling-based fallback and may not detect events as quickly as on Linux or Windows.
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

    /**
     * Event filters supported by the trigger.
     * Each filter is mapped to one or more WatchService event kinds:
     * CREATE          → ENTRY_CREATE
     * UPDATE          → ENTRY_MODIFY
     * CREATE_OR_UPDATE → ENTRY_CREATE and ENTRY_MODIFY
     * DELETE          → ENTRY_DELETE
     */
    public enum EventType {
        CREATE,
        UPDATE,
        CREATE_OR_UPDATE,
        DELETE
    }

    /**
     * Represents the type of filesystem change that occurred.
     */
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

    // Runtime state fields
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    @Getter(AccessLevel.NONE)
    private transient final AtomicBoolean active = new AtomicBoolean(false);

    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    @Getter(AccessLevel.NONE)
    private transient final AtomicBoolean ready = new AtomicBoolean(false);

    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    @Getter(AccessLevel.NONE)
    private transient final AtomicReference<WatchService> watchService = new AtomicReference<>();

    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    @Getter(AccessLevel.NONE)
    private transient Logger logger;

    /**
     * Package-private readiness check for testing.
     * Returns true when the trigger is active and has successfully initialized the watcher.
     */
    boolean isReady() {
        return active.get() && ready.get();
    }

    /**
     * Retrieves allowed paths from plugin configuration.
     * Matches AbstractLocalTask.allowedPaths() for consistent security policy.
     * Throws SecurityException if not configured (required for all environments).
     */
    private List<String> getAllowedPaths(RunContext runContext) {
        Optional<List<String>> allowedPathConfig = runContext.pluginConfiguration(AbstractLocalTask.ALLOWED_PATHS);

        if (allowedPathConfig.isEmpty() || allowedPathConfig.get().isEmpty()) {
            throw new SecurityException(
                "The 'allowed-paths' configuration is required to enable access to the local filesystem. " +
                "You must define at least one allowed path in the plugin configuration, `kestra.plugins.configurations`. " +
                "Refer to the example in the plugin documentation."
            );
        }

        return allowedPathConfig.get();
    }

    /**
     * Validates that a path is within the configured allowed-paths.
     * Matches AbstractLocalTask.validatePath() for consistent security policy.
     * Throws SecurityException if the path is not allowed or if allowed-paths is not configured.
     */
    private void validatePath(Path path, RunContext runContext) {
        List<String> renderedAllowedPaths = getAllowedPaths(runContext);

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

        List<Path> normalizedAllowedPaths = renderedAllowedPaths.stream()
            .map(allowed -> {
                try {
                    return Paths.get(allowed).toRealPath();
                } catch (IOException e) {
                    return Paths.get(allowed).toAbsolutePath().normalize();
                }
            })
            .toList();

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
        this.logger = runContext.logger();

        return Flux.<Execution>create(emitter -> {
            Map<WatchKey, Path> watchKeyMap = new ConcurrentHashMap<>();
            Set<Path> registeredDirectories = Collections.newSetFromMap(new ConcurrentHashMap<>());
            Set<Path> knownDirectories = Collections.newSetFromMap(new ConcurrentHashMap<>());
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

                // Step 1: Render and validate the `from` property
                String renderedFrom = runContext.render(this.from).as(String.class)
                    .orElseThrow(() -> new IllegalArgumentException("`from` is required"));
                Path rootDirectory = Paths.get(renderedFrom).toAbsolutePath().normalize();

                logger.info("RealtimeTrigger starting for directory: {}", rootDirectory);

                // Step 2: Render and compile regex pattern BEFORE creating resources (fail-fast)
                EventType renderedOn = runContext.render(this.on).as(EventType.class)
                    .orElse(EventType.CREATE_OR_UPDATE);
                Pattern compiledPattern = null;
                if (this.regExp != null) {
                    String renderedRegExp = runContext.render(this.regExp).as(String.class).orElse(null);
                    if (renderedRegExp != null) {
                        try {
                            compiledPattern = java.util.regex.Pattern.compile(renderedRegExp);
                        } catch (java.util.regex.PatternSyntaxException e) {
                            throw new IllegalArgumentException(
                                "Invalid regex pattern in 'regExp': " + renderedRegExp + " - " + e.getMessage(), e
                            );
                        }
                    }
                }

                boolean rRecursive = runContext.render(this.recursive)
                    .as(Boolean.class)
                    .orElse(false);

                // Step 3: Validate root directory against allowed-paths configuration
                validatePath(rootDirectory, runContext);

                if (!Files.exists(rootDirectory)) {
                    throw new IllegalArgumentException("Directory does not exist: " + rootDirectory);
                }
                if (!Files.isDirectory(rootDirectory)) {
                    throw new IllegalArgumentException("Path is not a directory: " + rootDirectory);
                }

                // Step 4: Check cancellation before creating resources
                if (!active.get()) {
                    emitter.complete();
                    return;
                }

                logger.info("Watching directory: {}", rootDirectory);

                // Step 5: Create WatchService
                createWatchService();

                // Check cancellation after resource creation
                if (!active.get()) {
                    emitter.complete();
                    return;
                }

                // Step 6: Register root directory
                registerDirectory(rootDirectory, watchKeyMap, registeredDirectories);
                knownDirectories.add(rootDirectory.toAbsolutePath().normalize());

                if (rRecursive) {
                    registerDirectoryTree(
                        rootDirectory,
                        watchKeyMap,
                        registeredDirectories,
                        knownDirectories,
                        runContext
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

                ready.set(true);

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
                        key = service.take();
                    } catch (ClosedWatchServiceException e) {
                        // Normal shutdown: stop() closed the WatchService, so take() unblocked.
                        break;
                    } catch (InterruptedException e) {
                        // Thread interrupted, but check active flag before assuming shutdown.
                        if (!active.get()) {
                            break;
                        }
                        // If still active, it's an unexpected interrupt; continue watching.
                        continue;
                    }

                    // Get the directory associated with this WatchKey
                    Path directory = watchKeyMap.get(key);
                    if (directory == null) {
                        // Race condition: the key was removed or map was cleared during shutdown.
                        // Safely discard this key without trying to reset it.
                        continue;
                    }

                    // Process all events for this key
                    for (WatchEvent<?> event : key.pollEvents()) {
                        try {
                            WatchEvent.Kind<?> kind = event.kind();

                            if (kind == StandardWatchEventKinds.OVERFLOW) {
                                logger.warn("WatchService overflow event; some filesystem events may have been missed");
                                continue;
                            }

                            Path eventPath = directory.resolve((Path) event.context());
                            Path normalizedEventPath = eventPath.toAbsolutePath().normalize();

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
                                    validatePath(eventPath, runContext);

                                    registerDirectory(eventPath, watchKeyMap, registeredDirectories);
                                    knownDirectories.add(eventPath.toAbsolutePath().normalize());

                                    registerDirectoryTree(
                                        eventPath,
                                        watchKeyMap,
                                        registeredDirectories,
                                        knownDirectories,
                                        runContext
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

                            if (!shouldTrigger(kind, renderedOn)) {
                                continue;
                            }

                            if (compiledPattern != null && !matchesRegExp(eventPath, compiledPattern)) {
                                continue;
                            }

                            if (Files.isSymbolicLink(eventPath)) {
                                logger.warn("Security: Rejecting symbolic link event: {}", eventPath);
                                continue;
                            }

                            // Security: Validate event path against allowed-paths before emission
                            try {
                                validatePath(eventPath, runContext);
                            } catch (SecurityException | IllegalArgumentException e) {
                                logger.warn(
                                    "Security: Rejecting event for path outside allowed-paths: {} - {}",
                                    eventPath,
                                    e.getMessage()
                                );
                                continue;
                            }

                            // Process event based on type
                            ChangeType changeType = null;
                            if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                                changeType = ChangeType.CREATE;
                            } else if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {
                                changeType = ChangeType.UPDATE;
                            } else if (kind == StandardWatchEventKinds.ENTRY_DELETE) {
                                changeType = ChangeType.DELETE;
                            }

                            if (changeType != null) {
                                emitEventExecution(eventPath, changeType, conditionContext, context, emitter);
                            }

                        } catch (Exception e) {
                            // An error processing this individual event should not terminate the entire watcher.
                            logger.warn("Error processing filesystem event: {}", e.getMessage(), e);
                            // Continue to the next event
                        }
                    }

                    // Reset the key for future events
                    boolean valid = key.reset();
                    if (!valid) {
                        // Key is no longer valid (directory deleted, etc.)
                        Path registeredDirectory = watchKeyMap.remove(key);
                        if (registeredDirectory != null) {
                            Path normalizedDirectory = registeredDirectory.toAbsolutePath().normalize();

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
                cleanup(watchKeyMap, registeredDirectories, knownDirectories);
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
        FluxSink<Execution> emitter
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

            Output output = Output.builder()
                .file(file)
                .changeType(changeType)
                .build();

            Execution execution = TriggerService.generateRealtimeExecution(
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

    /**
     * Tests whether a file path matches the compiled regex pattern.
     * The pattern is applied against the full absolute path of the file.
     *
     * @param path the file path to test
     * @param pattern the compiled regex pattern (may be null)
     * @return true if the pattern is null (no filter) or the path matches; false otherwise
     */
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
                    WatchKey key = entry.getKey();
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

            WatchKey key = normalizedDirectory.register(
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
     * @param runContext the current run context used for path validation
     * @throws Exception if the directory stream cannot be opened or an unrecoverable
     *                    registration error occurs
     */
    private void registerDirectoryTree(
        Path root,
        Map<WatchKey, Path> map,
        Set<Path> dirs,
        Set<Path> knownDirs,
        RunContext runContext
    ) throws Exception {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(root)) {
            for (Path entry : stream) {
                if (!Files.isDirectory(entry) || Files.isSymbolicLink(entry)) {
                    continue;
                }

                try {
                    validatePath(entry, runContext);

                    registerDirectory(entry, map, dirs);
                    knownDirs.add(entry.toAbsolutePath().normalize());

                    registerDirectoryTree(
                        entry,
                        map,
                        dirs,
                        knownDirs,
                        runContext
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

    @Override
    public void stop() {
        if (active.compareAndSet(true, false)) {
            // Unblock the event loop by closing the WatchService.
            // The evaluate() thread will perform final cleanup in its finally block.
            closeWatchService();
        }
    }

    @Override
    public void kill() {
        if (logger != null) {
            logger.debug("RealtimeTrigger kill signal received");
        }
        stop();
    }

    /**
     * Closes the WatchService to unblock the event loop.
     * Uses getAndSet(null) to atomically claim ownership.
     * Only the thread that successfully gets a non-null reference will close it.
     */
    private void closeWatchService() {
        WatchService svc = watchService.getAndSet(null);
        if (svc == null) {
            return;
        }

        try {
            svc.close();
            logger.info("RealtimeTrigger WatchService closed");
        } catch (Exception e) {
            logger.warn("Error closing WatchService: {}", e.getMessage());
        }
    }

    private void cleanup(
        Map<WatchKey, Path> watchKeyMap,
        Set<Path> registeredDirectories,
        Set<Path> knownDirectories
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
            ready.set(false);

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
