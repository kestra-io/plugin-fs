package io.kestra.plugin.fs.nfs;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.kestra.core.models.triggers.StatefulTriggerService.*;
import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger a flow when files are created or updated on an NFS mount."
)
@Plugin(
    examples = {
        @Example(
            title = "Trigger a flow when a new CSV file is detected in an NFS directory.",
            full = true,
            code = """
                id: nfs_trigger_flow
                namespace: dev
                
                tasks:
                  - id: log_files
                    type: io.kestra.plugin.core.log.Log
                    message: "Received {{ trigger.files | length }} files: {{ trigger.files | jq('.[].uri') }}"
                
                triggers:
                  - id: nfs_watch
                    type: io.kestra.plugin.fs.nfs.Trigger
                    from: /mnt/nfs/local_test/trigger_in
                    interval: PT10S
                    on: CREATE
                    regExp: ".*\\.csv$"
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Trigger.Output>, StatefulTriggerInterface {

    @Schema(title = "The interval between checks.")
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @Schema(title = "The directory path to list from.")
    @PluginProperty(dynamic = true)
    @NotNull
    private String from;

    @Schema(title = "A regular expression to filter files.")
    @PluginProperty(dynamic = true)
    private String regExp;

    @Schema(title = "Whether to list files recursively.")
    @PluginProperty
    @Builder.Default
    private Boolean recursive = false;

    @Schema(title = "Which change type to listen for.")
    @Builder.Default
    private final Property<On> on = Property.ofValue(On.CREATE_OR_UPDATE);

    @Schema(
        title = "A unique key to track the trigger state.",
        description = "If not set, it defaults to a key generated from the flow and trigger IDs."
    )
    @PluginProperty(dynamic = true)
    private Property<String> stateKey;

    @Schema(
        title = "The Time-to-Live (TTL) for the trigger state.",
        description = "This duration controls how long the trigger's memory of processed files is retained. " +
                      "If a file is not seen again within this TTL, it will be considered new upon its next appearance. " +
                      "This is useful for handling file retention policies or temporary files."
    )
    @PluginProperty(dynamic = true)
    private Property<Duration> stateTtl;

    @Override
    public Duration getInterval() {
        return this.interval;
    }

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext triggerContext) throws Exception {
        RunContext runContext = conditionContext.getRunContext();

        String renderedFrom = runContext.render(this.from);
        Path fromPath = NfsService.toNfsPath(renderedFrom);
        String renderedRegExp = runContext.render(this.regExp);
        var rOn = runContext.render(on).as(On.class).orElse(On.CREATE_OR_UPDATE);
        var rStateKey = runContext.render(stateKey).as(String.class).orElse(StatefulTriggerService.defaultKey(triggerContext.getNamespace(), triggerContext.getFlowId(), id));
        var rStateTtl = runContext.render(stateTtl).as(Duration.class);


        Map<String, Entry> state = readState(runContext, rStateKey, rStateTtl);
        List<TriggeredFile> toFire = new ArrayList<>();

        try (Stream<Path> stream = this.recursive ? Files.walk(fromPath) : Files.list(fromPath)) {
            List<Path> files = stream
                .filter(path -> renderedRegExp == null || path.toString().matches(renderedRegExp))
                .filter(path -> !Files.isDirectory(path)) // Only trigger on files, not directories
                .collect(Collectors.toList());


            for (Path path : files) {
                BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
                Instant modifiedAt = attrs.lastModifiedTime().toInstant();
                String key = path.toUri().toString();
                String version = String.format("%d_%s", modifiedAt.toEpochMilli(), key);

                var candidate = Entry.candidate(key, version, modifiedAt);
                var change = computeAndUpdateState(state, candidate, rOn);

                if (change.fire()) {
                    var changeType = change.isNew() ? ChangeType.CREATE : ChangeType.UPDATE;
                    toFire.add(TriggeredFile.builder()
                        .file(this.map(path))
                        .changeType(changeType)
                        .build());
                }
            }
        }

        writeState(runContext, rStateKey, state, rStateTtl);

        if (toFire.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(TriggerService.generateExecution(this, conditionContext, triggerContext, Output.builder().files(toFire).build()));
    }

    private io.kestra.plugin.fs.nfs.List.File map(Path path) throws IOException {
        BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
        return io.kestra.plugin.fs.nfs.List.File.builder()
            .name(path.getFileName().toString())
            .localPath(path)
            .uri(path.toUri())
            .isDirectory(attrs.isDirectory())
            .isSymbolicLink(attrs.isSymbolicLink())
            .isHidden(Files.isHidden(path))
            .creationTime(attrs.creationTime().toInstant())
            .lastAccessTime(attrs.lastAccessTime().toInstant())
            .lastModifiedTime(attrs.lastModifiedTime().toInstant())
            .size(attrs.size())
            .build();
    }

    public enum ChangeType {
        CREATE,
        UPDATE
    }

    @Getter
    @AllArgsConstructor
    @Builder
    public static class TriggeredFile {
        @JsonUnwrapped
        private final io.kestra.plugin.fs.nfs.List.File file;
        private final ChangeType changeType;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "List of files that triggered the flow, each with its change type.")
        private final java.util.List<TriggeredFile> files;
    }
}


