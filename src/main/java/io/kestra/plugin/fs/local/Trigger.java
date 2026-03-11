package io.kestra.plugin.fs.local;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.fs.local.models.File;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;

import static io.kestra.core.models.triggers.StatefulTriggerService.*;
import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger on new local files",
    description = """
        Polls a directory under configured `allowed-paths` (default every 60s) and fires when files are created or updated.
        Limits to `maxFiles` (default 25) and can MOVE or DELETE matched files; `moveDirectory` is required when MOVE is chosen.

        Example (Kestra config):
        ```yaml
        plugins:
          configurations:
            - type: io.kestra.plugin.fs.local
              values:
                allowed-paths:
                  - /data/files
        ```
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Wait for one or more files in a given local file system's directory and process each of these files sequentially.",
            full = true,
            code = """
                id: local_trigger_flow
                namespace: company.team

                tasks:
                  - id: for_each_file
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.files }}"
                    tasks:
                      - id: return
                        type: io.kestra.plugin.core.debug.Return
                        format: "{{ taskrun.value | jq('.path') }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.fs.local.Trigger
                    from: "/home/dev/kestra"
                    interval: PT10S
                    action: MOVE
                    recursive: true
                    moveDirectory: "/home/dev/archive/"
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<List.Output>, StatefulTriggerInterface {

    @Schema(title = "Interval between checks")
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @Schema(
        title = "Directory to watch"
    )
    @NotNull
    private Property<String> from;

    @Schema(
        title = "Destination directory when action is MOVE"
    )
    private Property<String> moveDirectory;

    @Schema(title = "Regex pattern to match file names")
    private Property<String> regExp;

    @Schema(title = "Include files in subdirectories")
    @Builder.Default
    private Property<Boolean> recursive = Property.ofValue(false);

    @Schema(
        title = "Action to take on matched files"
    )
    @Builder.Default
    private Property<Downloads.Action> action = Property.ofValue(Downloads.Action.NONE);

    @Builder.Default private final Property<On> on = Property.ofValue(On.CREATE_OR_UPDATE);

    private Property<String> stateKey;

    private Property<Duration> stateTtl;

    @Builder.Default
    @Schema(
        title = "Maximum files to process per poll"
    )
    private Property<Integer> maxFiles = Property.ofValue(25);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext triggerContext) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        var logger = runContext.logger();
        var rOn = runContext.render(on).as(On.class).orElse(On.CREATE_OR_UPDATE);
        var rStateKey = runContext.render(stateKey).as(String.class).orElse(StatefulTriggerService.defaultKey(triggerContext.getNamespace(), triggerContext.getFlowId(), id));
        var rStateTtl = runContext.render(stateTtl).as(Duration.class);
        var rFrom = runContext.render(this.from).as(String.class).orElseThrow();

        io.kestra.plugin.fs.local.List listTask = io.kestra.plugin.fs.local.List.builder()
            .id(io.kestra.plugin.fs.local.List.class.getSimpleName())
            .type(io.kestra.plugin.fs.local.List.class.getName())
            .from(Property.ofValue(rFrom))
            .regExp(this.regExp)
            .recursive(this.recursive)
            .maxFiles(this.maxFiles)
            .build();

        io.kestra.plugin.fs.local.List.Output listOutput = listTask.run(runContext);

        if (listOutput.getFiles().isEmpty()) {
            return Optional.empty();
        }

        Map<String, StatefulTriggerService.Entry> state = readState(runContext, rStateKey, rStateTtl);

        java.util.List<File> actionFiles = new ArrayList<>();

        java.util.List<TriggeredFile> toFire = listOutput.getFiles().stream()
            .flatMap(throwFunction(fileItem -> {
                if (fileItem.isDirectory()) {
                    return Stream.empty();
                }

                var uri = Optional.ofNullable(fileItem.getUri().toString()).orElse(fileItem.getLocalPath().toUri().toString());
                var attrs = Files.readAttributes(fileItem.getLocalPath(), BasicFileAttributes.class);
                var modifiedAt = attrs.lastModifiedTime().toInstant();
                var key = Optional.ofNullable(attrs.fileKey()).map(Object::toString).orElseGet(() -> fileItem.getLocalPath().toUri().toString());
                var version = String.format("%d_%s", modifiedAt.toEpochMilli(), uri);

                var candidate = StatefulTriggerService.Entry.candidate(key, version, modifiedAt);

                var change = computeAndUpdateState(state, candidate, rOn);

                if (change.fire()) {
                    var changeType = change.isNew() ? ChangeType.CREATE : ChangeType.UPDATE;

                    var downloadTask = Download.builder()
                        .id(Download.class.getSimpleName())
                        .type(Download.class.getName())
                        .from(Property.ofValue(fileItem.getLocalPath().toString()))
                        .build();

                    var downloadOutput = downloadTask.run(runContext);
                    var downloaded = fileItem.withUri(downloadOutput.getUri());

                    actionFiles.add(fileItem);

                    return Stream.of(TriggeredFile.builder()
                        .file(downloaded)
                        .changeType(changeType)
                        .build());
                }
                return Stream.empty();
            }))
            .toList();

        writeState(runContext, rStateKey, state, rStateTtl);

        if (toFire.isEmpty()) {
            return Optional.empty();
        }

        int rMaxFiles = runContext.render(this.maxFiles).as(Integer.class).orElse(25);
        java.util.List<TriggeredFile> limitedToFire = toFire;
        java.util.List<File> limitedActionFiles = actionFiles;
        if (toFire.size() > rMaxFiles) {
            logger.warn("Too many files to process ({}), limiting to {}", toFire.size(), rMaxFiles);
            int limit = Math.min(rMaxFiles, toFire.size());
            limitedToFire = toFire.subList(0, limit);
            limitedActionFiles = actionFiles.subList(0, Math.min(limit, actionFiles.size()));
        }

        if (limitedToFire.isEmpty()) {
            return Optional.empty();
        }

        Downloads.Action selectedAction = this.action != null ?
            runContext.render(this.action).as(Downloads.Action.class).orElse(Downloads.Action.NONE) :
            Downloads.Action.NONE;

        java.util.List<File> filesToProcess = limitedActionFiles.stream()
            .filter(file -> !file.isDirectory())
            .toList();

        if (selectedAction != Downloads.Action.NONE) {
            Downloads.performAction(filesToProcess, selectedAction, this.moveDirectory, runContext);
        }

        return Optional.of(TriggerService.generateExecution(this, conditionContext, triggerContext, Output.builder().files(limitedToFire).build()));
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
        private final File file;
        private final ChangeType changeType;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "List of files that triggered the flow, each with its change type.")
        private final java.util.List<TriggeredFile> files;
    }

}
