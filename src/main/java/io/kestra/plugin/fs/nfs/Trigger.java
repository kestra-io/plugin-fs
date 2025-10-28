package io.kestra.plugin.fs.nfs;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static io.kestra.core.models.triggers.StatefulTriggerService.*;
import static io.kestra.core.utils.Rethrow.throwFunction;



@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger a flow when files are detected or modified on an NFS mount."
)
@Plugin(
    examples = {
        @Example(
            title = "Trigger a flow when a new CSV file arrives in an NFS directory.",
            full = true,
            code = """
                id: nfs_listen
                namespace: company.team

                tasks:
                  - id: log_files
                    type: io.kestra.plugin.core.log.Log
                    message: "Received {{ trigger.files | length }} files: {% for file in trigger.files %}{{ file.file.uri }} ({{ file.changeType }}){% endfor %}"

                triggers:
                  - id: watch_nfs
                    type: io.kestra.plugin.fs.nfs.Trigger
                    from: /mnt/nfs_input/data
                    interval: PT10S
                    regExp: ".*\\.csv$"
                    on: CREATE
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Trigger.Output>, StatefulTriggerInterface {

    @Schema(title = "The directory path to watch.")
    @NotNull
    private Property<String> from;

    @Schema(title = "A regular expression to filter files.")
    private Property<String> regExp;

    @Schema(title = "Whether to list files recursively.")
    @Builder.Default
    private Boolean recursive = false;

    @Schema(title = "The interval between checks.")
    @Builder.Default
    private Duration interval = Duration.ofSeconds(60);

    @Schema(title = "When to trigger the flow (CREATE, UPDATE, or CREATE_OR_UPDATE).")
    @Builder.Default
    private Property<On> on = Property.ofValue(On.CREATE_OR_UPDATE);

    @Schema(title = "Unique key for storing the trigger state.")
    private Property<String> stateKey;

    @Schema(title = "Time-to-live for the trigger state.")
    private Property<Duration> stateTtl;

    @Override
    public Duration getInterval() {
        return this.interval;
    }

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext triggerContext) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        String rFrom = runContext.render(this.from).as(String.class).orElseThrow(() -> new IllegalArgumentException("`from` cannot be null or empty"));
        Path fromPath = NfsService.toNfsPath(rFrom);
        String rRegExp = runContext.render(this.regExp).as(String.class).orElse(null);
        On rOn = runContext.render(on).as(On.class).orElse(On.CREATE_OR_UPDATE);
        String rStateKey = runContext.render(stateKey).as(String.class)
            .orElse(StatefulTriggerService.defaultKey(triggerContext.getNamespace(), triggerContext.getFlowId(), id));
        Optional<Duration> rStateTtl = runContext.render(stateTtl).as(Duration.class);

        Map<String, StatefulTriggerService.Entry> state = readState(runContext, rStateKey, rStateTtl);
        List<TriggeredFile> toFire = new ArrayList<>();

        logger.debug("Evaluating trigger for path: {}", fromPath);

        try (Stream<Path> stream = this.recursive ? Files.walk(fromPath) : Files.list(fromPath)) {
            List<Path> paths = stream
                .filter(path -> !Files.isDirectory(path))
                .filter(path -> rRegExp == null || path.toString().matches(rRegExp))
                .toList();

            for(Path path: paths) {
                 try {
                     BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
                     var modifiedAt = attrs.lastModifiedTime().toInstant();
                     var key = path.toUri().toString();
                     var version = String.format("%d_%d", modifiedAt.toEpochMilli(), attrs.size());

                     var candidate = StatefulTriggerService.Entry.candidate(key, version, modifiedAt);
                     var change = computeAndUpdateState(state, candidate, rOn);

                     if (change.fire()) {
                         var changeType = change.isNew() ? ChangeType.CREATE : ChangeType.UPDATE;
                         
                         io.kestra.plugin.fs.nfs.List.File fileModel = mapToFile(path);
                         toFire.add(TriggeredFile.builder()
                             .file(fileModel)
                             .changeType(changeType)
                             .build());
                         logger.info("File {} detected with change type: {}", path, changeType);
                     }
                 } catch (IOException e) {
                     logger.warn("Error processing path {}: {}", path, e.getMessage(), e);
                 }
            }
        } catch (IOException e) {
            logger.error("Error walking/listing path {}: {}", fromPath, e.getMessage(), e);
            return Optional.empty();
        }

        writeState(runContext, rStateKey, state, rStateTtl);

        if (toFire.isEmpty()) {
            logger.debug("No new or updated files found.");
            return Optional.empty();
        }

        logger.info("Triggering execution for {} files.", toFire.size());
        Execution execution = TriggerService.generateExecution(this, conditionContext, triggerContext, Output.builder().files(toFire).build());
        return Optional.of(execution);
    }

     // Use Fully Qualified Name (FQN) for the return type
     private io.kestra.plugin.fs.nfs.List.File mapToFile(Path path) throws IOException {
        BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
                         
        return io.kestra.plugin.fs.nfs.List.File.builder()
            .name(path.getFileName().toString())
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
        private final List<TriggeredFile> files;
    }
}

