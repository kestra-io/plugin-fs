package io.kestra.plugin.fs.local;

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

import java.time.Duration;
import java.util.Optional;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger a flow as soon as new files are detected in a given local file system's directory."
)
@Plugin(
    examples = {
        @Example(
            title = "Wait for one or more files in a given local file system's directory.",
            full = true,
            code = """
                id: local_trigger_flow
                namespace: company.team

                triggers:
                  - id: watch
                    type: io.kestra.plugin.fs.local.Trigger
                    from: "/home/dev/kestra"
                    interval: PT10S
                    action: MOVE
                    moveDirectory: "/home/dev/archive/"
                """
        )
    }
)
public class Trigger extends AbstractTrigger
    implements PollingTriggerInterface, TriggerOutput<Downloads.Output> {

    @Schema(title = "The interval between checks")
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @Schema(title = "The directory to list")
    @NotNull
    private Property<String> from;

    @Schema(title = "The destination directory in case off `MOVE`")
    private Property<String> moveDirectory;

    @Schema(title = "Regex pattern to match file names")
    private Property<String> regExp;

    @Schema(title = "Include files in subdirectories")
    @Builder.Default
    private Property<Boolean> recursive = Property.ofValue(false);

    @Schema(title = "The action to do on downloaded files")
    @Builder.Default
    private Property<Downloads.Action> action = Property.ofValue(Downloads.Action.NONE);

    @Schema(title = "The maximum number of files to retrieve at once")
    private Property<Integer> maxFiles;

    @Override
    public Optional<Execution> evaluate(
        ConditionContext conditionContext,
        TriggerContext triggerContext
    ) throws Exception {

        RunContext runContext = conditionContext.getRunContext();

        String renderedFrom = runContext.render(this.from).as(String.class).orElseThrow();

        io.kestra.plugin.fs.local.List listTask = io.kestra.plugin.fs.local.List.builder()
            .id(io.kestra.plugin.fs.local.List.class.getSimpleName())
            .type(io.kestra.plugin.fs.local.List.class.getName())
            .from(Property.ofValue(renderedFrom))
            .regExp(this.regExp)
            .recursive(this.recursive)
            .maxFiles(this.maxFiles)
            .build();

        io.kestra.plugin.fs.local.List.Output listOutput = listTask.run(runContext);

        if (listOutput.getFiles().isEmpty()) {
            return Optional.empty();
        }

        Integer rMaxFiles = runContext.render(this.maxFiles).as(Integer.class).orElse(null);
        java.util.List<File> selectedFiles = listOutput.getFiles();

        if (rMaxFiles != null && selectedFiles.size() > rMaxFiles) {
            selectedFiles = selectedFiles.subList(0, rMaxFiles);
        }

        java.util.List<File> downloadedFiles = selectedFiles
            .stream()
            .map(throwFunction(fileItem -> {
                if (fileItem.isDirectory()) {
                    return fileItem;
                }

                Download downloadTask = Download.builder()
                    .id(Download.class.getSimpleName())
                    .type(Download.class.getName())
                    .from(Property.ofValue(fileItem.getLocalPath().toString()))
                    .build();

                Download.Output downloadOutput = downloadTask.run(runContext);

                return fileItem.withUri(downloadOutput.getUri());
            }))
            .toList();

        Downloads.Action selectedAction =
            runContext.render(this.action)
                .as(Downloads.Action.class)
                .orElse(Downloads.Action.NONE);

        java.util.List<File> filesToProcess = selectedFiles.stream()
            .filter(file -> !file.isDirectory())
            .toList();

        if (selectedAction != Downloads.Action.NONE) {
            Downloads.performAction(
                filesToProcess,
                selectedAction,
                this.moveDirectory,
                runContext
            );
        }

        return Optional.of(
            TriggerService.generateExecution(
                this,
                conditionContext,
                triggerContext,
                Downloads.Output.builder()
                    .files(downloadedFiles)
                    .build()
            )
        );
    }
}
