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
import java.util.*;

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
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Downloads.Output> {

    @Schema(title = "The interval between checks")
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @Schema(
        title = "The directory to list"
    )
    @NotNull
    private Property<String> from;

    @Schema(
        title = "The destination directory in case off `MOVE` "
    )
    private Property<String> moveDirectory;

    @Schema(title = "Regex pattern to match file names")
    private Property<String> regExp;

    @Schema(title = "Include files in subdirectories")
    @Builder.Default
    private Property<Boolean> recursive = Property.ofValue(false);

    @Schema(
        title = "The action to do on downloaded files"
    )
    @Builder.Default
    private Property<Downloads.Action> action = Property.ofValue(Downloads.Action.NONE);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext triggerContext) throws Exception {
        RunContext runContext = conditionContext.getRunContext();

        String renderedFrom = runContext.render(this.from).as(String.class).orElseThrow();

        io.kestra.plugin.fs.local.List listTask = io.kestra.plugin.fs.local.List.builder()
            .id(io.kestra.plugin.fs.local.List.class.getSimpleName())
            .type(io.kestra.plugin.fs.local.List.class.getName())
            .from(Property.ofValue(renderedFrom))
            .regExp(this.regExp)
            .recursive(this.recursive)
            .build();

        io.kestra.plugin.fs.local.List.Output listOutput = listTask.run(runContext);

        java.util.List<File> downloadedFiles = listOutput
            .getFiles()
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

        Downloads.Action selectedAction = this.action != null ?
            runContext.render(this.action).as(Downloads.Action.class).orElse(Downloads.Action.NONE) :
            Downloads.Action.NONE;

        if (selectedAction != Downloads.Action.NONE) {
            Downloads.performAction(renderedFrom, selectedAction, this.moveDirectory, runContext);
        }

        return Optional.of(TriggerService.generateExecution(
            this,
            conditionContext,
            triggerContext,
            Downloads.Output.builder().files(downloadedFiles).build()
        ));
    }
}

