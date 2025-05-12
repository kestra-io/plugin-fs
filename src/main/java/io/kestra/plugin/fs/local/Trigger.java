package io.kestra.plugin.fs.local;

import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.FileUtils;
import io.kestra.plugin.fs.local.models.File;
import io.kestra.plugin.fs.vfs.VfsService;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.FileType;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
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
    private Property<Boolean> recursive = Property.of(false);

    @Schema(
        title = "The action to do on downloaded files"
    )
    @Builder.Default
    private Property<Downloads.Action> action = Property.of(Downloads.Action.NONE);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext triggerContext) throws Exception {
        RunContext runContext = conditionContext.getRunContext();

        String renderedFrom = runContext.render(this.from).as(String.class).orElseThrow();

        io.kestra.plugin.fs.local.List listTask = io.kestra.plugin.fs.local.List.builder()
            .id(io.kestra.plugin.fs.local.List.class.getSimpleName())
            .type(io.kestra.plugin.fs.local.List.class.getName())
            .from(Property.of(renderedFrom))
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

                Path sourcePath = resolveLocalPath(fileItem.getLocalPath().toString(), runContext);
                if (!Files.exists(sourcePath)) {
                    throw new IOException("Source file '" + sourcePath + "' does not exist");
                }

                java.io.File tempFile = runContext.workingDir().createTempFile(FileUtils.getExtension(fileItem.getName())).toFile();
                Files.copy(sourcePath, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                URI storageUri = runContext.storage().putFile(tempFile);

                return fileItem.withUri(storageUri);
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

