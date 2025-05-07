package io.kestra.plugin.fs.local;

import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
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
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Trigger.Output> {

    @Schema(title = "The interval between checks")
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @Schema(title = "Directory to watch (e.g. file:///tmp/)")
    @NotNull
    private Property<String> directory;

    @Schema(title = "Regex pattern to match file names")
    private Property<String> regExp;

    @Schema(title = "Include files in subdirectories")
    @Builder.Default
    private Property<Boolean> recursive = Property.of(false);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext triggerContext) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        String renderedDir = runContext.render(directory).as(String.class).orElseThrow();
        Path dirPath = Paths.get(URI.create(renderedDir));
        if (!Files.exists(dirPath) || !Files.isDirectory(dirPath)) {
            logger.warn("Directory does not exist or is not a directory: {}", dirPath);
            return Optional.empty();
        }

        String regex = runContext.render(this.regExp).as(String.class).orElse(".*");
        boolean isRecursive = runContext.render(this.recursive).as(Boolean.class).orElse(false);

        try (Stream<Path> stream = isRecursive ? Files.walk(dirPath) : Files.list(dirPath)) {
            java.util.List<Path> matchingFiles = stream
                .filter(Files::isRegularFile)
                .filter(p -> p.getFileName().toString().matches(regex))
                .toList();

            if (matchingFiles.isEmpty()) {
                return Optional.empty();
            }

            List task = List.builder()
                .from(Property.of(renderedDir))
                .recursive(Property.of(isRecursive))
                .build();

            List.Output output = task.run(runContext);

            java.util.List<File> files = output.getFiles().stream()
                .map(throwFunction(fileItem -> {
                    Download downloadTask = Download.builder()
                            .from(Property.of(fileItem.getLocalPath().getPath()))
                            .build();

                    Download.Output downloadOutput = null;
                    downloadOutput = downloadTask.run(runContext);
                    return fileItem.withUri(downloadOutput.getUri());
                })).toList();

            return Optional.of(TriggerService.generateExecution(
                this,
                conditionContext,
                triggerContext,
                Downloads.Output.builder().files(files).build()
            ));
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "List of detected files")
        private final java.util.List<File> files;
    }
}
