package io.kestra.plugin.fs.local;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.NoSuchElementException;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete a file or directory from the local filesystem.",
    description = """
        Local filesystem access is disabled by default.
        You must configure the plugin default `allowed-paths` in your Kestra configuration.

        Example (Kestra config):
        ```yaml
        plugins:
          configurations:
            - type: io.kestra.plugin.fs.local.Delete
              values:
                allowed-paths:
                  - /data/files
        ```
        """
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: fs_local_delete
                namespace: company.team

                tasks:
                  - id: delete
                    type: io.kestra.plugin.fs.local.Delete
                    from: /data/uploads/file.txt
                """
        )
    }
)
public class Delete extends AbstractLocalTask implements RunnableTask<Delete.Output> {

    @Schema(title = "The local file path to delete")
    @NotNull
    private Property<String> from;

    @Schema(title = "Raise an error if the file is not found")
    @Builder.Default
    private Property<Boolean> errorOnMissing = Property.ofValue(false);

    @Schema(
        title = "Whether to include subdirectories",
        description = "If true, the task will recursively delete files in all subdirectories."
    )
    @Builder.Default
    private Property<Boolean> recursive = Property.ofValue(true);

    @Override
    public Output run(RunContext runContext) throws Exception {

        String renderedFrom = runContext.render(this.from).as(String.class).orElseThrow();

        Path sourcePath = resolveLocalPath(renderedFrom, runContext);

        if (!Files.exists(sourcePath)) {
            if (runContext.render(this.errorOnMissing).as(Boolean.class).orElse(true)) {
                throw new NoSuchElementException("File does not exist '" + sourcePath +
                    "'. To avoid this error, configure `errorOnMissing: false` in your configuration.");
            }

            runContext.logger().debug("File doesn't exist '{}'", sourcePath);

            return Output.builder()
                .deleted(false)
                .build();
        }

        Output.OutputBuilder outputBuilder = Output.builder();

        if (Boolean.TRUE.equals(runContext.render(this.recursive).as(Boolean.class).orElse(false))
            && Files.isDirectory(sourcePath)) {
            outputBuilder.deleted(deleteDirectoryRecursively(sourcePath));

            runContext.logger().debug("Deleted directory '{}'", sourcePath);
        } else {
           outputBuilder.deleted(Files.deleteIfExists(sourcePath));
           runContext.logger().debug("Deleted file '{}'", sourcePath);
        }

        return outputBuilder
            .build();
    }

    private boolean deleteDirectoryRecursively(Path dir) {
        try {
            Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Whether the file was deleted")
        private final boolean deleted;
    }
}