package io.kestra.plugin.fs.local;

import io.kestra.core.exceptions.KestraRuntimeException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;

import java.io.IOException;
import java.nio.file.*;
import java.util.Comparator;
import java.util.stream.Stream;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Move or rename local files",
    description = """
        Moves files or directories within configured `allowed-paths`; destination parents are created as needed.
        Overwrite is false by default and directories cannot be moved into themselves.
        Local access requires `allowed-paths` in plugin defaults.

        Example (Kestra config):
        ```yaml
        plugins:
          configurations:
            - type: io.kestra.plugin.fs.local.Move
              values:
                allowed-paths:
                  - /data/files
        ```
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Move a file within the local filesystem",
            code = """
                id: move_file
                namespace: company.team

                tasks:
                  - id: move
                    type: io.kestra.plugin.fs.local.Move
                    from: /input/data.csv
                    to: /archive/data.csv
                    overwrite: true
                """
        )
    }
)
public class Move extends AbstractLocalTask implements RunnableTask<VoidOutput> {
    @Schema(
        title = "Source file or directory path"
    )
    @NotNull
    private Property<String> from;

    @Schema(
        title = "Destination path on the local file system"
    )
    @NotNull
    private Property<String> to;

    @Schema(
        title = "Overwrite existing files",
        description = "If false, the task fails when the destination already exists."
    )
    @Builder.Default
    protected Property<Boolean> overwrite = Property.ofValue(false);

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        String renderedFrom = runContext.render(this.from).as(String.class).orElseThrow();
        String renderedTo = runContext.render(this.to).as(String.class).orElseThrow();
        boolean shouldOverwrite = runContext.render(overwrite).as(Boolean.class).orElse(false);

        Path sourcePath = resolveLocalPath(renderedFrom, runContext);
        Path targetPath = resolveLocalPath(renderedTo, runContext);

        if (!Files.exists(sourcePath)) {
            throw new IllegalArgumentException("Source path does not exist: " + sourcePath);
        }

        if (Files.isDirectory(sourcePath)) {
            moveDirectory(sourcePath, targetPath, shouldOverwrite, runContext);
        } else {
            moveFile(sourcePath, targetPath, shouldOverwrite, runContext);
        }

        return null;
    }

    private void moveFile(Path source, Path target, boolean overwrite, RunContext runContext) throws IOException {
        Path finalTarget;

        if ((Files.exists(target) && Files.isDirectory(target)) || target.toString().endsWith(FileSystems.getDefault().getSeparator())) {
            Files.createDirectories(target);
            finalTarget = target.resolve(source.getFileName());
        } else {
            finalTarget = target;
        }

        if (Files.exists(finalTarget) && !overwrite) {
            throw new KestraRuntimeException(String.format(
                "Target file already exists : %s.\nSet 'overwrite: true' to replace the existing file.", finalTarget));
        }

        Files.createDirectories(finalTarget.getParent());

        try {
            Files.move(source, finalTarget, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(source, finalTarget, StandardCopyOption.REPLACE_EXISTING);
        }

        runContext.logger().info("Moved file from '{}' to '{}'", source, finalTarget);
    }

    private void moveDirectory(Path sourceDir, Path targetDir, boolean overwrite, RunContext runContext) throws IOException {
        if (!Files.exists(targetDir)) {
            try {
                Files.createDirectories(targetDir.getParent());
                Files.move(sourceDir, targetDir, StandardCopyOption.ATOMIC_MOVE);
                runContext.logger().info("Moved directory from '{}' to '{}'", sourceDir, targetDir);
                return;
            } catch (AtomicMoveNotSupportedException e) {
                Files.createDirectories(targetDir);
            }
        } else {
            if (!Files.isDirectory(targetDir)) {
                throw new IllegalArgumentException("Cannot move a directory to a file: " + targetDir);
            }

            if (sourceDir.equals(targetDir) || targetDir.startsWith(sourceDir)) {
                throw new IllegalArgumentException("Cannot move a directory into itself or its subdirectory");
            }
        }

        try (Stream<Path> paths = Files.walk(sourceDir)) {
            paths.forEach(source -> {
                try {
                    Path relativePath = sourceDir.relativize(source);
                    Path target = targetDir.resolve(relativePath);

                    if (Files.isDirectory(source)) {
                        if (!Files.exists(target)) {
                            Files.createDirectories(target);
                        }
                    } else {
                        if (Files.exists(target) && !overwrite) {
                            runContext.logger().warn("Target file already exists. Skipping existing file: {}", target);
                        } else {
                            if (!Files.exists(target.getParent())) {
                                Files.createDirectories(target.getParent());
                            }
                            Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
                            Files.delete(source);
                            runContext.logger().debug("Moved '{}' to '{}'", source, target);
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Error moving " + source + ": " + e.getMessage(), e);
                }
            });
        }

        try (Stream<Path> pathsToDelete = Files.walk(sourceDir).sorted(Comparator.reverseOrder())) {
            pathsToDelete.forEach(path -> {
                try {
                    Files.delete(path);
                } catch (IOException e) {
                    runContext.logger().warn("Failed to delete {}: {}", path, e.getMessage());
                }
            });
        }

        runContext.logger().info("Moved directory from '{}' to '{}'", sourceDir, targetDir);
    }
}
