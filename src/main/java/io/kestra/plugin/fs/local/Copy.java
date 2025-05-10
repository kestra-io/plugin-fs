package io.kestra.plugin.fs.local;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
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
import java.util.stream.Stream;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Copy a file within the local filesystem",
            code = """
                id: copy_file
                namespace: company.team

                tasks:
                  - id: copy
                    type: io.kestra.plugin.fs.local.Copy
                    from: "input/data.csv"
                    to: "backup/data.csv"
                    basePath: "/Users/malay/desktop/kestra-output"
                    overwrite: true
                """
        )
    }
)
public class Copy extends AbstractLocalTask implements RunnableTask<VoidOutput> {

    @Schema(
        title = "The file or directory to move from local file system."
    )
    @NotNull
    private Property<String> from;

    @Schema(
        title = "The path to move the file or directory to on the local file system."
    )
    @NotNull
    private Property<String> to;

    @Schema(
        title = "Overwrite.",
        description = "If set to false, it will raise an exception if the destination folder or file already exists."
    )
    @Builder.Default
    protected Property<Boolean> overwrite = Property.of(false);

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
            copyDirectory(sourcePath, targetPath, shouldOverwrite, runContext);
        } else {
            copyFile(sourcePath, targetPath, shouldOverwrite, runContext);
        }

        return null;
    }

    private void copyFile(Path source, Path target, boolean overwrite, RunContext runContext) throws IOException {
        if (Files.isDirectory(target)) {
            target = target.resolve(source.getFileName());
        }

        if (Files.exists(target) && !overwrite) {
            runContext.logger().warn("Target file already exists: {}. Configure 'overwrite: true' to replace it.", target);
            throw new IllegalArgumentException("Target file already exists: " + target);
        }

        Files.createDirectories(target.getParent());

        Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
        runContext.logger().info("Copied file from '{}' to '{}'", source, target);
    }

    private void copyDirectory(Path sourceDir, Path targetDir, boolean overwrite, RunContext runContext) throws IOException {
        if (!Files.exists(targetDir)) {
            Files.createDirectories(targetDir);
        } else if (!Files.isDirectory(targetDir)) {
            throw new IllegalArgumentException("Cannot copy a directory to a file: " + targetDir);
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
                            runContext.logger().debug("Copied '{}' to '{}'", source, target);
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Error copying " + source + ": " + e.getMessage(), e);
                }
            });
        }
        runContext.logger().info("Copied directory from '{}' to '{}'", sourceDir, targetDir);
    }
}
