package io.kestra.plugin.fs.local;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete local file or directory",
    description = """
        Removes a file or directory under the configured `allowed-paths`; access outside is denied. Recursive deletion defaults to true for directories.
        Set `errorOnMissing: true` to fail when the target is absent.

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

    @Schema(title = "Local path to delete")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> from;

    @Schema(title = "Raise an error if missing")
    @Builder.Default
    @PluginProperty(group = "reliability")
    private Property<Boolean> errorOnMissing = Property.ofValue(false);

    @Schema(
        title = "Include subdirectories",
        description = "If true, deletes directory contents recursively."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> recursive = Property.ofValue(false);

    @Schema(
        title = "A regular expression to filter files for deletion",
        description = """
            When set, `from` must point to a directory. The pattern is matched against the absolute OS path \
            of each file (for example, `/data/uploads/file.csv`). Only regular files are deleted; directories \
            are skipped.
            """
    )
    @PluginProperty(group = "advanced")
    private Property<String> regExp;
<<<<<<< HEAD
=======
>>>>>>> Stashed changes
>>>>>>> 4f9b401 (fix: v2 compatibility)

    @Override
    public Output run(RunContext runContext) throws Exception {

        var rFrom = runContext.render(this.from).as(String.class).orElseThrow();
        var sourcePath = resolveLocalPath(rFrom, runContext);
        var rRegExp = runContext.render(this.regExp).as(String.class).orElse(null);

        if (rRegExp != null) {
            return deleteMatching(runContext, sourcePath, rRegExp);
        }

        if (!Files.exists(sourcePath)) {
            if (runContext.render(this.errorOnMissing).as(Boolean.class).orElse(false)) {
                throw new NoSuchElementException("File does not exist '" + sourcePath +
                    "'. To avoid this error, configure `errorOnMissing: false` in your configuration.");
            }

            runContext.logger().debug("File doesn't exist '{}'", sourcePath);

            return Output.builder()
                .uri(sourcePath.toUri())
                .deleted(false)
                .uris(List.of())
                .build();
        }

        var outputBuilder = Output.builder().uri(sourcePath.toUri());

        if (Boolean.TRUE.equals(runContext.render(this.recursive).as(Boolean.class).orElse(false))
            && Files.isDirectory(sourcePath)) {
            outputBuilder.deleted(deleteDirectoryRecursively(sourcePath));

            runContext.logger().debug("Deleted directory '{}'", sourcePath);
        } else {
           outputBuilder.deleted(Files.deleteIfExists(sourcePath));
           runContext.logger().debug("Deleted file '{}'", sourcePath);
        }

        return outputBuilder
            .uris(List.of())
            .build();
    }

    private Output deleteMatching(RunContext runContext, Path sourcePath, String rRegExp) throws Exception {
        if (!Files.exists(sourcePath)) {
            if (runContext.render(this.errorOnMissing).as(Boolean.class).orElse(false)) {
                throw new NoSuchElementException("Directory does not exist '" + sourcePath +
                    "'. To avoid this error, configure `errorOnMissing: false` in your configuration.");
            }

            runContext.logger().debug("Directory doesn't exist '{}'", sourcePath);

            return Output.builder()
                .uri(sourcePath.toUri())
                .deleted(false)
                .uris(List.of())
                .build();
        }

        final Pattern pattern;
        try {
            pattern = Pattern.compile(rRegExp);
        } catch (PatternSyntaxException e) {
            throw new IllegalArgumentException("Invalid regExp '" + rRegExp + "': " + e.getMessage(), e);
        }

        var isRecursive = Boolean.TRUE.equals(runContext.render(this.recursive).as(Boolean.class).orElse(false));
        var stream = isRecursive ? Files.walk(sourcePath) : Files.walk(sourcePath, 1);

        List<URI> uris = new ArrayList<>();
        try (stream) {
            var matched = stream
                .filter(p -> p.toAbsolutePath().startsWith(sourcePath.toAbsolutePath()))
                .filter(p -> Files.isRegularFile(p) && pattern.matcher(p.toAbsolutePath().toString()).matches())
                .toList();

            runContext.logger().warn(
                "Deleting {} file(s) matching regExp '{}' under '{}'",
                matched.size(), rRegExp, sourcePath
            );

            for (var file : matched) {
                Files.delete(file);
                uris.add(file.toUri());
            }
        }

        return Output.builder()
            .uri(sourcePath.toUri())
            .deleted(!uris.isEmpty())
            .uris(uris)
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
        @Schema(title = "URI of the deleted file or directory, or the target directory when regExp is used")
        private final URI uri;

        @Schema(title = "Whether the file was deleted")
        private final boolean deleted;

        @Schema(title = "URIs of all deleted files when regExp is used")
        private final List<URI> uris;
    }
}
