package io.kestra.plugin.fs.local;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.fs.local.models.File;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "List files in the local filesystem.",
    description = """
        Local filesystem access is disabled by default.
        You must configure the plugin default `allowed-paths` in your Kestra configuration.

        Example (Kestra config):
        ```yaml
        plugins:
          configurations:
            - type: io.kestra.plugin.fs.local.List
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
                id: fs_local_list
                namespace: company.team

                tasks:
                  - id: list_files
                    type: io.kestra.plugin.fs.local.List
                    from: "/data/input"
                    regExp: ".*.csv"
                    recursive: true
                """
        )
    }
)
public class List extends AbstractLocalTask implements RunnableTask<List.Output> {

    @Schema(
        title = "The fully-qualified URIs that point to the path"
    )
    @NotNull
    private Property<String> from;

    @Schema(
        title = "Regular expression to filter files",
        description = "Only files matching this regular expression will be listed."
    )
    private Property<String> regExp;

    @Schema(
        title = "Whether to include subdirectories",
        description = "If true, the task will recursively list files in all subdirectories."
    )
    @Builder.Default
    private Property<Boolean> recursive = Property.ofValue(false);

    @Builder.Default
    @Schema(
        title = "The maximum number of files to retrieve at once"
    )
    private Property<Integer> maxFiles = Property.ofValue(25);

    @Override
    public Output run(RunContext runContext) throws Exception {
        String resolvedDirectory = runContext.render(this.from).as(String.class).orElseThrow();

        Path directoryPath = resolveLocalPath(resolvedDirectory, runContext);

        if (!Files.exists(directoryPath)) {
            throw new IllegalArgumentException("Source path does not exist: " + directoryPath);
        }

        String fileRegex = this.regExp != null ? runContext.render(this.regExp).as(String.class).orElseThrow() : ".*";
        int maxDepth = runContext.render(recursive).as(Boolean.class).orElse(false) ? Integer.MAX_VALUE : 1;

        java.util.List<File> files = Files.find(directoryPath, maxDepth, (path, basicFileAttributes) -> basicFileAttributes.isRegularFile() && path.toString().matches(fileRegex))
            .map(throwFunction(path -> {
                BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
                return File.from(path, attrs);
            }))
            .filter(Objects::nonNull)
            .toList();

        int rMaxFiles = runContext.render(this.maxFiles).as(Integer.class).orElse(25);
        if (files.size() > rMaxFiles) {
            runContext.logger().warn("Too many files to process, skipping");
            return Output.builder()
                .files(java.util.List.of())
                .count(0)
                .build();
        }

        return Output.builder()
            .files(files)
            .count(files.size())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "List of files found"
        )
        private java.util.List<File> files;

        @Schema(
            title = "Count of files found"
        )
        private Integer count;
    }
}
