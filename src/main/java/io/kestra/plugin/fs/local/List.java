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
    title = "List local files",
    description = """
        Lists files under a directory allowed by `allowed-paths`; optional regexp filter and recursion. Limits results to `maxFiles` (default 25).
        Local access requires `allowed-paths` in plugin defaults.

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
        title = "Directory to scan"
    )
    @NotNull
    private Property<String> from;

    @Schema(
        title = "Regular expression filter",
        description = "Only files matching this regex are listed."
    )
    private Property<String> regExp;

    @Schema(
        title = "Include subdirectories",
        description = "If true, list files recursively."
    )
    @Builder.Default
    private Property<Boolean> recursive = Property.ofValue(false);

    @Builder.Default
    @Schema(
        title = "Maximum files to retrieve"
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
            runContext.logger().warn("Too many files to process ({}), limiting to {}", files.size(), rMaxFiles);
            files = files.subList(0, rMaxFiles);
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
