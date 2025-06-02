package io.kestra.plugin.fs.local;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.fs.local.models.File;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;

import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "List files in the local filesystem."
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

    @Override
    public Output run(RunContext runContext) throws Exception {
        String resolvedDirectory = runContext.render(this.from).as(String.class).orElseThrow();

        Path directoryPath = resolveLocalPath(resolvedDirectory, runContext);

        if (!Files.exists(directoryPath)) {
            throw new IllegalArgumentException("Source path does not exist: " + directoryPath);
        }

        String fileRegex = this.regExp != null ? runContext.render(this.regExp).as(String.class).orElseThrow() : ".*";
        int maxDepth = runContext.render(recursive).as(Boolean.class).orElse(false) ? Integer.MAX_VALUE : 1;

        java.util.List<File> files = Files.find(directoryPath, maxDepth, (path, basicFileAttributes) -> {
                return basicFileAttributes.isRegularFile() && path.getFileName().toString().matches(fileRegex);
            })
            .map(throwFunction(path -> {
                BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
                return File.from(path, attrs);
            }))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

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