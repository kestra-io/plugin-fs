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
import org.apache.commons.vfs2.FileType;

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
    title = "List files in a local filesystem from",
    description = "Lists files in a specified from on the local filesystem"
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
                    regExp: ".*\\.csv"
                    recursive: true
                    workerGroup: "etl-worker"
                """
        )
    }
)
public class List extends AbstractLocalTask implements RunnableTask<List.Output> {

    @Schema(
        title = "The from to list files from",
        description = "The absolute path to the from on the local filesystem"
    )
    @NotNull
    private Property<String> from;

    @Schema(
        title = "Regular expression to filter files",
        description = "Only files matching this regular expression will be listed"
    )
    private Property<String> regExp;

    @Schema(
        title = "Whether to include subdirectories",
        description = "If true, will recursively list files in all subdirectories"
    )
    @Builder.Default
    private Property<Boolean> recursive = Property.of(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        String resolvedDirectory = runContext.render(this.from).as(String.class).orElseThrow();

        var basePath = runContext.render(this.basePath).as(String.class).orElse(USER_DIR);

        Path directoryPath = resolveLocalPath(resolvedDirectory, basePath);

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