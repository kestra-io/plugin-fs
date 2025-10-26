package io.kestra.plugin.fs.nfs;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
// Removed non-existent import: import io.kestra.plugin.fs.ListInterface;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "List files from an NFS mount point."
)
@Plugin(
    examples = {
        @Example(
            title = "List files in an NFS directory.",
            code = {
                "from: /mnt/nfs/data",
                "recursive: true",
                "regExp: \".*\\.csv$\""
            }
        )
    }
)
// Removed non-existent interface: ListInterface
public class List extends Task implements RunnableTask<List.Output> {

    @Schema(title = "The directory path to list from.")
    @PluginProperty(dynamic = true)
    @NotNull
    private String from;

    @Schema(title = "A regular expression to filter files.")
    @PluginProperty(dynamic = true)
    private String regExp;

    @Schema(title = "Whether to list files recursively.")
    @PluginProperty
    @Builder.Default
    private Boolean recursive = false;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String renderedFrom = runContext.render(this.from);
        Path fromPath = NfsService.toNfsPath(renderedFrom);
        String renderedRegExp = runContext.render(this.regExp);

        java.util.List<File> files;
        try (Stream<Path> stream = this.recursive ? Files.walk(fromPath) : Files.list(fromPath)) {
            files = stream
                .filter(path -> renderedRegExp == null || path.toString().matches(renderedRegExp))
                .map(throwFunction(this::map))
                .collect(Collectors.toList());
        }

        return Output.builder().files(files).build();
    }

    private File map(Path path) throws IOException {
        BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
        return File.builder()
            .name(path.getFileName().toString())
            .localPath(path)
            .uri(path.toUri())
            .isDirectory(attrs.isDirectory())
            .isSymbolicLink(attrs.isSymbolicLink())
            .isHidden(Files.isHidden(path))
            .creationTime(attrs.creationTime().toInstant())
            .lastAccessTime(attrs.lastAccessTime().toInstant())
            .lastModifiedTime(attrs.lastModifiedTime().toInstant())
            .size(attrs.size())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The list of files found.")
        private final java.util.List<File> files;
    }

    @SuperBuilder
    @Getter
    @EqualsAndHashCode
    @ToString
    public static class File {
        @Schema(title = "The name of the file.")
        private final String name;

        @Schema(title = "The absolute URI of the file.")
        private final URI uri;

        @Schema(title = "The java.nio.file.Path of the file.")
        private final Path localPath;

        @Schema(title = "Whether the file is a directory.")
        private final boolean isDirectory;

        @Schema(title = "Whether the file is a symbolic link.")
        private final boolean isSymbolicLink;

        @Schema(title = "Whether the file is hidden.")
        private final boolean isHidden;

        @Schema(title = "The size of the file in bytes.")
        private final long size;

        @Schema(title = "The creation time of the file.")
        private final Instant creationTime;

        @Schema(title = "The last access time of the file.")
        private final Instant lastAccessTime;

        @Schema(title = "The last modified time of the file.")
        private final Instant lastModifiedTime;
    }
}

