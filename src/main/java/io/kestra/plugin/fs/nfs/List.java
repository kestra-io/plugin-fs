package io.kestra.plugin.fs.nfs;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.Optional;
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
            full = true,
            title = "List files in an NFS directory.",
            code = """
                id: nfs_list
                namespace: company.team

                tasks:
                  - id: list_files
                    type: io.kestra.plugin.fs.nfs.List
                    from: /mnt/nfs/shared/documents
                    regExp: ".*\\.pdf$"
                    recursive: true
            """
        )
    }
    
)
public class List extends Task implements RunnableTask<List.Output> {

    @Schema(title = "The directory path to list from.")
    @NotNull
    private Property<String> from;

    @Schema(title = "A regular expression to filter files.")
    private Property<String> regExp;

    @Schema(title = "Whether to list files recursively.")
    @Builder.Default
    private Property<Boolean> recursive = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        NfsService nfsService = NfsService.getInstance();
        String rFrom = runContext.render(this.from).as(String.class).orElseThrow(() -> new IllegalArgumentException("'from' property is required"));
        Optional<String> rRegExp = runContext.render(this.regExp).as(String.class);
        boolean rRecursive = runContext.render(this.recursive).as(Boolean.class).orElse(false);

        Path fromPath = nfsService.toNfsPath(rFrom);

        logger.info("Listing files from '{}' (Recursive: {}, RegExp: '{}')", fromPath, rRecursive, rRegExp.orElse("None"));

        java.util.List<File> files;
        try (Stream<Path> stream = rRecursive ? Files.walk(fromPath) : Files.list(fromPath)) {
            Stream<Path> filteredStream = stream;

            if (rRecursive) {
                filteredStream = filteredStream.filter(path -> !path.equals(fromPath));
            }
            
            if (rRegExp.isPresent()) {
                String finalRegExp = rRegExp.get(); 
                filteredStream = filteredStream.filter(path -> path.toString().matches(finalRegExp));
            }

            files = filteredStream
                .map(throwFunction(this::mapToFile))
                .collect(Collectors.toList());
        }

        logger.info("Found {} files matching the criteria.", files.size());

        return Output.builder().files(files).build();
    }

    File mapToFile(Path path) throws IOException {
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
    @AllArgsConstructor
    public static class File {
        @Schema(title = "The name of the file.")
        private final String name;

        @Schema(title = "The absolute URI of the file.")
        private final URI uri;

        @Schema(title = "The java.nio.file.Path of the file.")
        @ToString.Exclude
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
