package io.kestra.plugin.fs.local.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Builder;
import lombok.Getter;
import lombok.With;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;

@Getter
@Builder
public class File {
    @With
    private final URI uri;
    private final Path localPath;
    private final String name;
    private final String parent;
    private final Long size;
    private final Instant createdDate;
    private final Instant modifiedDate;
    private final Instant accessedDate;
    private final boolean isDirectory;

    /**
     * Returns a copy of this file relocated to {@code newLocalPath}, keeping the Kestra storage URI untouched.
     * Used after a MOVE action so that reported paths point to where the file actually is.
     */
    public File withLocalPath(Path newLocalPath) {
        Path absolute = newLocalPath.toAbsolutePath().normalize();

        return File.builder()
            .uri(this.uri)
            .localPath(absolute)
            .name(absolute.getFileName().toString())
            .parent(absolute.getParent().toString())
            .size(this.size)
            .createdDate(this.createdDate)
            .modifiedDate(this.modifiedDate)
            .accessedDate(this.accessedDate)
            .isDirectory(this.isDirectory)
            .build();
    }

    public static File from(Path path, BasicFileAttributes attrs) {
        return File.builder()
            .uri(path.toUri())
            .localPath(path.toAbsolutePath())
            .name(path.getFileName().toString())
            .parent(path.getParent().toString())
            .size(attrs.size())
            .isDirectory(attrs.isDirectory())
            .modifiedDate(attrs.lastModifiedTime().toInstant())
            .accessedDate(attrs.lastAccessTime().toInstant())
            .createdDate(attrs.creationTime().toInstant())
            .build();
    }
}
