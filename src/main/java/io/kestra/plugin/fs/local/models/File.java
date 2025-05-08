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
