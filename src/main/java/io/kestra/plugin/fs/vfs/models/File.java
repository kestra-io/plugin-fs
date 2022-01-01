package io.kestra.plugin.fs.vfs.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.jcraft.jsch.SftpATTRS;
import lombok.Builder;
import lombok.Getter;
import lombok.With;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileObject;

import java.lang.reflect.Field;
import java.net.URI;
import java.time.Instant;

@Getter
@Builder
public class File {
    @JsonIgnore
    private final URI serverPath;
    @With
    private final URI path;
    private final String name;
    private final FileType fileType;
    private final boolean symbolicLink;
    private final Long size;
    private final Integer userId;
    private final Integer groupId;
    private final Integer permissions;
    private final Integer flags;
    private final Instant accessDate;
    private final Instant updatedDate;

    public static File of(AbstractFileObject<?> fileObject) throws FileSystemException, NoSuchFieldException, IllegalAccessException {
        FileBuilder builder = File.builder()
            .path(URI.create(fileObject.getName().getPath()))
            .serverPath(fileObject.getURI())
            .name(FilenameUtils.getName(fileObject.getName().getPath()))
            .fileType(fileObject.getType())
            .symbolicLink(fileObject.isSymbolicLink());

        try {
            Field field = fileObject.getClass().getDeclaredField("attrs");
            field.setAccessible(true);
            SftpATTRS attrs = (SftpATTRS) field.get(fileObject);

            builder
                .size(attrs.getSize())
                .userId(attrs.getUId())
                .groupId(attrs.getGId())
                .permissions(attrs.getPermissions())
                .flags(attrs.getFlags())
                .accessDate(Instant.ofEpochSecond(attrs.getATime()))
                .accessDate(Instant.ofEpochSecond(attrs.getMTime()))
                .updatedDate(Instant.ofEpochSecond(attrs.getATime()));
        } catch (NoSuchFieldException ignored) {
        }

        return builder.build();
    }
}
