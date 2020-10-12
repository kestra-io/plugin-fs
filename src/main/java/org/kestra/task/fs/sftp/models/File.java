package org.kestra.task.fs.sftp.models;

import com.jcraft.jsch.SftpATTRS;
import lombok.Builder;
import lombok.Getter;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.sftp.SftpFileObject;

import java.lang.reflect.Field;
import java.net.URI;
import java.time.Instant;

@Getter
@Builder
public class File {
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

    public static File of(SftpFileObject fileObject) throws FileSystemException, NoSuchFieldException, IllegalAccessException {
        Field field = fileObject.getClass().getDeclaredField("attrs");
        field.setAccessible(true);
        SftpATTRS attrs = (SftpATTRS) field.get(fileObject);

        return File.builder()
            .path(URI.create(fileObject.getName().getPath()))
            .name(FilenameUtils.getName(fileObject.getName().getPath()))
            .fileType(fileObject.getType())
            .symbolicLink(fileObject.isSymbolicLink())
            .size(attrs.getSize())
            .userId(attrs.getUId())
            .groupId(attrs.getGId())
            .permissions(attrs.getPermissions())
            .flags(attrs.getFlags())
            .accessDate(Instant.ofEpochSecond(attrs.getATime()))
            .accessDate(Instant.ofEpochSecond(attrs.getMTime()))
            .updatedDate(Instant.ofEpochSecond(attrs.getATime()))
            .build();
    }
}
