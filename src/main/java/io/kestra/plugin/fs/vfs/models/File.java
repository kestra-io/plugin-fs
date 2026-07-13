package io.kestra.plugin.fs.vfs.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.jcraft.jsch.SftpATTRS;
import io.kestra.plugin.fs.vfs.VfsService;
import lombok.Builder;
import lombok.Getter;
import lombok.With;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.apache.commons.vfs2.provider.URLFileName;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
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

    public static File of(AbstractFileObject<?> fileObject) throws FileSystemException, URISyntaxException {
        FileBuilder builder = File.builder()
            .path(new URI(null, fileObject.getName().getPath(), null))
            .serverPath(serverPath(fileObject))
            .name(FilenameUtils.getName(fileObject.getName().getPath()))
            .fileType(fileObject.getType())
            .symbolicLink(fileObject.isSymbolicLink());

        if (fileObject.getType().hasContent()) {
            // size/updatedDate via the provider-agnostic VFS2 FileContent API so they are
            // populated for every provider (previously reflection-only, so null for FTP/FTPS/SMB).
            // getLastModifiedTime() works for files and folders, but getSize() throws on folders,
            // so size stays null for directory entries.
            var content = fileObject.getContent();
            builder.updatedDate(Instant.ofEpochMilli(content.getLastModifiedTime()));

            if (fileObject.getType() == FileType.FILE) {
                builder.size(content.getSize());
            }
        }

        // userId/groupId/permissions/flags/accessDate have no provider-agnostic VFS2 equivalent
        // and are populated best-effort via the jsch SftpATTRS field, which only exists on the
        // SFTP provider; they stay null for FTP/FTPS/SMB, as before.
        try {
            Field field = fileObject.getClass().getDeclaredField("attrs");
            field.setAccessible(true);
            SftpATTRS attrs = (SftpATTRS) field.get(fileObject);

            builder
                .userId(attrs.getUId())
                .groupId(attrs.getGId())
                .permissions(attrs.getPermissions())
                .flags(attrs.getFlags())
                .accessDate(Instant.ofEpochSecond(attrs.getATime()));
        } catch (NoSuchFieldException | IllegalAccessException ignored) {
        }

        return builder.build();
    }

    @SuppressWarnings("deprecation")
    private static URI serverPath(AbstractFileObject<?> fileObject) throws URISyntaxException {
        return switch (fileObject.getName()) {
            case URLFileName urlFileName -> new URI(
                    urlFileName.getScheme(),
                    VfsService.basicAuth(urlFileName.getUserName(), urlFileName.getPassword()),
                    urlFileName.getHostName(),
                    urlFileName.getPort(),
                    urlFileName.getPath(),
                    urlFileName.getQueryString(),
                    null
                );
            case GenericFileName genericFileName -> new URI(
                    genericFileName.getScheme(),
                    VfsService.basicAuth(genericFileName.getUserName(), genericFileName.getPassword()),
                    genericFileName.getHostName(),
                    genericFileName.getPort(),
                    genericFileName.getPath(),
                    null,
                    null
                );
            default -> fileObject.getURI();
        };
    }
}
