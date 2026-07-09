package io.kestra.plugin.fs.vfs.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
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
    private final Instant updatedDate;

    // userId/groupId/permissions/flags/accessDate used to be populated via reflection into a
    // jsch-specific SftpATTRS field, which only existed for the SFTP provider and was always null
    // for FTP/FTPS/SMB. Replaced with the provider-agnostic VFS2 FileContent API below, which has
    // no equivalent for those four fields, so they were dropped rather than kept permanently null.
    public static File of(AbstractFileObject<?> fileObject) throws FileSystemException, URISyntaxException {
        FileBuilder builder = File.builder()
            .path(new URI(null, fileObject.getName().getPath(), null))
            .serverPath(serverPath(fileObject))
            .name(FilenameUtils.getName(fileObject.getName().getPath()))
            .fileType(fileObject.getType())
            .symbolicLink(fileObject.isSymbolicLink());

        if (fileObject.getType() == FileType.FILE) {
            var content = fileObject.getContent();
            builder
                .size(content.getSize())
                .updatedDate(Instant.ofEpochMilli(content.getLastModifiedTime()));
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
