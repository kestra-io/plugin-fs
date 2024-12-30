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
import org.apache.commons.vfs2.provider.smb.SmbFileName;

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

    public static File of(AbstractFileObject<?> fileObject) throws FileSystemException, NoSuchFieldException, IllegalAccessException, URISyntaxException {
        FileBuilder builder = File.builder()
            .path(URI.create(fileObject.getName().getPath().replace(" ", "%20")))
            .serverPath(serverPath(fileObject))
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
                .accessDate(Instant.ofEpochSecond(attrs.getMTime()))
                .updatedDate(Instant.ofEpochSecond(attrs.getATime()));
        } catch (NoSuchFieldException ignored) {
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
            case GenericFileName genericFileName -> {
                String share = extractShareForSmb(genericFileName);
                yield  new URI(
                    genericFileName.getScheme(),
                    VfsService.basicAuth(genericFileName.getUserName(), genericFileName.getPassword()),
                    genericFileName.getHostName(),
                    genericFileName.getPort(),
                    share == null ? genericFileName.getPath() : String.join("", "/", share, genericFileName.getPath()),
                    null,
                    null
                );
            }
            default -> fileObject.getURI();
        };
    }

    private static String extractShareForSmb(GenericFileName genericFileName) {
        return switch (genericFileName) {
            case SmbFileName smbFileName -> smbFileName.getShare();
            case net.idauto.oss.jcifsng.vfs2.provider.SmbFileName smbFileName -> smbFileName.getShare();
            default -> null;
        };
    }
}
