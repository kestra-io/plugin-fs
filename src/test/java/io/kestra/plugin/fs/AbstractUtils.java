package io.kestra.plugin.fs;

import com.devskiller.friendly_id.FriendlyId;
import io.kestra.core.models.property.Property;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.fs.sftp.Upload;
import io.kestra.plugin.fs.vfs.Delete;
import io.kestra.plugin.fs.vfs.List;
import jakarta.inject.Inject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Objects;

public abstract class AbstractUtils {

    @Inject
    private StorageInterface storageInterface;

    public URI uploadToStorage() throws Exception {
        File applicationFile = new File(Objects.requireNonNull(AbstractUtils.class.getClassLoader()
                .getResource("application.yml"))
            .toURI()
        );

        return storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + FriendlyId.createFriendlyId()),
            new FileInputStream(applicationFile)
        );
    }

    public Upload.Output upload(String to) throws Exception {
        return this.upload(uploadToStorage(), to);
    }

    public Upload.Output update(String to) throws Exception {
        return this.update(to, "Updated content -" + IdUtils.create());
    }

    public Upload.Output update(String to, String content) throws Exception {
        File tempFile = File.createTempFile("update-", ".yml");
        try (FileWriter writer = new FileWriter(tempFile, StandardCharsets.UTF_8)) {
            writer.write(content);
        }

        URI storageUri = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + FriendlyId.createFriendlyId()),
            Files.newInputStream(tempFile.toPath())
        );

        return this.upload(storageUri, to);
    }

    abstract public Upload.Output upload(URI source, String to) throws Exception;

    abstract public List.Output list(String dir) throws Exception;

    abstract public Delete.Output delete(String file) throws Exception;
}
