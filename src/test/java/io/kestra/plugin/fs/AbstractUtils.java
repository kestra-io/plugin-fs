package io.kestra.plugin.fs;

import com.devskiller.friendly_id.FriendlyId;
import io.kestra.core.models.property.Property;
import io.kestra.core.storages.StorageInterface;
import io.kestra.plugin.fs.sftp.Upload;
import io.kestra.plugin.fs.vfs.Delete;
import io.kestra.plugin.fs.vfs.List;
import jakarta.inject.Inject;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
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
            null,
            null,
            new URI("/" + FriendlyId.createFriendlyId()),
            new FileInputStream(applicationFile)
        );
    }

    public Upload.Output upload(String to) throws Exception {
        return this.upload(uploadToStorage(), to);
    }

    abstract public Upload.Output upload(URI source, String to) throws Exception;

    abstract public List.Output list(String dir) throws Exception;

    abstract public Delete.Output delete(String file) throws Exception;
}
