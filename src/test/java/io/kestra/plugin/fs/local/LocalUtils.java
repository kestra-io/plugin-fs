package io.kestra.plugin.fs.local;

import com.devskiller.friendly_id.FriendlyId;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.AbstractUtils;
import io.micronaut.context.annotation.Prototype;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.Map;
import java.util.Objects;


@Slf4j
@Prototype
public class LocalUtils {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    public Upload.Output upload(String filePath) throws Exception {
        URI uri = uploadToStorage();

        Upload task = Upload.builder()
            .id(UploadTest.class.getSimpleName())
            .type(Upload.class.getName())
            .from(Property.ofValue(uri.toString()))
            .to(Property.ofValue(filePath))
            .overwrite(Property.ofValue(true))
            .build();

        return task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
    }

    public io.kestra.plugin.fs.local.List.Output list(String filePath) throws Exception {
        io.kestra.plugin.fs.local.List task = io.kestra.plugin.fs.local.List.builder()
            .id(LocalUtils.class.getSimpleName())
            .type(io.kestra.plugin.fs.local.List.class.getName())
            .from(Property.ofValue(filePath))
            .recursive(Property.ofValue(true))
            .build();

        return task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
    }

    public io.kestra.plugin.fs.local.Delete.Output delete(String file) throws Exception {
        var task = io.kestra.plugin.fs.local.Delete.builder()
            .id(LocalUtils.class.getSimpleName())
            .type(Delete.class.getName())
            .from(Property.ofValue(file))
            .recursive(Property.ofValue(true))
            .build();

        return task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
    }

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
}