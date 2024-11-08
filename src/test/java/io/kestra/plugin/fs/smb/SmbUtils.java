package io.kestra.plugin.fs.smb;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.AbstractUtils;
import io.kestra.plugin.fs.vfs.Delete;
import io.kestra.plugin.fs.vfs.List;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.net.URI;
import java.util.Map;

@Singleton
public class SmbUtils extends AbstractUtils {
    public static final String SHARE_NAME = "upload";
    public static final String SECOND_SHARE_NAME = "another_upload";

    @Inject
    private RunContextFactory runContextFactory;

    public Upload.Output upload(URI source, String to) throws Exception {
        var task = Upload.builder()
            .id(SmbUtils.class.getSimpleName())
            .type(SmbUtils.class.getName())
            .from(Property.of(source.toString()))
            .to(Property.of(to))
            .host(Property.of("localhost"))
            .username(Property.of("alice"))
            .password(Property.of("alipass"))
            .build();

        return task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
    }

    @Override
    public List.Output list(String dir) throws Exception {
        return io.kestra.plugin.fs.smb.List.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(TriggerTest.class.getName())
            .from(Property.of(dir))
            .host(Property.of("localhost"))
            .port(Property.of("445"))
            .username(Property.of("alice"))
            .password(Property.of("alipass"))
            .build()
            .run(this.runContextFactory.of());
    }

    @Override
    public Delete.Output delete(String file) throws Exception {
        var task = io.kestra.plugin.fs.smb.Delete.builder()
            .id(SmbUtils.class.getSimpleName())
            .type(SmbUtils.class.getName())
            .uri(Property.of(file))
            .host(Property.of("localhost"))
            .username(Property.of("alice"))
            .password(Property.of("alipass"))
            .build();

        return task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
    }
}
