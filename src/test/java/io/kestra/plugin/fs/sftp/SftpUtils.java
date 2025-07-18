package io.kestra.plugin.fs.sftp;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.AbstractUtils;
import io.kestra.plugin.fs.vfs.Delete;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.net.URI;
import java.util.Map;

@Singleton
class SftpUtils extends AbstractUtils {
    @Inject
    private RunContextFactory runContextFactory;

    public static final Property<String> USERNAME = Property.ofValue("foo");
    public static final Property<String> PASSWORD = Property.ofValue("pass*+=");

    public Upload.Output upload(URI source, String to) throws Exception {
        var task = Upload.builder()
            .id(SftpUtils.class.getSimpleName())
            .type(Upload.class.getName())
            .from(Property.ofValue(source.toString()))
            .to(Property.ofValue(to))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .rootDir(Property.ofValue(true))
            .build();

        return task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
    }

    @Override
    public io.kestra.plugin.fs.vfs.List.Output list(String dir) throws Exception {
        return io.kestra.plugin.fs.sftp.List.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(TriggerTest.class.getName())
            .from(Property.ofValue(dir))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .build()
            .run(this.runContextFactory.of());
    }

    @Override
    public Delete.Output delete(String file) throws Exception {
        var task = io.kestra.plugin.fs.sftp.Delete.builder()
            .id(SftpUtils.class.getSimpleName())
            .type(Upload.class.getName())
            .uri(Property.ofValue(file))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .rootDir(Property.ofValue(true))
            .build();

        return task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
    }
}
