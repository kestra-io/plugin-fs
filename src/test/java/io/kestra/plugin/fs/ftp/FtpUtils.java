package io.kestra.plugin.fs.ftp;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.AbstractUtils;
import io.kestra.plugin.fs.vfs.List;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.net.URI;
import java.util.Map;

@Singleton
public class FtpUtils extends AbstractUtils {
    @Inject
    private RunContextFactory runContextFactory;
    public static final Property<String> USERNAME = Property.of("guest");
    public static final Property<String> PASSWORD = Property.of("O7m)&H/0Em4/T8RqCa!Al=M@N6^;@+");

    public Upload.Output upload(URI source, String to) throws Exception {
        var task = Upload.builder()
            .id(FtpUtils.class.getSimpleName())
            .type(FtpUtils.class.getName())
            .from(Property.of(source.toString()))
            .to(Property.of(to))
            .host(Property.of("localhost"))
            .port(Property.of("6621"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        return task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
    }

    @Override
    public List.Output list(String dir) throws Exception {
        return io.kestra.plugin.fs.ftp.List.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(TriggerTest.class.getName())
            .from(Property.of(dir))
            .host(Property.of("localhost"))
            .port(Property.of("6621"))
            .username(USERNAME)
            .password(PASSWORD)
            .build()
            .run(this.runContextFactory.of());
    }

    @Override
    public Delete.Output delete(String file) throws Exception {
        var task = io.kestra.plugin.fs.ftp.Delete.builder()
            .id(FtpUtils.class.getSimpleName())
            .type(FtpUtils.class.getName())
            .uri(Property.of(file))
            .host(Property.of("localhost"))
            .port(Property.of("6621"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        return task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
    }
}
