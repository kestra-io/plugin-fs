package io.kestra.plugin.fs.sftp;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.AbstractUtils;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.net.URI;

@Singleton
class SftpUtils extends AbstractUtils {
    @Inject
    private RunContextFactory runContextFactory;

    public Upload.Output upload(URI source, String to) throws Exception {
        var task = Upload.builder()
            .id(SftpUtils.class.getSimpleName())
            .type(Upload.class.getName())
            .from(source.toString())
            .to(to)
            .host("localhost")
            .port("6622")
            .username("foo")
            .password("pass")
            .rootDir(true)
            .build();

        return task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
    }

    @Override
    public io.kestra.plugin.fs.vfs.List.Output list(String dir) throws Exception {
        return io.kestra.plugin.fs.sftp.List.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(TriggerTest.class.getName())
            .from(dir)
            .host("localhost")
            .port("6622")
            .username("foo")
            .password("pass")
            .build()
            .run(this.runContextFactory.of());
    }
}
