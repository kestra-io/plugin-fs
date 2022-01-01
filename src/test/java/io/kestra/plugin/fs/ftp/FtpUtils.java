package io.kestra.plugin.fs.ftp;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.AbstractUtils;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.net.URI;

@Singleton
public class FtpUtils extends AbstractUtils {
    @Inject
    private RunContextFactory runContextFactory;

    public Upload.Output upload(URI source, String to) throws Exception {
        var task = Upload.builder()
            .id(FtpUtils.class.getSimpleName())
            .type(FtpUtils.class.getName())
            .from(source.toString())
            .to(to)
            .host("localhost")
            .port("6621")
            .username("guest")
            .password("guest")
            .build();

        return task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
    }
}
