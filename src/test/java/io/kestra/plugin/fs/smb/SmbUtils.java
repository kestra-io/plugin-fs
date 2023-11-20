package io.kestra.plugin.fs.smb;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.AbstractUtils;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.net.URI;

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
            .from(source.toString())
            .to(to)
            .host("localhost")
            .username("alice")
            .password("alipass")
            .build();

        return task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
    }
}
