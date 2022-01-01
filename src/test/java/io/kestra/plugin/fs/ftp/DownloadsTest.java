package io.kestra.plugin.fs.ftp;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class DownloadsTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private FtpUtils ftpUtils;

    @Value("${kestra.variables.globals.random}")
    private String random;

    @Test
    void run() throws Exception {
        String out1 = FriendlyId.createFriendlyId();
        ftpUtils.upload("/upload/" + random + "/" + out1);
        String out2 = FriendlyId.createFriendlyId();
        ftpUtils.upload("/upload/" + random + "/" + out2);

        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(DownloadsTest.class.getName())
            .from("/upload/" + random + "/")
            .action(Downloads.Action.DELETE)
            .host("localhost")
            .port("6621")
            .username("guest")
            .password("guest")
            .build();

        Downloads.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(run.getFiles().size(), is(2));
    }
}
