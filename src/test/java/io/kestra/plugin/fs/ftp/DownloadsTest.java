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
import static org.hamcrest.Matchers.endsWith;
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
    void run_DeleteAfterDownloads() throws Exception {
        String toUploadDir = "/upload/" + random;

        String out1 = FriendlyId.createFriendlyId();
        ftpUtils.upload(toUploadDir + "/" + out1 + ".txt");
        String out2 = FriendlyId.createFriendlyId();
        ftpUtils.upload(toUploadDir + "/" + out2 + ".txt");

        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(DownloadsTest.class.getName())
            .from(toUploadDir + "/")
            .action(Downloads.Action.DELETE)
            .host("localhost")
            .port("6621")
            .username("guest")
            .password("guest")
            .build();

        Downloads.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(run.getFiles().size(), is(2));
        assertThat(run.getFiles().get(0).getPath().getPath(), endsWith(".txt"));

        assertThat(ftpUtils.list(toUploadDir).getFiles().isEmpty(), is(true));
    }

    @Test
    void run_NoneAfterDownloads() throws Exception {
        String toUploadDir = "/upload/" + random;
        String out1 = FriendlyId.createFriendlyId();
        ftpUtils.upload(toUploadDir + "/" + out1 + ".txt");
        String out2 = FriendlyId.createFriendlyId();
        ftpUtils.upload(toUploadDir + "/" + out2 + ".txt");

        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(DownloadsTest.class.getName())
            .from(toUploadDir + "/")
            .action(Downloads.Action.NONE)
            .host("localhost")
            .port("6621")
            .username("guest")
            .password("guest")
            .build();

        Downloads.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(run.getFiles().size(), is(2));
        assertThat(run.getFiles().get(0).getPath().getPath(), endsWith(".txt"));

        assertThat(ftpUtils.list(toUploadDir).getFiles().size(), is(2));
    }
}
