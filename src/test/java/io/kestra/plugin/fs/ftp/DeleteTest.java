package io.kestra.plugin.fs.ftp;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.sftp.Delete;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

@MicronautTest
class DeleteTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private FtpUtils sftpUtils;

    @Test
    void run() throws Exception {
        String from = IdUtils.create() + "/" + IdUtils.create() + ".yaml";

        sftpUtils.upload(from);

        io.kestra.plugin.fs.ftp.Delete task;
        task = io.kestra.plugin.fs.ftp.Delete.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(DeleteTest.class.getName())
            .uri(from)
            .host("localhost")
            .port("6621")
            .username("guest")
            .password("guest")
            .build();

        Delete.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(run.getUri().getPath(), containsString(from));
        assertThat(run.isDeleted(), is(true));
    }
}
