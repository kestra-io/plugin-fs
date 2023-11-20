package io.kestra.plugin.fs.smb;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.sftp.Delete;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.commons.vfs2.FileSystemException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@MicronautTest
class DeleteTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private SmbUtils smbUtils;

    @Test
    void run() throws Exception {
        String from = SmbUtils.SHARE_NAME + "/" + IdUtils.create() + "/" + IdUtils.create() + ".yml";

        smbUtils.upload(from);

        Download fetch = Download.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(DeleteTest.class.getName())
            .from(from)
            .host("localhost")
            .port("445")
            .username("alice")
            .password("alipass")
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, fetch, ImmutableMap.of());
        Assertions.assertDoesNotThrow(() -> fetch.run(runContext));

        io.kestra.plugin.fs.smb.Delete task;
        task = io.kestra.plugin.fs.smb.Delete.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(DeleteTest.class.getName())
            .uri(from)
            .host("localhost")
            .port("445")
            .username("alice")
            .password("alipass")
            .build();

        Delete.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(run.getUri().getPath(), endsWith(from));
        assertThat(run.isDeleted(), is(true));

        FileSystemException fileSystemException = Assertions.assertThrows(
            FileSystemException.class,
            () -> fetch.run(TestsUtils.mockRunContext(runContextFactory, fetch, ImmutableMap.of()))
        );
        assertThat(fileSystemException.getMessage(), containsString("because it does not exist"));
    }
}
