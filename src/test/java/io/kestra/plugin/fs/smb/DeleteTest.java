package io.kestra.plugin.fs.smb;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.sftp.Delete;
import jakarta.inject.Inject;
import org.apache.commons.vfs2.FileSystemException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
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
            .from(Property.of(from))
            .host(Property.of("localhost"))
            .port(Property.of("445"))
            .username(Property.of("alice"))
            .password(Property.of("alipass"))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, fetch, Map.of());
        Assertions.assertDoesNotThrow(() -> fetch.run(runContext));

        io.kestra.plugin.fs.smb.Delete task;
        task = io.kestra.plugin.fs.smb.Delete.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(DeleteTest.class.getName())
            .uri(Property.of(from))
            .host(Property.of("localhost"))
            .port(Property.of("445"))
            .username(Property.of("alice"))
            .password(Property.of("alipass"))
            .build();

        Delete.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getUri().getPath(), endsWith(from));
        assertThat(run.isDeleted(), is(true));

        FileSystemException fileSystemException = Assertions.assertThrows(
            FileSystemException.class,
            () -> fetch.run(TestsUtils.mockRunContext(runContextFactory, fetch, Map.of()))
        );
        assertThat(fileSystemException.getMessage(), containsString("because it does not exist"));
    }
}
