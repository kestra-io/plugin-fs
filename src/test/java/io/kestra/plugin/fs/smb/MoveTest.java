package io.kestra.plugin.fs.smb;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.vfs2.FileSystemException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;

@MicronautTest
class MoveTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private SmbUtils smbUtils;

    @Test
    void moveFileToOther() throws Exception {
        String from = SmbUtils.SHARE_NAME + "/" + IdUtils.create() + "/" + IdUtils.create() + ".yaml";
        String to = SmbUtils.SHARE_NAME + "/" + IdUtils.create() + "-move/" + IdUtils.create() + "/" + IdUtils.create() + "/" + IdUtils.create() + ".yaml";

        smbUtils.upload(from);

        Move task = Move.builder()
            .id(MoveTest.class.getSimpleName())
            .type(MoveTest.class.getName())
            .from(from)
            .to(to)
            .host("localhost")
            .username("alice")
            .password("alipass")
            .build();

        Move.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(run.getTo().getPath(), endsWith(to));

        Download fetchFrom = Download.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(DeleteTest.class.getName())
            .from(from)
            .host("localhost")
            .port("445")
            .username("alice")
            .password("alipass")
            .build();
        Assertions.assertThrows(FileSystemException.class, () -> fetchFrom.run(TestsUtils.mockRunContext(runContextFactory, fetchFrom, ImmutableMap.of())));

        Download fetchTo = fetchFrom.toBuilder()
            .from(to)
            .build();
        Assertions.assertDoesNotThrow(() -> fetchTo.run(TestsUtils.mockRunContext(runContextFactory, fetchTo, ImmutableMap.of())));
    }

    @Test
    void moveFiletoDirectory() throws Exception {
        String from = SmbUtils.SHARE_NAME + "/" + IdUtils.create() + "/" + IdUtils.create() + ".yaml";
        String to = SmbUtils.SHARE_NAME + "/" + IdUtils.create() + "-move/" + IdUtils.create() + "/" + IdUtils.create() + "/";

        smbUtils.upload(from);

        Move task = Move.builder()
            .id(MoveTest.class.getSimpleName())
            .type(Move.class.getName())
            .from(from)
            .to(to)
            .host("localhost")
            .username("alice")
            .password("alipass")
            .build();

        Move.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(run.getTo().getPath(), endsWith(to + FilenameUtils.getName(from)));

        Download fetchFrom = Download.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(DeleteTest.class.getName())
            .from(from)
            .host("localhost")
            .port("445")
            .username("alice")
            .password("alipass")
            .build();
        Assertions.assertThrows(FileSystemException.class, () -> fetchFrom.run(TestsUtils.mockRunContext(runContextFactory, fetchFrom, ImmutableMap.of())));

        Download fetchTo = fetchFrom.toBuilder()
            .from(to + "/" + FilenameUtils.getName(from))
            .build();
        Assertions.assertDoesNotThrow(() -> fetchTo.run(TestsUtils.mockRunContext(runContextFactory, fetchTo, ImmutableMap.of())));
    }

    @Test
    void moveDirectory() throws Exception {
        String from = SmbUtils.SHARE_NAME + "/" + IdUtils.create() + "/" + IdUtils.create() + ".yaml";
        String to = SmbUtils.SHARE_NAME + "/" + IdUtils.create() + "-move/" + IdUtils.create() + "/";

        smbUtils.upload(from);

        Move task = Move.builder()
            .id(MoveTest.class.getSimpleName())
            .type(Move.class.getName())
            .from(FilenameUtils.getPath(from))
            .to(to)
            .host("localhost")
            .username("alice")
            .password("alipass")
            .build();

        Move.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(run.getTo().getPath(), endsWith(to));

        Download fetchFrom = Download.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(DeleteTest.class.getName())
            .from(from)
            .host("localhost")
            .port("445")
            .username("alice")
            .password("alipass")
            .build();
        Assertions.assertThrows(FileSystemException.class, () -> fetchFrom.run(TestsUtils.mockRunContext(runContextFactory, fetchFrom, ImmutableMap.of())));

        Download fetchTo = fetchFrom.toBuilder()
            .from(to + "/" + FilenameUtils.getName(from))
            .build();
        Assertions.assertDoesNotThrow(() -> fetchTo.run(TestsUtils.mockRunContext(runContextFactory, fetchTo, ImmutableMap.of())));
    }
}
