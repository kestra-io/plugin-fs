package io.kestra.plugin.fs.smb;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.vfs2.FileSystemException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.kestra.plugin.fs.smb.SmbUtils.PASSWORD;
import static io.kestra.plugin.fs.smb.SmbUtils.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;

@KestraTest
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
            .from(Property.of(from))
            .to(Property.of(to))
            .host(Property.of("localhost"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        Move.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getTo().getPath(), endsWith(to));

        Download fetchFrom = Download.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(DeleteTest.class.getName())
            .from(Property.of(from))
            .host(Property.of("localhost"))
            .port(Property.of("445"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();
        Assertions.assertThrows(FileSystemException.class, () -> fetchFrom.run(TestsUtils.mockRunContext(runContextFactory, fetchFrom, Map.of())));

        Download fetchTo = fetchFrom.toBuilder()
            .from(Property.of(to))
            .build();
        Assertions.assertDoesNotThrow(() -> fetchTo.run(TestsUtils.mockRunContext(runContextFactory, fetchTo, Map.of())));
    }

    @Test
    void moveFiletoDirectory() throws Exception {
        String from = SmbUtils.SHARE_NAME + "/" + IdUtils.create() + "/" + IdUtils.create() + ".yaml";
        String to = SmbUtils.SHARE_NAME + "/" + IdUtils.create() + "-move/" + IdUtils.create() + "/" + IdUtils.create() + "/";

        smbUtils.upload(from);

        Move task = Move.builder()
            .id(MoveTest.class.getSimpleName())
            .type(Move.class.getName())
            .from(Property.of(from))
            .to(Property.of(to))
            .host(Property.of("localhost"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        Move.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getTo().getPath(), endsWith(to + FilenameUtils.getName(from)));

        Download fetchFrom = Download.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(DeleteTest.class.getName())
            .from(Property.of(from))
            .host(Property.of("localhost"))
            .port(Property.of("445"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();
        Assertions.assertThrows(FileSystemException.class, () -> fetchFrom.run(TestsUtils.mockRunContext(runContextFactory, fetchFrom, Map.of())));

        Download fetchTo = fetchFrom.toBuilder()
            .from(Property.of(to + "/" + FilenameUtils.getName(from)))
            .build();
        Assertions.assertDoesNotThrow(() -> fetchTo.run(TestsUtils.mockRunContext(runContextFactory, fetchTo, Map.of())));
    }

    @Test
    void moveFileWithSpaceToDirectoryWithSpace() throws Exception {
        String from = SmbUtils.SHARE_NAME + "/" + IdUtils.create() + "/" + IdUtils.create() + " space .yaml";
        String to = SmbUtils.SHARE_NAME + "/" + IdUtils.create() + "-move/" + IdUtils.create() + "/ space " + IdUtils.create() + "space2/";

        smbUtils.upload(from);

        Move task = Move.builder()
                .id(MoveTest.class.getSimpleName())
                .type(Move.class.getName())
                .from(Property.of(from))
                .to(Property.of(to))
                .host(Property.of("localhost"))
                .username(USERNAME)
                .password(PASSWORD)
                .build();

        Move.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getTo().getPath(), endsWith(to + FilenameUtils.getName(from)));

        Download fetchFrom = Download.builder()
                .id(DeleteTest.class.getSimpleName())
                .type(DeleteTest.class.getName())
                .from(Property.of(from))
                .host(Property.of("localhost"))
                .port(Property.of("445"))
                .username(USERNAME)
                .password(PASSWORD)
                .build();
        Assertions.assertThrows(FileSystemException.class, () -> fetchFrom.run(TestsUtils.mockRunContext(runContextFactory, fetchFrom, Map.of())));

        Download fetchTo = fetchFrom.toBuilder()
                .from(Property.of(to + "/" + FilenameUtils.getName(from)))
                .build();
        Assertions.assertDoesNotThrow(() -> fetchTo.run(TestsUtils.mockRunContext(runContextFactory, fetchTo, Map.of())));
    }

    @Test
    void moveDirectory() throws Exception {
        String from = SmbUtils.SHARE_NAME + "/" + IdUtils.create() + "/" + IdUtils.create() + ".yaml";
        String to = SmbUtils.SHARE_NAME + "/" + IdUtils.create() + "-move/" + IdUtils.create() + "/";

        smbUtils.upload(from);

        Move task = Move.builder()
            .id(MoveTest.class.getSimpleName())
            .type(Move.class.getName())
            .from(Property.of(FilenameUtils.getPath(from)))
            .to(Property.of(to))
            .host(Property.of("localhost"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        Move.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getTo().getPath(), endsWith(to));

        Download fetchFrom = Download.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(DeleteTest.class.getName())
            .from(Property.of(from))
            .host(Property.of("localhost"))
            .port(Property.of("445"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();
        Assertions.assertThrows(FileSystemException.class, () -> fetchFrom.run(TestsUtils.mockRunContext(runContextFactory, fetchFrom, Map.of())));

        Download fetchTo = fetchFrom.toBuilder()
            .from(Property.of(to + "/" + FilenameUtils.getName(from)))
            .build();
        Assertions.assertDoesNotThrow(() -> fetchTo.run(TestsUtils.mockRunContext(runContextFactory, fetchTo, Map.of())));
    }
}
