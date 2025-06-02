package io.kestra.plugin.fs.smb;

import io.kestra.core.exceptions.KestraRuntimeException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.vfs2.FileSystemException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static io.kestra.plugin.fs.smb.SmbUtils.PASSWORD;
import static io.kestra.plugin.fs.smb.SmbUtils.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

        Move task = createMoveTask(from, to);

        Move.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getTo().getPath(), endsWith(to));

        Download fetchFrom = createDownloadTask(from);
        Assertions.assertThrows(FileSystemException.class, () -> fetchFrom.run(TestsUtils.mockRunContext(runContextFactory, fetchFrom, Map.of())));

        Download fetchTo = fetchFrom.toBuilder()
            .from(Property.ofValue(to))
            .build();
        Assertions.assertDoesNotThrow(() -> fetchTo.run(TestsUtils.mockRunContext(runContextFactory, fetchTo, Map.of())));
    }

    @Test
    void moveFiletoDirectory() throws Exception {
        String from = SmbUtils.SHARE_NAME + "/" + IdUtils.create() + "/" + IdUtils.create() + ".yaml";
        String to = SmbUtils.SHARE_NAME + "/" + IdUtils.create() + "-move/" + IdUtils.create() + "/" + IdUtils.create() + "/";

        smbUtils.upload(from);

        Move task = createMoveTask(from, to);

        Move.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getTo().getPath(), endsWith(to + FilenameUtils.getName(from)));

        Download fetchFrom = createDownloadTask(from);
        Assertions.assertThrows(FileSystemException.class, () -> fetchFrom.run(TestsUtils.mockRunContext(runContextFactory, fetchFrom, Map.of())));

        Download fetchTo = fetchFrom.toBuilder()
            .from(Property.ofValue(to + "/" + FilenameUtils.getName(from)))
            .build();
        Assertions.assertDoesNotThrow(() -> fetchTo.run(TestsUtils.mockRunContext(runContextFactory, fetchTo, Map.of())));
    }

    @Test
    void moveFileWithSpaceToDirectoryWithSpace() throws Exception {
        String from = SmbUtils.SHARE_NAME + "/" + IdUtils.create() + "/" + IdUtils.create() + " space .yaml";
        String to = SmbUtils.SHARE_NAME + "/" + IdUtils.create() + "-move/" + IdUtils.create() + "/ space " + IdUtils.create() + "space2/";

        smbUtils.upload(from);

        Move task = createMoveTask(from, to);

        Move.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getTo().getPath(), endsWith(to + FilenameUtils.getName(from)));

        Download fetchFrom = createDownloadTask(from);
        Assertions.assertThrows(FileSystemException.class, () -> fetchFrom.run(TestsUtils.mockRunContext(runContextFactory, fetchFrom, Map.of())));

        Download fetchTo = fetchFrom.toBuilder()
                .from(Property.ofValue(to + "/" + FilenameUtils.getName(from)))
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
            .from(Property.ofValue(FilenameUtils.getPath(from)))
            .to(Property.ofValue(to))
            .host(Property.ofValue("localhost"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        Move.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getTo().getPath(), endsWith(to));

        Download fetchFrom = createDownloadTask(from);
        Assertions.assertThrows(FileSystemException.class, () -> fetchFrom.run(TestsUtils.mockRunContext(runContextFactory, fetchFrom, Map.of())));

        Download fetchTo = fetchFrom.toBuilder()
            .from(Property.ofValue(to + "/" + FilenameUtils.getName(from)))
            .build();
        Assertions.assertDoesNotThrow(() -> fetchTo.run(TestsUtils.mockRunContext(runContextFactory, fetchTo, Map.of())));
    }

    @ParameterizedTest
    @ValueSource(booleans =  {true, false})
    void moveFile_fileExistsInDestination(boolean overwrite) throws Exception {
        String fileName = "testFileName-" + IdUtils.create();
        String from = "upload/" + IdUtils.create() + "/" + fileName + ".yaml";
        String to = "upload/" + IdUtils.create() + "-move/" + IdUtils.create() + "/" + IdUtils.create() + "/" + fileName + ".yaml";

        smbUtils.upload(from);

        //First move should be successful because nothing in direction folder
        Move task = createMoveTask(from, to);
        io.kestra.plugin.fs.sftp.Move.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
        assertThat(run.getTo().getPath(), containsString(to));

        //Do the same move again
        smbUtils.upload(from);
        Move secondMoveTask = createMoveTask(from, to, overwrite);
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        //If overwrite then no exception, otherwise throw exception
        if (overwrite) {
            io.kestra.plugin.fs.sftp.Move.Output secondMove = secondMoveTask.run(runContext);
            assertThat(secondMove.getTo().getPath(), containsString(to));
        } else {
            KestraRuntimeException exception = assertThrows(KestraRuntimeException.class, () -> secondMoveTask.run(runContext));
            assertThat(exception.getMessage(), containsString(fileName));
        }
    }

    private static Move createMoveTask(String from, String to) {
        return createMoveTask(from, to, false);
    }

    private static Move createMoveTask(String from, String to, boolean overwrite) {
        return Move.builder()
            .id(MoveTest.class.getSimpleName())
            .type(Move.class.getName())
            .from(Property.ofValue(from))
            .to(Property.ofValue(to))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("445"))
            .username(SmbUtils.USERNAME)
            .password(SmbUtils.PASSWORD)
            .overwrite(Property.ofValue(overwrite))
            .build();
    }

    private static Download createDownloadTask(String from) {
        return Download.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(DeleteTest.class.getName())
            .from(Property.ofValue(from))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("445"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();
    }
}
