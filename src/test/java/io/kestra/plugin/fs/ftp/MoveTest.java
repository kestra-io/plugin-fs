package io.kestra.plugin.fs.ftp;

import io.kestra.core.exceptions.KestraRuntimeException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.apache.commons.io.FilenameUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static io.kestra.plugin.fs.ftp.FtpUtils.PASSWORD;
import static io.kestra.plugin.fs.ftp.FtpUtils.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class MoveTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private FtpUtils ftpUtils;

    @Test
    void moveFileToOther() throws Exception {
        String from = "upload/" + IdUtils.create() + "/" + IdUtils.create() + ".yaml";
        String to = "upload/" + IdUtils.create() + "-move/" + IdUtils.create() + "/" + IdUtils.create() + "/" + IdUtils.create() + ".yaml";

        ftpUtils.upload(from);

        Move task = createMoveTask(from, to);

        Move.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getTo().getPath(), containsString(to));
    }

    @Test
    void moveFiletoDirectory() throws Exception {
        String from = "upload/" + IdUtils.create() + "/" + IdUtils.create() + ".yaml";
        String to = "upload/" + IdUtils.create() + "-move/" + IdUtils.create() + "/" + IdUtils.create() + "/";

        ftpUtils.upload(from);

        Move task = createMoveTask(from, to);

        Move.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getTo().getPath(), containsString(to));
    }

    @Test
    void moveDirectory() throws Exception {
        String from = "upload/" + IdUtils.create() + "/" + IdUtils.create() + ".yaml";
        String to = "upload/" + IdUtils.create() + "-move/"  + IdUtils.create() + "/";

        ftpUtils.upload(from);

        Move task = createMoveTask(FilenameUtils.getPath(from), to);

        Move.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getTo().getPath(), containsString(to));
    }

    @Test
    void moveFileWithSpaceToDirectoryWithSpace() throws Exception {
        //Add space in filename
        String from = "upload/" + IdUtils.create() + "/" + IdUtils.create() + " space .yaml";
        String to = "upload/" + IdUtils.create() + "-move/" + IdUtils.create() + "/" + IdUtils.create() + " space2 /";

        ftpUtils.upload(from);

        Move task = createMoveTask(from, to);

        Move.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getTo().getPath(), containsString(" "));
        assertThat(run.getFrom().getPath(), containsString(" "));
        assertThat(run.getTo().getPath(), containsString(to));
    }

    @ParameterizedTest
    @ValueSource(booleans =  {true, false})
    void moveFile_fileExistsInDestination(boolean overwrite) throws Exception {
        String fileName = "testFileName-" + IdUtils.create();
        String from = "upload/" + IdUtils.create() + "/" + fileName + ".yaml";
        String to = "upload/" + IdUtils.create() + "-move/" + IdUtils.create() + "/" + IdUtils.create() + "/" + fileName + ".yaml";

        ftpUtils.upload(from);

        //First move should be successful because nothing in direction folder
        Move task = createMoveTask(from, to);
        io.kestra.plugin.fs.sftp.Move.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
        assertThat(run.getTo().getPath(), containsString(to));

        //Do the same move again
        ftpUtils.upload(from);
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
            .port(Property.ofValue("6621"))
            .username(USERNAME)
            .password(PASSWORD)
            .overwrite(Property.ofValue(overwrite))
            .build();
    }
}
