package io.kestra.plugin.fs.sftp;

import io.kestra.core.exceptions.KestraRuntimeException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.vfs.models.File;
import jakarta.inject.Inject;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.vfs2.FileType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static io.kestra.plugin.fs.sftp.SftpUtils.PASSWORD;
import static io.kestra.plugin.fs.sftp.SftpUtils.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class MoveTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private SftpUtils sftpUtils;

    @Test
    void moveFileToOther() throws Exception {
        String from = "upload/" + IdUtils.create() + "/" + IdUtils.create() + ".yaml";
        String to = "upload/" + IdUtils.create() + "-move/" + IdUtils.create() + "/" + IdUtils.create() + "/" + IdUtils.create() + ".yaml";

        sftpUtils.upload(from);

        Move task = createMoveTask(from, to);

        Move.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getTo().getPath(), containsString(to));
    }

    @Test
    void moveFiletoDirectory() throws Exception {
        String from = "upload/" + IdUtils.create() + "/" + IdUtils.create() + ".yaml";
        String to = "upload/" + IdUtils.create() + "-move/" + IdUtils.create() + "/" + IdUtils.create() + "/";

        sftpUtils.upload(from);

        Move task = createMoveTask(from, to);

        Move.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getTo().getPath(), containsString(to));
    }

    @Test
    void moveFileWithSpaceToDirectoryWithSpace() throws Exception {
        String from = "upload/" + IdUtils.create() + "/" + IdUtils.create() + " space .yaml";
        String to = "upload/" + IdUtils.create() + "-move/" + IdUtils.create() + "/" + IdUtils.create() + " space 2 /";

        sftpUtils.upload(from);

        Move task = createMoveTask(from, to);

        Move.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getTo().getPath(), containsString(" "));
        assertThat(run.getFrom().getPath(), containsString(" "));
        assertThat(run.getTo().getPath(), containsString(to));
    }

    @Test
    void moveDirectory() throws Exception {
        String from = "upload/" + IdUtils.create() + "/" + IdUtils.create() + ".yaml";
        String to = "upload/" + IdUtils.create() + "-move/" + IdUtils.create() + "/";

        sftpUtils.upload(from);

        Move task = createMoveTask(FilenameUtils.getPath(from), to);

        Move.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getTo().getPath(), containsString(to));
    }

    @Test
    void shouldKeepNonAsciiFileNameWhenMovingToDirectory() throws Exception {
        String fileName = IdUtils.create() + "-facturación-ñandú.txt";
        String from = "upload/" + IdUtils.create() + "/" + fileName;
        String to = "upload/" + IdUtils.create() + "-move/";

        sftpUtils.upload(from);

        Move task = createMoveTask(from, to);

        Move.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getTo().getPath(), endsWith("/" + fileName));
        assertThat(sftpUtils.list("/" + to).getFiles().stream().map(File::getName).toList(), contains(fileName));
    }

    @ParameterizedTest
    @ValueSource(booleans =  {true, false})
    void moveFile_fileExistsInDestination(boolean overwrite) throws Exception {
        String fileName = "testFileName-" + IdUtils.create();
        String from = "upload/" + IdUtils.create() + "/" + fileName + ".yaml";
        String to = "upload/" + IdUtils.create() + "-move/" + IdUtils.create() + "/" + IdUtils.create() + "/" + fileName + ".yaml";

        sftpUtils.upload(from);

        //First move should be successful because nothing in direction folder
        Move task = createMoveTask(from, to);
        Move.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
        assertThat(run.getTo().getPath(), containsString(to));

        //Do the same move again
        sftpUtils.upload(from);
        Move secondMoveTask = createMoveTask(from, to, overwrite);
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        //If overwrite then no exception, otherwise throw exception
        if (overwrite) {
            Move.Output secondMove = secondMoveTask.run(runContext);
            assertThat(secondMove.getTo().getPath(), containsString(to));
        } else {
            KestraRuntimeException exception = assertThrows(KestraRuntimeException.class, () -> secondMoveTask.run(runContext));
            assertThat(exception.getMessage(), containsString(fileName));
        }
    }

    @Test
    void moveFile_fileExistsInDestinationWithDifferentName() throws Exception {
        var source = "upload/%s/incoming.txt".formatted(IdUtils.create());
        var destination = "upload/%s/current.txt".formatted(IdUtils.create());

        sftpUtils.update(source, "new incoming data");
        sftpUtils.update(destination, "protected existing data");

        var task = createMoveTask(source, destination);
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        var exception = assertThrows(KestraRuntimeException.class, () -> task.run(runContext));

        assertThat(exception.getMessage(), containsString("current.txt"));
        assertThat(sftpUtils.list(FilenameUtils.getPath(source)).getFiles(), hasSize(1));
        assertThat(sftpUtils.list(FilenameUtils.getPath(destination)).getFiles(), hasSize(1));
        assertThat(downloadContent(source, runContext), is("new incoming data"));
        assertThat(downloadContent(destination, runContext), is("protected existing data"));
    }

    @Test
    void moveFileToNonExistentTargetCreatesFileNotDirectory() throws Exception {
        var source = "upload/%s/source.txt".formatted(IdUtils.create());
        var targetDir = "upload/%s-nested/sub/".formatted(IdUtils.create());
        var destination = targetDir + "report.txt";

        sftpUtils.update(source, "sample report content");

        var task = createMoveTask(source, destination);
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        var run = task.run(runContext);

        assertThat(run.getTo().getPath(), containsString(destination));

        var targetFiles = sftpUtils.list(targetDir).getFiles();
        assertThat(targetFiles, hasSize(1));
        assertThat(targetFiles.getFirst().getName(), is("report.txt"));
        assertThat(targetFiles.getFirst().getFileType(), is(FileType.FILE));
        assertThat(downloadContent(destination, runContext), is("sample report content"));
        assertThat(sftpUtils.list(FilenameUtils.getPath(source)).getFiles(), hasSize(0));
    }

    private String downloadContent(String path, RunContext runContext) throws Exception {
        var task = Download.builder()
            .id(MoveTest.class.getSimpleName())
            .type(Download.class.getName())
            .from(Property.ofValue(path))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();
        var output = task.run(runContext);
        try (var input = runContext.storage().getFile(output.getTo())) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static Move createMoveTask(String from, String to) {
        return createMoveTask(from, to, false);
    }

    private static Move createMoveTask(String from, String to, boolean overwrite) {
        return Move.builder()
            .id(MoveTest.class.getSimpleName())
            .type(MoveTest.class.getName())
            .from(Property.ofValue(from))
            .to(Property.ofValue(to))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .overwrite(Property.ofValue(overwrite))
            .username(USERNAME)
            .password(PASSWORD)
            .build();
    }
}
