package io.kestra.plugin.fs.ftp;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.apache.commons.io.FilenameUtils;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

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

        Move task = Move.builder()
            .id(MoveTest.class.getSimpleName())
            .type(MoveTest.class.getName())
            .from(from)
            .to(to)
            .host("localhost")
            .port("6621")
            .username("guest")
            .password("guest")
            .build();

        Move.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(run.getTo().getPath(), containsString(to));
    }

    @Test
    void moveFiletoDirectory() throws Exception {
        String from = "upload/" + IdUtils.create() + "/" + IdUtils.create() + ".yaml";
        String to = "upload/" + IdUtils.create() + "-move/" + IdUtils.create() + "/" + IdUtils.create() + "/";

        ftpUtils.upload(from);

        Move task = Move.builder()
            .id(MoveTest.class.getSimpleName())
            .type(Move.class.getName())
            .from(from)
            .to(to)
            .host("localhost")
            .port("6621")
            .username("guest")
            .password("guest")
            .build();

        Move.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(run.getTo().getPath(), containsString(to));
    }

    @Test
    void moveDirectory() throws Exception {
        String from = "upload/" + IdUtils.create() + "/" + IdUtils.create() + ".yaml";
        String to = "upload/" + IdUtils.create() + "-move/"  + IdUtils.create() + "/";

        ftpUtils.upload(from);

        Move task = Move.builder()
            .id(MoveTest.class.getSimpleName())
            .type(Move.class.getName())
            .from(FilenameUtils.getPath(from))
            .to(to)
            .host("localhost")
            .port("6621")
            .username("guest")
            .password("guest")
            .build();

        Move.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(run.getTo().getPath(), containsString(to));
    }

    @Test
    void moveFileWithSpaceToDirectoryWithSpace() throws Exception {
        //Add space in filename
        String from = "upload/" + IdUtils.create() + "/" + IdUtils.create() + " space .yaml";
        String to = "upload/" + IdUtils.create() + "-move/" + IdUtils.create() + "/" + IdUtils.create() + " space2 /";

        ftpUtils.upload(from);

        Move task = Move.builder()
                .id(MoveTest.class.getSimpleName())
                .type(Move.class.getName())
                .from(from)
                .to(to)
                .host("localhost")
                .port("6621")
                .username("guest")
                .password("guest")
                .build();

        Move.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(run.getTo().getPath(), containsString(" "));
        assertThat(run.getFrom().getPath(), containsString(" "));
        assertThat(run.getTo().getPath(), containsString(to));
    }
}
