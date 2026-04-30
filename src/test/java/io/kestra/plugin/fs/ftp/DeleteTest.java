package io.kestra.plugin.fs.ftp;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.vfs.Delete;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.kestra.plugin.fs.ftp.FtpUtils.PASSWORD;
import static io.kestra.plugin.fs.ftp.FtpUtils.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class DeleteTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private FtpUtils ftpUtils;

    @Test
    void run() throws Exception {
        String from = IdUtils.create() + "/" + IdUtils.create() + ".yaml";

        ftpUtils.upload(from);

        io.kestra.plugin.fs.ftp.Delete task;
        task = io.kestra.plugin.fs.ftp.Delete.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(DeleteTest.class.getName())
            .uri(Property.ofValue(from))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6621"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        Delete.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getUri().getPath(), containsString(from));
        assertThat(run.isDeleted(), is(true));
    }

    @Test
    void runDirectoryRecursive() throws Exception {
        String directory = IdUtils.create();
        String nestedFile = directory + "/" + IdUtils.create() + "/" + IdUtils.create() + ".yaml";

        ftpUtils.upload(nestedFile);

        io.kestra.plugin.fs.ftp.Delete task;
        task = io.kestra.plugin.fs.ftp.Delete.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(DeleteTest.class.getName())
            .uri(Property.ofValue(directory))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6621"))
            .recursive(Property.ofValue(true))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        Delete.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
        Delete.Output nestedFileDelete = ftpUtils.delete(nestedFile);

        assertThat(run.getUri().getPath(), containsString(directory));
        assertThat(run.isDeleted(), is(true));
        assertThat(nestedFileDelete.isDeleted(), is(false));
    }

    @Test
    void runRegExpMatchingFiles() throws Exception {
        String directory = IdUtils.create();
        String matchingFile = directory + "/" + IdUtils.create() + ".csv";
        String nonMatchingFile = directory + "/" + IdUtils.create() + ".yaml";

        ftpUtils.upload(matchingFile);
        ftpUtils.upload(nonMatchingFile);

        io.kestra.plugin.fs.ftp.Delete task = io.kestra.plugin.fs.ftp.Delete.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(DeleteTest.class.getName())
            .uri(Property.ofValue(directory))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6621"))
            .username(USERNAME)
            .password(PASSWORD)
            .regExp(Property.ofValue(".*\\.csv"))
            .build();

        Delete.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.isDeleted(), is(true));
        assertThat(run.getUris(), hasSize(1));
        assertThat(run.getUris().getFirst().getPath(), containsString(".csv"));

        // the .yaml file must still be present
        var remaining = ftpUtils.list(directory);
        assertThat(remaining.getFiles(), hasSize(1));
        assertThat(remaining.getFiles().getFirst().getName(), endsWith(".yaml"));
    }
}
