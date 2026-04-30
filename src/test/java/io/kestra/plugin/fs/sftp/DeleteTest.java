package io.kestra.plugin.fs.sftp;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.kestra.plugin.fs.sftp.SftpUtils.PASSWORD;
import static io.kestra.plugin.fs.sftp.SftpUtils.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class DeleteTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private SftpUtils sftpUtils;

    @Test
    void run() throws Exception {
        String from = "upload/" + IdUtils.create() + "/" + IdUtils.create() + ".yaml";

        sftpUtils.upload(from);

        Delete task = Delete.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(DeleteTest.class.getName())
            .uri(Property.ofValue(from))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        Delete.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getUri().getPath(), containsString(from));
        assertThat(run.isDeleted(), is(true));
    }

    @Test
    void runDirectoryRecursive() throws Exception {
        String directory = "upload/" + IdUtils.create();
        String nestedFile = directory + "/" + IdUtils.create() + "/" + IdUtils.create() + ".yaml";

        sftpUtils.upload(nestedFile);

        Delete task = Delete.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(DeleteTest.class.getName())
            .uri(Property.ofValue(directory))
            .host(Property.ofValue("localhost"))
            .recursive(Property.ofValue(true))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        Delete.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
        Delete.Output nestedFileDelete = sftpUtils.delete(nestedFile);

        assertThat(run.getUri().getPath(), containsString(directory));
        assertThat(run.isDeleted(), is(true));
        assertThat(nestedFileDelete.isDeleted(), is(false));
    }

    @Test
    void runRegExpMatchingFiles() throws Exception {
        String directory = "upload/" + IdUtils.create();
        String matchingFile = directory + "/" + IdUtils.create() + ".csv";
        String nonMatchingFile = directory + "/" + IdUtils.create() + ".yaml";

        sftpUtils.upload(matchingFile);
        sftpUtils.upload(nonMatchingFile);

        Delete task = Delete.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(DeleteTest.class.getName())
            .uri(Property.ofValue(directory))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .regExp(Property.ofValue(".*\\.csv"))
            .build();

        Delete.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.isDeleted(), is(true));
        assertThat(run.getUris(), hasSize(1));
        assertThat(run.getUris().getFirst().getPath(), containsString(".csv"));

        // the .yaml file must still be present
        var remaining = sftpUtils.list(directory);
        assertThat(remaining.getFiles(), hasSize(1));
        assertThat(remaining.getFiles().getFirst().getName(), endsWith(".yaml"));
    }

    @Test
    void runRegExpNoMatch() throws Exception {
        String directory = "upload/" + IdUtils.create();
        String file = directory + "/" + IdUtils.create() + ".yaml";

        sftpUtils.upload(file);

        Delete task = Delete.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(DeleteTest.class.getName())
            .uri(Property.ofValue(directory))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .regExp(Property.ofValue(".*\\.csv"))
            .build();

        Delete.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.isDeleted(), is(false));
        assertThat(run.getUris(), empty());

        // the original file is untouched
        var remaining = sftpUtils.list(directory);
        assertThat(remaining.getFiles(), hasSize(1));
    }
}
