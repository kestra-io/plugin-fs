package io.kestra.plugin.fs.smb;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.codelibs.jcifs.smb.impl.SmbException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.kestra.plugin.fs.smb.SmbUtils.PASSWORD;
import static io.kestra.plugin.fs.smb.SmbUtils.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.hasSize;

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
            .from(Property.ofValue(from))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("445"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, fetch, Map.of());
        Assertions.assertDoesNotThrow(() -> fetch.run(runContext));

        io.kestra.plugin.fs.smb.Delete task;
        task = io.kestra.plugin.fs.smb.Delete.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(DeleteTest.class.getName())
            .uri(Property.ofValue(from))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("445"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        io.kestra.plugin.fs.vfs.Delete.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getUri().getPath(), endsWith(from));
        assertThat(run.isDeleted(), is(true));

        Assertions.assertThrows(
            SmbException.class,
            () -> fetch.run(TestsUtils.mockRunContext(runContextFactory, fetch, Map.of()))
        );
    }

    @Test
    void runWithRegExp() throws Exception {
        String dir = "/" + IdUtils.create();
        String basePath = SmbUtils.SHARE_NAME + dir;

        // Upload two CSV files and one YAML file into the same directory
        smbUtils.upload(basePath + "/file1.csv");
        smbUtils.upload(basePath + "/file2.csv");
        smbUtils.upload(basePath + "/keep.yaml");

        var task = io.kestra.plugin.fs.smb.Delete.builder()
            .id(IdUtils.create())
            .type(io.kestra.plugin.fs.smb.Delete.class.getName())
            .uri(Property.ofValue(basePath + "/"))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("445"))
            .username(USERNAME)
            .password(PASSWORD)
            .regExp(Property.ofValue(".*\\.csv"))
            .build();

        var output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        // Two CSV files deleted, the YAML file kept
        assertThat(output.isDeleted(), is(true));
        assertThat(output.getUris(), hasSize(2));

        // Confirm the YAML file still exists
        var listTask = List.builder()
            .id(IdUtils.create())
            .type(List.class.getName())
            .from(Property.ofValue(basePath))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("445"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        var listOutput = listTask.run(TestsUtils.mockRunContext(runContextFactory, listTask, Map.of()));
        assertThat(listOutput.getFiles(), hasSize(1));
        assertThat(listOutput.getFiles().getFirst().getName(), is("keep.yaml"));
    }
}
