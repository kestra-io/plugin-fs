package io.kestra.plugin.fs.smb;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.kestra.plugin.fs.smb.SmbUtils.PASSWORD;
import static io.kestra.plugin.fs.smb.SmbUtils.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

@KestraTest
class DownloadsTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private SmbUtils smbUtils;

    @Test
    void run_DeleteAfterDownloads() throws Exception {
        String rootFolder = IdUtils.create();
        String toUploadDir = "/" + SmbUtils.SHARE_NAME + "/" + rootFolder;
        smbUtils.upload(toUploadDir + "/" + IdUtils.create() + ".txt");
        smbUtils.upload(toUploadDir + "/" + IdUtils.create() + ".txt");

        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(DownloadsTest.class.getName())
            .from(Property.of(toUploadDir))
            .action(Property.of(Downloads.Action.DELETE))
            .host(Property.of("localhost"))
            .port(Property.of("445"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        Downloads.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(2));
        assertThat(run.getFiles().getFirst().getPath().getPath(), endsWith(".txt"));
        assertThat(run.getOutputFiles().size(), is(2));

        assertThat(smbUtils.list(toUploadDir).getFiles().isEmpty(), is(true));
    }

    @Test
    void run_MoveAfterDownloads() throws Exception {
        String rootFolder = IdUtils.create();
        String toUploadDir = "/" + SmbUtils.SHARE_NAME + "/" + rootFolder;
        smbUtils.upload(toUploadDir + "/" + IdUtils.create() + ".txt");
        smbUtils.upload(toUploadDir + "/" + IdUtils.create() + ".txt");

        String archiveShareDirectory = SmbUtils.SECOND_SHARE_NAME + "/" + rootFolder;
        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(DownloadsTest.class.getName())
            .from(Property.of(toUploadDir))
            .moveDirectory(Property.of(archiveShareDirectory + "/"))
            .action(Property.of(Downloads.Action.MOVE))
            .host(Property.of("localhost"))
            .port(Property.of("445"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        Downloads.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(2));
        assertThat(run.getFiles().getFirst().getPath().getPath(), endsWith(".txt"));
        assertThat(run.getOutputFiles().size(), is(2));

        run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
        assertThat(run.getFiles().isEmpty(), is(true));

        task = task.toBuilder()
            .from(Property.of(archiveShareDirectory))
            .build();
        run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
        assertThat(run.getFiles().size(), is(2));
        assertThat(run.getFiles().getFirst().getPath().getPath(), endsWith(".txt"));
        assertThat(run.getOutputFiles().size(), is(2));

        assertThat(smbUtils.list(toUploadDir).getFiles().isEmpty(), is(true));
        assertThat(smbUtils.list(archiveShareDirectory).getFiles().size(), is(2));
    }

    @Test
    void run_NoneAfterDownloads() throws Exception {
        String rootFolder = IdUtils.create();
        String toUploadDir = "/" + SmbUtils.SHARE_NAME + "/" + rootFolder;
        smbUtils.upload(toUploadDir + "/" + IdUtils.create() + ".txt");
        smbUtils.upload(toUploadDir + "/" + IdUtils.create() + ".txt");

        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(DownloadsTest.class.getName())
            .from(Property.of(toUploadDir))
            .action(Property.of(Downloads.Action.NONE))
            .host(Property.of("localhost"))
            .port(Property.of("445"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        Downloads.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(2));
        assertThat(run.getFiles().getFirst().getPath().getPath(), endsWith(".txt"));
        assertThat(run.getOutputFiles().size(), is(2));

        assertThat(smbUtils.list(toUploadDir).getFiles().size(), is(2));
    }
}
