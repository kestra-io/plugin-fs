package io.kestra.plugin.fs.smb;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

@MicronautTest
class DownloadsTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private SmbUtils smbUtils;

    @Test
    void run_DeleteAfterDownloads() throws Exception {
        String rootFolder = IdUtils.create();
        smbUtils.upload("/" + SmbUtils.SHARE_NAME + "/" + rootFolder + "/" + IdUtils.create() + ".txt");
        smbUtils.upload("/" + SmbUtils.SHARE_NAME + "/" + rootFolder + "/" + IdUtils.create() + ".txt");

        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(DownloadsTest.class.getName())
            .from("/" + SmbUtils.SHARE_NAME + "/" + rootFolder)
            .action(Downloads.Action.DELETE)
            .host("localhost")
            .port("445")
            .username("alice")
            .password("alipass")
            .build();

        Downloads.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(run.getFiles().size(), is(2));
        assertThat(run.getFiles().get(0).getPath().getPath(), endsWith(".txt"));

        run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
        assertThat(run.getFiles().isEmpty(), is(true));
    }

    @Test
    void run_MoveAfterDownloads() throws Exception {
        String rootFolder = IdUtils.create();
        smbUtils.upload("/" + SmbUtils.SHARE_NAME + "/" + rootFolder + "/" + IdUtils.create() + ".txt");
        smbUtils.upload("/" + SmbUtils.SHARE_NAME + "/" + rootFolder + "/" + IdUtils.create() + ".txt");

        String archiveShareDirectory = SmbUtils.SECOND_SHARE_NAME + "/" + rootFolder;
        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(DownloadsTest.class.getName())
            .from(SmbUtils.SHARE_NAME + "/" + rootFolder)
            .moveDirectory(archiveShareDirectory + "/")
            .action(Downloads.Action.MOVE)
            .host("localhost")
            .port("445")
            .username("alice")
            .password("alipass")
            .build();

        Downloads.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(run.getFiles().size(), is(2));
        assertThat(run.getFiles().get(0).getPath().getPath(), endsWith(".txt"));

        run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
        assertThat(run.getFiles().isEmpty(), is(true));

        task = task.toBuilder()
            .from(archiveShareDirectory)
            .build();
        run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
        assertThat(run.getFiles().size(), is(2));
        assertThat(run.getFiles().get(0).getPath().getPath(), endsWith(".txt"));
    }
}
