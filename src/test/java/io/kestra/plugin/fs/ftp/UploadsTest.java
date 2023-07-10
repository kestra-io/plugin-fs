package io.kestra.plugin.fs.ftp;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.vfs.models.File;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@MicronautTest
class UploadsTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private FtpUtils ftpUtils;

    @Value("${kestra.variables.globals.random}")
    private String random;

    @Test
    void run() throws Exception {
        URI uri1 = ftpUtils.uploadToStorage();
        URI uri2 = ftpUtils.uploadToStorage();

        Uploads uploadsTask = Uploads.builder().id(UploadsTest.class.getSimpleName())
                .type(UploadsTest.class.getName())
                .from(new String[]{uri1.toString(), uri2.toString()})
                .to("/upload/" + random + "/")
                .host("localhost")
                .port("6621")
                .username("guest")
                .password("guest")
                .build();
        Uploads.Output uploadsRun = uploadsTask.run(TestsUtils.mockRunContext(runContextFactory, uploadsTask, ImmutableMap.of()));

        Downloads downloadsTask = Downloads.builder()
                .id(UploadsTest.class.getSimpleName())
                .type(UploadsTest.class.getName())
                .from("/upload/" + random + "/")
                .action(Downloads.Action.DELETE)
                .host("localhost")
                .port("6621")
                .username("guest")
                .password("guest")
                .build();

        Downloads.Output downloadsRun = downloadsTask.run(TestsUtils.mockRunContext(runContextFactory, downloadsTask, ImmutableMap.of()));

        assertThat(uploadsRun.getFiles().size(), is(2));
        assertThat(downloadsRun.getFiles().size(), is(2));
        List<String> remoteFileUris = downloadsRun.getFiles().stream().map(File::getServerPath).map(URI::getPath).toList();
        assertThat(uploadsRun.getFiles().stream().map(URI::getPath).toList(), Matchers.everyItem(
                Matchers.is(Matchers.in(remoteFileUris))
        ));
    }
}
