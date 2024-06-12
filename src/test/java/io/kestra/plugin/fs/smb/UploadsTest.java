package io.kestra.plugin.fs.smb;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.vfs.models.File;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class UploadsTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private SmbUtils smbUtils;

    private final String random = IdUtils.create();

    @Test
    void run() throws Exception {
        URI uri1 = smbUtils.uploadToStorage();
        URI uri2 = smbUtils.uploadToStorage();

        Uploads uploadsTask = Uploads.builder().id(UploadsTest.class.getSimpleName())
            .type(UploadsTest.class.getName())
            .from(List.of(uri1.toString(), uri2.toString()))
            .to(SmbUtils.SHARE_NAME + "/" + random + "/")
            .host("localhost")
            .username("alice")
            .password("alipass")
            .build();
        Uploads.Output uploadsRun = uploadsTask.run(TestsUtils.mockRunContext(runContextFactory, uploadsTask, ImmutableMap.of()));

        URI uri3 = smbUtils.uploadToStorage();
        URI uri4 = smbUtils.uploadToStorage();
        uploadsTask = Uploads.builder().id(UploadsTest.class.getSimpleName())
            .type(UploadsTest.class.getName())
            .from("{{ inputs.uris }}")
            .to(SmbUtils.SHARE_NAME + "/" + random + "/")
            .host("localhost")
            .username("alice")
            .password("alipass")
            .build();
        Uploads.Output uploadsRunTemplate = uploadsTask.run(TestsUtils.mockRunContext(runContextFactory, uploadsTask,
            ImmutableMap.of("uris", "[\"" + uri3.toString() + "\",\"" + uri4.toString() + "\"]"))
        );

        Downloads downloadsTask = Downloads.builder()
            .id(UploadsTest.class.getSimpleName())
            .type(UploadsTest.class.getName())
            .from(SmbUtils.SHARE_NAME + "/" + random + "/")
            .action(Downloads.Action.DELETE)
            .host("localhost")
            .username("alice")
            .password("alipass")
            .build();

        Downloads.Output downloadsRun = downloadsTask.run(TestsUtils.mockRunContext(runContextFactory, downloadsTask, ImmutableMap.of()));

        assertThat(uploadsRun.getFiles().size(), is(2));
        assertThat(uploadsRunTemplate.getFiles().size(), is(2));
        assertThat(downloadsRun.getFiles().size(), is(4));
        List<String> remoteFileUris = downloadsRun.getFiles().stream().map(File::getServerPath).map(URI::getPath).toList();
        assertThat(uploadsRun.getFiles().stream().map(URI::getPath).toList(), Matchers.everyItem(
            Matchers.is(Matchers.in(remoteFileUris))
        ));
        assertThat(uploadsRunTemplate.getFiles().stream().map(URI::getPath).toList(), Matchers.everyItem(
            Matchers.is(Matchers.in(remoteFileUris))
        ));
    }
}
