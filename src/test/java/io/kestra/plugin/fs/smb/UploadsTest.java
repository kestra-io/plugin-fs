package io.kestra.plugin.fs.smb;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.vfs.models.File;
import jakarta.inject.Inject;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static io.kestra.plugin.fs.smb.SmbUtils.PASSWORD;
import static io.kestra.plugin.fs.smb.SmbUtils.USERNAME;
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
            .to(Property.of(SmbUtils.SHARE_NAME + "/" + random + "/"))
            .host(Property.of("localhost"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();
        Uploads.Output uploadsRun = uploadsTask.run(TestsUtils.mockRunContext(runContextFactory, uploadsTask, Map.of()));

        URI uri3 = smbUtils.uploadToStorage();
        URI uri4 = smbUtils.uploadToStorage();
        uploadsTask = Uploads.builder().id(UploadsTest.class.getSimpleName())
            .type(UploadsTest.class.getName())
            .from("{{ inputs.uris }}")
            .to(Property.of(SmbUtils.SHARE_NAME + "/" + random + "/"))
            .host(Property.of("localhost"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();
        Uploads.Output uploadsRunTemplate = uploadsTask.run(TestsUtils.mockRunContext(runContextFactory, uploadsTask,
            Map.of("uris", "[\"" + uri3.toString() + "\",\"" + uri4.toString() + "\"]"))
        );

        Downloads downloadsTask = Downloads.builder()
            .id(UploadsTest.class.getSimpleName())
            .type(UploadsTest.class.getName())
            .from(Property.of(SmbUtils.SHARE_NAME + "/" + random + "/"))
            .action(Property.of(Downloads.Action.DELETE))
            .host(Property.of("localhost"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        Downloads.Output downloadsRun = downloadsTask.run(TestsUtils.mockRunContext(runContextFactory, downloadsTask, Map.of()));

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
