package io.kestra.plugin.fs.sftp;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.vfs.Uploads.Output;
import io.kestra.plugin.fs.vfs.models.File;
import jakarta.inject.Inject;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static io.kestra.plugin.fs.sftp.SftpUtils.PASSWORD;
import static io.kestra.plugin.fs.sftp.SftpUtils.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class UploadsTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private SftpUtils sftpUtils;

    private final String random = IdUtils.create();

    @Test
    void run() throws Exception {
        URI uri1 = sftpUtils.uploadToStorage();
        URI uri2 = sftpUtils.uploadToStorage();
        Uploads uploads = Uploads.builder()
            .id(UploadsTest.class.getSimpleName())
            .type(UploadsTest.class.getName())
            .from(List.of(uri1.toString(), uri2.toString()))
            .to(Property.of("/upload/" + random))
            .host(Property.of("localhost"))
            .port(Property.of("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();
        Output uploadsRun = uploads.run(TestsUtils.mockRunContext(runContextFactory, uploads, Map.of()));

        Downloads task = Downloads.builder()
            .id(UploadsTest.class.getSimpleName())
            .type(UploadsTest.class.getName())
            .from(Property.of("/upload/" + random + "/"))
            .action(Property.of(Downloads.Action.DELETE))
            .host(Property.of("localhost"))
            .port(Property.of("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        Downloads.Output downloadsRun = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(uploadsRun.getFiles().size(), is(2));
        assertThat(downloadsRun.getFiles().size(), is(2));
        List<String> remoteFileUris = downloadsRun.getFiles().stream().map(File::getServerPath).map(URI::getPath).toList();
        assertThat(uploadsRun.getFiles().stream().map(URI::getPath).toList(), Matchers.everyItem(
                Matchers.is(Matchers.in(remoteFileUris))
        ));
    }
}
