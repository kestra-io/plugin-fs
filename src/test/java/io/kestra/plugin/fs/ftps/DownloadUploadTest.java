package io.kestra.plugin.fs.ftps;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.ftp.FtpUtils;
import io.kestra.plugin.fs.vfs.models.File;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

@MicronautTest
class DownloadUploadTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private FtpUtils ftpUtils;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void run() throws Exception {
        String to = IdUtils.create() + "/" + IdUtils.create() + ".yaml";

        URI uri = ftpUtils.uploadToStorage();

        var upload = Upload.builder()
            .id(DownloadUploadTest.class.getSimpleName())
            .type(DownloadUploadTest.class.getName())
            .from(uri.toString())
            .to(to)
            .host("127.0.0.1")
            .port("6990")
            .username("guest")
            .password("guest")
            .build();

        var uploadRun = upload.run(TestsUtils.mockRunContext(runContextFactory, upload, ImmutableMap.of()));

        var download = Download.builder()
            .id(DownloadUploadTest.class.getSimpleName())
            .type(DownloadUploadTest.class.getName())
            .from(uploadRun.getTo().getPath())
            .host("127.0.0.1")
            .port("6990")
            .username("guest")
            .password("guest")
            .build();

        var downloadRun = download.run(TestsUtils.mockRunContext(runContextFactory, download, ImmutableMap.of()));

        assertThat(IOUtils.toString(this.storageInterface.get(null, downloadRun.getTo()), Charsets.UTF_8), is(IOUtils.toString(this.storageInterface.get(null, uri), Charsets.UTF_8)));
        assertThat(downloadRun.getFrom().getPath(), endsWith(".yaml"));
    }



    @Test
    void downloadsUploads() throws Exception {
        URI uri1 = ftpUtils.uploadToStorage();
        URI uri2 = ftpUtils.uploadToStorage();

        String sftpPath = "/upload/" + IdUtils.create() + "/";

        Uploads uploadsTask = Uploads.builder().id(DownloadUploadTest.class.getSimpleName())
                .type(DownloadUploadTest.class.getName())
                .from(List.of(uri1.toString(), uri2.toString()))
                .to(sftpPath)
                .host("127.0.0.1")
                .port("6990")
                .username("guest")
                .password("guest")
                .build();
        Uploads.Output uploadsRun = uploadsTask.run(TestsUtils.mockRunContext(runContextFactory, uploadsTask, ImmutableMap.of()));

        io.kestra.plugin.fs.ftps.Downloads downloadsTask = io.kestra.plugin.fs.ftps.Downloads.builder()
                .id(DownloadUploadTest.class.getSimpleName())
                .type(DownloadUploadTest.class.getName())
                .from(sftpPath)
                .action(io.kestra.plugin.fs.ftp.Downloads.Action.DELETE)
                .host("127.0.0.1")
                .port("6990")
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
