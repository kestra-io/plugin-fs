package io.kestra.plugin.fs.ftps;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.ftp.FtpUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.hamcrest.MatcherAssert.assertThat;
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

        assertThat(IOUtils.toString(this.storageInterface.get(downloadRun.getTo()), Charsets.UTF_8), is(IOUtils.toString(this.storageInterface.get(uri), Charsets.UTF_8)));
    }
}
