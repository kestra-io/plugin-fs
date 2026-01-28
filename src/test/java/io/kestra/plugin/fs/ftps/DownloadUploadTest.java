package io.kestra.plugin.fs.ftps;

import com.google.common.base.Charsets;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.ftp.FtpUtils;
import io.kestra.plugin.fs.vfs.models.File;
import jakarta.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static io.kestra.plugin.fs.ftp.FtpUtils.PASSWORD;
import static io.kestra.plugin.fs.ftp.FtpUtils.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

@KestraTest
class DownloadUploadTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private FtpUtils ftpUtils;

    @Inject
    private StorageInterface storageInterface;

    @BeforeAll
    static void waitForFtps() throws Exception {
        waitForPort("127.0.0.1", 6990, Duration.ofSeconds(30));
    }

    @Test
    void run() throws Exception {
        String to = IdUtils.create() + "/" + IdUtils.create() + ".yaml";

        URI uri = ftpUtils.uploadToStorage();

        var upload = Upload.builder()
            .id(DownloadUploadTest.class.getSimpleName())
            .type(DownloadUploadTest.class.getName())
            .from(Property.ofValue(uri.toString()))
            .to(Property.ofValue(to))
            .host(Property.ofValue("127.0.0.1"))
            .port(Property.ofValue("6990"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        var uploadRun = upload.run(TestsUtils.mockRunContext(runContextFactory, upload, Map.of()));

        var download = Download.builder()
            .id(DownloadUploadTest.class.getSimpleName())
            .type(DownloadUploadTest.class.getName())
            .from(Property.ofValue(uploadRun.getTo().getPath()))
            .host(Property.ofValue("127.0.0.1"))
            .port(Property.ofValue("6990"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        var downloadRun = download.run(TestsUtils.mockRunContext(runContextFactory, download, Map.of()));

        assertThat(IOUtils.toString(this.storageInterface.get(TenantService.MAIN_TENANT, null, downloadRun.getTo()), Charsets.UTF_8), is(IOUtils.toString(this.storageInterface.get(TenantService.MAIN_TENANT, null, uri), Charsets.UTF_8)));
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
                .to(Property.ofValue(sftpPath))
                .host(Property.ofValue("127.0.0.1"))
                .port(Property.ofValue("6990"))
                .username(USERNAME)
                .password(PASSWORD)
                .build();
        Uploads.Output uploadsRun = uploadsTask.run(TestsUtils.mockRunContext(runContextFactory, uploadsTask, Map.of()));

        io.kestra.plugin.fs.ftps.Downloads downloadsTask = io.kestra.plugin.fs.ftps.Downloads.builder()
                .id(DownloadUploadTest.class.getSimpleName())
                .type(DownloadUploadTest.class.getName())
                .from(Property.ofValue(sftpPath))
                .action(Property.ofValue(io.kestra.plugin.fs.ftp.Downloads.Action.DELETE))
                .host(Property.ofValue("127.0.0.1"))
                .port(Property.ofValue("6990"))
                .username(USERNAME)
                .password(PASSWORD)
                .build();

        Downloads.Output downloadsRun = downloadsTask.run(TestsUtils.mockRunContext(runContextFactory, downloadsTask, Map.of()));

        assertThat(uploadsRun.getFiles().size(), is(2));
        assertThat(downloadsRun.getFiles().size(), is(2));
        List<String> remoteFileUris = downloadsRun.getFiles().stream().map(File::getServerPath).map(URI::getPath).toList();
        assertThat(uploadsRun.getFiles().stream().map(URI::getPath).toList(), Matchers.everyItem(
                Matchers.is(Matchers.in(remoteFileUris))
        ));
    }

    private static void waitForPort(String host, int port, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), 500);
                return;
            } catch (IOException ignored) {
                Thread.sleep(200);
            }
        }

        throw new IllegalStateException("Timed out waiting for FTPS server on " + host + ":" + port);
    }
}
