package io.kestra.plugin.fs.ftps;

import com.google.common.base.Charsets;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.ftp.FtpUtils;
import io.kestra.plugin.fs.vfs.models.File;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.apache.commons.io.IOUtils;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;

import static io.kestra.plugin.fs.ftp.FtpUtils.PASSWORD;
import static io.kestra.plugin.fs.ftp.FtpUtils.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
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

    @Inject
    @Named(QueueFactoryInterface.WORKERTASKLOG_NAMED)
    private QueueInterface<LogEntry> logQueue;

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

    @Test
    void insecureTrustAllCertificates_logsWarning() throws Exception {
        List<LogEntry> logs = new CopyOnWriteArrayList<>();
        var receive = TestsUtils.receive(logQueue, l -> logs.add(l.getLeft()));

        URI uri = ftpUtils.uploadToStorage();

        var upload = Upload.builder()
            .id(DownloadUploadTest.class.getSimpleName())
            .type(DownloadUploadTest.class.getName())
            .from(Property.ofValue(uri.toString()))
            .to(Property.ofValue(IdUtils.create() + "/" + IdUtils.create() + ".yaml"))
            .host(Property.ofValue("127.0.0.1"))
            .port(Property.ofValue("6990"))
            .username(USERNAME)
            .password(PASSWORD)
            .insecureTrustAllCertificates(Property.ofValue(true))
            .build();

        upload.run(TestsUtils.mockRunContext(runContextFactory, upload, Map.of()));

        String expectedLog = "insecureTrustAllCertificates";
        TestsUtils.awaitLog(logs, log -> log.getMessage() != null && log.getMessage().contains(expectedLog));
        receive.blockLast();
        assertThat(logs.stream().anyMatch(log -> log.getMessage() != null && log.getMessage().contains(expectedLog)), is(true));
    }

    @ParameterizedTest
    @ValueSource(strings = {"report[1].txt", "report[100%].txt"})
    void shouldUploadListDownloadAndDeleteWhenNameContainsBrackets(String fileName) throws Exception {
        var dir = "/upload/" + IdUtils.create();
        var path = dir + "/" + fileName;
        var source = ftpUtils.uploadToStorage();

        var upload = Upload.builder()
            .id(IdUtils.create())
            .type(Upload.class.getName())
            .from(Property.ofValue(source.toString()))
            .to(Property.ofValue(path))
            .host(Property.ofValue("127.0.0.1"))
            .port(Property.ofValue("6990"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();
        upload.run(TestsUtils.mockRunContext(runContextFactory, upload, Map.of()));

        var list = io.kestra.plugin.fs.ftps.List.builder()
            .id(IdUtils.create())
            .type(io.kestra.plugin.fs.ftps.List.class.getName())
            .from(Property.ofValue(dir))
            .regExp(Property.ofValue(".*/" + Pattern.quote(fileName)))
            .host(Property.ofValue("127.0.0.1"))
            .port(Property.ofValue("6990"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();
        var listRun = list.run(TestsUtils.mockRunContext(runContextFactory, list, Map.of()));
        assertThat(listRun.getFiles().stream().map(File::getName).toList(), contains(fileName));

        var download = Download.builder()
            .id(IdUtils.create())
            .type(Download.class.getName())
            .from(Property.ofValue(path))
            .host(Property.ofValue("127.0.0.1"))
            .port(Property.ofValue("6990"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();
        var downloadRun = download.run(TestsUtils.mockRunContext(runContextFactory, download, Map.of()));
        assertThat(
            IOUtils.toString(this.storageInterface.get(TenantService.MAIN_TENANT, null, downloadRun.getTo()), Charsets.UTF_8),
            is(IOUtils.toString(this.storageInterface.get(TenantService.MAIN_TENANT, null, source), Charsets.UTF_8))
        );

        var delete = Delete.builder()
            .id(IdUtils.create())
            .type(Delete.class.getName())
            .uri(Property.ofValue(path))
            .errorOnMissing(Property.ofValue(true))
            .host(Property.ofValue("127.0.0.1"))
            .port(Property.ofValue("6990"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();
        assertThat(delete.run(TestsUtils.mockRunContext(runContextFactory, delete, Map.of())).isDeleted(), is(true));
        assertThat(list.run(TestsUtils.mockRunContext(runContextFactory, list, Map.of())).getFiles(), is(empty()));
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
