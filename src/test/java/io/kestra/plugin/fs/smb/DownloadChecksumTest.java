package io.kestra.plugin.fs.smb;

import io.kestra.core.exceptions.KestraRuntimeException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.vfs.ChecksumService;
import io.kestra.plugin.fs.vfs.Download.Output;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.kestra.plugin.fs.smb.SmbUtils.PASSWORD;
import static io.kestra.plugin.fs.smb.SmbUtils.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class DownloadChecksumTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private SmbUtils smbUtils;

    @Inject
    @Named(QueueFactoryInterface.WORKERTASKLOG_NAMED)
    private QueueInterface<LogEntry> logQueue;

    private static final String CONTENT = "deterministic content for checksum tests";

    private static String sha256(String value) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static String md5(String value) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("MD5");
        return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
    }

    private Download.DownloadBuilder<?, ?> downloadBuilder(String remotePath) {
        return Download.builder()
            .id(DownloadChecksumTest.class.getSimpleName())
            .type(Download.class.getName())
            .from(Property.ofValue(remotePath))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("445"))
            .username(USERNAME)
            .password(PASSWORD);
    }

    private String uploadFixture() throws Exception {
        String remotePath = "/" + SmbUtils.SHARE_NAME + "/" + IdUtils.create() + ".txt";
        smbUtils.update(remotePath, CONTENT);
        return remotePath;
    }

    @Test
    void downloadWithMatchingChecksum() throws Exception {
        String remotePath = uploadFixture();

        Download task = downloadBuilder(remotePath)
            .validateChecksum(Property.ofValue(true))
            .checksumExpected(Property.ofValue(sha256(CONTENT)))
            .build();

        Output output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.getTo(), notNullValue());
        assertThat(output.getChecksum(), is(sha256(CONTENT)));
    }

    @Test
    void downloadChecksumMismatchFails() throws Exception {
        String remotePath = uploadFixture();

        Download task = downloadBuilder(remotePath)
            .validateChecksum(Property.ofValue(true))
            .checksumExpected(Property.ofValue("deadbeef"))
            .build();

        KestraRuntimeException ex = assertThrows(
            KestraRuntimeException.class,
            () -> task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()))
        );
        assertThat(ex.getMessage(), containsString("Checksum mismatch"));
    }

    @Test
    void downloadValidateChecksumWithoutExpectedFails() throws Exception {
        String remotePath = uploadFixture();

        Download task = downloadBuilder(remotePath)
            .validateChecksum(Property.ofValue(true))
            .build();

        KestraRuntimeException ex = assertThrows(
            KestraRuntimeException.class,
            () -> task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()))
        );
        assertThat(ex.getMessage(), containsString("checksumExpected"));
    }

    @Test
    void downloadWithoutValidationStillExposesChecksum() throws Exception {
        String remotePath = uploadFixture();

        Download task = downloadBuilder(remotePath).build();

        Output output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.getChecksum(), is(sha256(CONTENT)));
    }

    @Test
    void downloadWithMd5Algorithm() throws Exception {
        List<LogEntry> logs = new CopyOnWriteArrayList<>();
        var receive = TestsUtils.receive(logQueue, l -> logs.add(l.getLeft()));

        String remotePath = uploadFixture();

        Download task = downloadBuilder(remotePath)
            .validateChecksum(Property.ofValue(true))
            .checksumAlgorithm(Property.ofValue(ChecksumService.Algorithm.MD5))
            .checksumExpected(Property.ofValue(md5(CONTENT)))
            .build();

        Output output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.getChecksum(), is(md5(CONTENT)));

        TestsUtils.awaitLog(logs, log -> log.getMessage() != null && log.getMessage().contains("deprecated"));
        receive.blockLast();
        assertThat(logs.stream().anyMatch(log -> log.getMessage() != null && log.getMessage().contains("MD5") && log.getMessage().contains("deprecated")), is(true));
    }
}
