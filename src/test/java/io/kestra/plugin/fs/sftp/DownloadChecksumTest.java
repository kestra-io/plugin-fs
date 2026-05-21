package io.kestra.plugin.fs.sftp;

import io.kestra.core.exceptions.KestraRuntimeException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.vfs.ChecksumService;
import io.kestra.plugin.fs.vfs.Download.Output;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;

import static io.kestra.plugin.fs.sftp.SftpUtils.PASSWORD;
import static io.kestra.plugin.fs.sftp.SftpUtils.USERNAME;
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
    private SftpUtils sftpUtils;

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
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD);
    }

    private String uploadFixture() throws Exception {
        String remotePath = "upload/" + IdUtils.create() + ".txt";
        sftpUtils.update(remotePath, CONTENT);
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
        String remotePath = uploadFixture();

        Download task = downloadBuilder(remotePath)
            .validateChecksum(Property.ofValue(true))
            .checksumAlgorithm(Property.ofValue(ChecksumService.Algorithm.MD5))
            .checksumExpected(Property.ofValue(md5(CONTENT)))
            .build();

        Output output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.getChecksum(), is(md5(CONTENT)));
    }
}
