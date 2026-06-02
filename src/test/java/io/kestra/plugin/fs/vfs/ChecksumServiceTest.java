package io.kestra.plugin.fs.vfs;

import io.kestra.core.exceptions.KestraRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ChecksumServiceTest {

    // SHA-256 of "hello world"
    private static final String HELLO_WORLD_SHA_256 = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";
    // MD5 of "hello world"
    private static final String HELLO_WORLD_MD5 = "5eb63bbbe01eeed093cb22bb8f5acdc3";

    @TempDir
    private Path tempDir;

    private Path file;

    @BeforeEach
    void setUp() throws IOException {
        file = tempDir.resolve("input.txt");
        Files.writeString(file, "hello world", StandardCharsets.UTF_8);
    }

    @Test
    void computeSha256() throws IOException {
        String computed = ChecksumService.compute(file, ChecksumService.Algorithm.SHA_256);
        assertThat(computed, is(HELLO_WORLD_SHA_256));
    }

    @Test
    void computeMd5() throws IOException {
        String computed = ChecksumService.compute(file, ChecksumService.Algorithm.MD5);
        assertThat(computed, is(HELLO_WORLD_MD5));
    }

    @Test
    void verifyMatchReturnsComputedDigest() throws IOException {
        String result = ChecksumService.verify(file, ChecksumService.Algorithm.SHA_256, HELLO_WORLD_SHA_256);
        assertThat(result, is(HELLO_WORLD_SHA_256));
    }

    @Test
    void verifyIsCaseInsensitive() throws IOException {
        String result = ChecksumService.verify(file, ChecksumService.Algorithm.SHA_256, HELLO_WORLD_SHA_256.toUpperCase());
        assertThat(result, equalToIgnoringCase(HELLO_WORLD_SHA_256));
    }

    @Test
    void verifyTrimsExpectedValue() throws IOException {
        String result = ChecksumService.verify(file, ChecksumService.Algorithm.SHA_256, "  " + HELLO_WORLD_SHA_256 + "  ");
        assertThat(result, is(HELLO_WORLD_SHA_256));
    }

    @Test
    void verifyMismatchFails() {
        KestraRuntimeException ex = assertThrows(
            KestraRuntimeException.class,
            () -> ChecksumService.verify(file, ChecksumService.Algorithm.SHA_256, "deadbeef")
        );
        assertThat(ex.getMessage(), containsString("Checksum mismatch"));
        assertThat(ex.getMessage(), containsString("deadbeef"));
        assertThat(ex.getMessage(), containsString(HELLO_WORLD_SHA_256));
    }

    @Test
    void verifyMissingExpectedFails() {
        KestraRuntimeException ex = assertThrows(
            KestraRuntimeException.class,
            () -> ChecksumService.verify(file, ChecksumService.Algorithm.SHA_256, null)
        );
        assertThat(ex.getMessage(), containsString("checksumExpected"));
    }

    @Test
    void verifyBlankExpectedFails() {
        assertThrows(
            KestraRuntimeException.class,
            () -> ChecksumService.verify(file, ChecksumService.Algorithm.SHA_256, "   ")
        );
    }
}
