package io.kestra.plugin.fs.local;

import io.kestra.core.exceptions.KestraRuntimeException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.vfs.ChecksumService;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Comparator;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.*;

@KestraTest
class DownloadTest {

    private Path sourceFile;
    private final String fileContent = "test content";
    // SHA-256 of "test content"
    private static final String FILE_CONTENT_SHA_256 = "6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72";
    // MD5 of "test content"
    private static final String FILE_CONTENT_MD5 = "9473fdd0d880a43c21b7778d34872157";

    @Inject
    private RunContextFactory runContextFactory;

    @BeforeEach
    void setUp() throws IOException {
        Path tempDir = Files.createTempDirectory(Path.of(Paths.get("/tmp").toAbsolutePath().toUri()), "kestra-test-download-");
        sourceFile = tempDir.resolve("source-file.txt");

        Files.writeString(sourceFile, fileContent);
    }

    @Test
    void download() throws Exception {
        Download task = Download.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Download.class.getName())
            .from(Property.ofValue(sourceFile.toString()))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        Download.Output output = task.run(runContext);

        assertThat(output.getUri(), notNullValue());
        assertThat((long) fileContent.length(), is(output.getSize()));

        String storedContent = new String(
            runContext.storage().getFile(output.getUri()).readAllBytes(),
            StandardCharsets.UTF_8
        );
        assertThat(fileContent, is(storedContent));
    }

    @Test
    void downloadComputesChecksumByDefault() throws Exception {
        Download task = Download.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Download.class.getName())
            .from(Property.ofValue(sourceFile.toString()))
            .build();

        Download.Output output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.getChecksum(), is(FILE_CONTENT_SHA_256));
    }

    @Test
    void downloadWithValidChecksum() throws Exception {
        Download task = Download.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Download.class.getName())
            .from(Property.ofValue(sourceFile.toString()))
            .validateChecksum(Property.ofValue(true))
            .checksumExpected(Property.ofValue(FILE_CONTENT_SHA_256))
            .build();

        Download.Output output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.getUri(), notNullValue());
        assertThat(output.getChecksum(), is(FILE_CONTENT_SHA_256));
    }

    @Test
    void downloadChecksumMismatchFails() {
        Download task = Download.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Download.class.getName())
            .from(Property.ofValue(sourceFile.toString()))
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
    void downloadValidateChecksumWithoutExpectedFails() {
        Download task = Download.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Download.class.getName())
            .from(Property.ofValue(sourceFile.toString()))
            .validateChecksum(Property.ofValue(true))
            .build();

        KestraRuntimeException ex = assertThrows(
            KestraRuntimeException.class,
            () -> task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()))
        );
        assertThat(ex.getMessage(), containsString("checksumExpected"));
    }

    @Test
    void downloadWithMd5Algorithm() throws Exception {
        Download task = Download.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Download.class.getName())
            .from(Property.ofValue(sourceFile.toString()))
            .validateChecksum(Property.ofValue(true))
            .checksumAlgorithm(Property.ofValue(ChecksumService.Algorithm.MD5))
            .checksumExpected(Property.ofValue(FILE_CONTENT_MD5))
            .build();

        Download.Output output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.getChecksum(), is(FILE_CONTENT_MD5));
    }

    @Test
    void downloadNonExistentFile() throws IOException {
        Path nonExistentFile = sourceFile.getParent().resolve("non-existent.txt");
        Download task = Download.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Download.class.getName())
            .from(Property.ofValue(nonExistentFile.toString()))
            .build();

        assertThrows(
            IllegalArgumentException.class,
            () -> task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()))
        );
    }

    @Test
    void downloadsWithMaxFiles() throws Exception {
        Path tempDir = Files.createTempDirectory(Path.of(Paths.get("/tmp").toAbsolutePath().toUri()), "kestra-test-downloads-");
        try {
            Files.writeString(tempDir.resolve("file1.txt"), "content1");
            Files.writeString(tempDir.resolve("file2.txt"), "content2");

            Downloads task = Downloads.builder()
                .id(DownloadTest.class.getSimpleName())
                .type(Downloads.class.getName())
                .from(Property.ofValue(tempDir.toString()))
                .maxFiles(Property.ofValue(1))
                .build();

            Downloads.Output output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

            assertThat(output.getFiles().size(), is(1));
            assertThat(output.getOutputFiles().size(), is(1));
        } finally {
            Files.walk(tempDir)
                .sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException ignored) {
                    }
                });
        }
    }

    @Test
    void downloadsMoveSingleFile() throws Exception {
        Path allowedRoot = Path.of("/tmp");
        Files.createDirectories(allowedRoot);

        Path sourceRoot = Files.createTempDirectory(allowedRoot, "kestra-test-move-single-src-");
        Path sourceFile = sourceRoot.resolve("report.csv");
        Path archiveRoot = Files.createTempDirectory(allowedRoot, "kestra-test-move-single-dst-");

        try {
            Files.writeString(sourceFile, "single file data");

            Downloads task = Downloads.builder()
                .id(DownloadTest.class.getSimpleName())
                .type(Downloads.class.getName())
                .from(Property.ofValue(sourceFile.toString()))
                .action(Property.ofValue(Downloads.Action.MOVE))
                .moveDirectory(Property.ofValue(archiveRoot.toString()))
                .build();

            Downloads.Output output = task.run(
                TestsUtils.mockRunContext(runContextFactory, task, Map.of())
            );

            Path destination = archiveRoot.resolve("report.csv");

            assertTrue(Files.exists(destination), "The moved file must exist in moveDirectory");
            assertThat(Files.readString(destination), is("single file data"));
            assertFalse(Files.exists(sourceFile), "The original file must be removed after MOVE");
            assertThat(
                output.getFiles().stream().filter(file -> !file.isDirectory()).count(),
                is(1L)
            );
        } finally {
            try (var paths = Files.walk(sourceRoot)) {
                paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            try (var paths = Files.walk(archiveRoot)) {
                paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }
    }

    @Test
    void downloadsMovePrefixNested() throws Exception {
        // Build source tree:
        //   source/team-A/report.csv
        //   source/team-B/report.csv
        Path sourceRoot = Files.createTempDirectory(
            Path.of(Paths.get("/tmp").toAbsolutePath().toUri()),
            "kestra-test-move-nested-src-"
        );
        Path archiveRoot = Files.createTempDirectory(
            Path.of(Paths.get("/tmp").toAbsolutePath().toUri()),
            "kestra-test-move-nested-dst-"
        );
        try {
            Path teamA = Files.createDirectories(sourceRoot.resolve("team-A"));
            Path teamB = Files.createDirectories(sourceRoot.resolve("team-B"));

            Path srcA = teamA.resolve("report.csv");
            Path srcB = teamB.resolve("report.csv");

            Files.writeString(srcA, "team-A data");
            Files.writeString(srcB, "team-B data");

            Downloads task = Downloads.builder()
                .id(DownloadTest.class.getSimpleName())
                .type(Downloads.class.getName())
                .from(Property.ofValue(sourceRoot.toString()))
                .recursive(Property.ofValue(true))
                .action(Property.ofValue(Downloads.Action.MOVE))
                .moveDirectory(Property.ofValue(archiveRoot.toString()))
                .build();

            Downloads.Output output = task.run(
                TestsUtils.mockRunContext(runContextFactory, task, Map.of())
            );

            // Both files must appear in the output
            long fileCount = output.getFiles().stream()
                .filter(f -> !f.isDirectory())
                .count();
            assertThat(fileCount, is(2L));

            // Destination paths must preserve the sub-directory structure
            Path dstA = archiveRoot.resolve("team-A").resolve("report.csv");
            Path dstB = archiveRoot.resolve("team-B").resolve("report.csv");

            assertTrue(Files.exists(dstA), "team-A/report.csv must exist in archive");
            assertTrue(Files.exists(dstB), "team-B/report.csv must exist in archive");

            assertThat(Files.readString(dstA), is("team-A data"));
            assertThat(Files.readString(dstB), is("team-B data"));

            // Original files must have been removed
            assertFalse(Files.exists(srcA), "source team-A/report.csv must be removed after MOVE");
            assertFalse(Files.exists(srcB), "source team-B/report.csv must be removed after MOVE");

        } finally {
            try (var paths = Files.walk(sourceRoot)) {
                paths.sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
            }

            try (var paths = Files.walk(archiveRoot)) {
                paths.sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
            }
        }
    }
}
