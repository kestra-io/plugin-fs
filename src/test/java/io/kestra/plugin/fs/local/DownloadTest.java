package io.kestra.plugin.fs.local;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@KestraTest
class DownloadTest {
    protected static final String USER_DIR = System.getenv().getOrDefault("KESRA_LOCAL_TEST_PATH", System.getProperty("user.home"));
    private Path sourceFile;
    private final String fileContent = "test content";

    @Inject
    private RunContextFactory runContextFactory;

    @BeforeEach
    void setUp() throws IOException {
        Path tempDir = Files.createTempDirectory(Path.of(USER_DIR), "kestra-test-download-");
        sourceFile = tempDir.resolve("source-file.txt");

        Files.writeString(sourceFile, fileContent);
    }

    @Test
    void download() throws Exception {
        Download task = Download.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Download.class.getName())
            .from(Property.of(sourceFile.toString()))
            .basePath(Property.of(USER_DIR))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        Download.Output output = task.run(runContext);

        assertNotNull(output.getUri());
        assertEquals(fileContent.length(), output.getSize());

        String storedContent = new String(
            runContext.storage().getFile(output.getUri()).readAllBytes(),
            StandardCharsets.UTF_8
        );
        assertEquals(fileContent, storedContent);
    }

    @Test
    void downloadNonExistentFile() {
        Path nonExistentFile = sourceFile.getParent().resolve("non-existent.txt");
        Download task = Download.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Download.class.getName())
            .from(Property.of(nonExistentFile.toString()))
            .basePath(Property.of(USER_DIR))
            .build();

        assertThrows(
            IllegalArgumentException.class,
            () -> task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()))
        );
    }
}