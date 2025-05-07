package io.kestra.plugin.fs.local;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.*;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@KestraTest
class CopyTest {
    protected static final String USER_DIR = System.getenv().getOrDefault("KESRA_LOCAL_TEST_PATH", System.getProperty("user.home"));
    private Path sourceFile;
    private Path targetFile;

    @Inject
    private RunContextFactory runContextFactory;

    @BeforeEach
    void setUp() throws IOException {
        Path tempDir = Files.createTempDirectory(Path.of(USER_DIR), "kestra-test-copy-");
        sourceFile = tempDir.resolve("file1.csv");
        targetFile = tempDir.resolve("file2.csv");

        Files.createFile(sourceFile);
    }

    @AfterEach
    void tearDown() throws IOException {
        Files.deleteIfExists(targetFile);
        Files.deleteIfExists(sourceFile);
    }

    @Test
    void copyFile() throws Exception {
        Copy task = Copy.builder()
            .id(CopyTest.class.getSimpleName())
            .type(Copy.class.getName())
            .from(Property.of(sourceFile.toString()))
            .to(Property.of(targetFile.toString()))
            .basePath(Property.of(USER_DIR))
            .overwrite(Property.of(true))
            .build();

        Copy.Output output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertTrue(Files.exists(targetFile));
        assertTrue(Files.exists(sourceFile));
    }

    @Test
    void copyFileWhenTargetExistsAndOverwriteIsFalse() throws Exception {
        Files.createFile(targetFile);

        Copy task = Copy.builder()
            .id(CopyTest.class.getSimpleName())
            .type(Copy.class.getName())
            .from(Property.of(sourceFile.toString()))
            .to(Property.of(targetFile.toString()))
            .basePath(Property.of(USER_DIR))
            .overwrite(Property.of(false))
            .build();

        assertThrows(IllegalArgumentException.class, () -> task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of())));
    }
}
