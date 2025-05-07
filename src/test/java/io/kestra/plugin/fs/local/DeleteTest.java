package io.kestra.plugin.fs.local;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.*;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@KestraTest
class DeleteTest {
    private Path testFile;
    private Path testDir;

    @Inject
    private RunContextFactory runContextFactory;

    @BeforeEach
    void setUp() throws IOException {
        RunContext runContext = runContextFactory.of();
        Path tempDir = Files.createTempDirectory(Path.of(runContext.workingDir().path().toUri()), "kestra-test-delete-");
        testFile = tempDir.resolve("test-file.txt");
        testDir = tempDir.resolve("test-dir");

        Files.createFile(testFile);
        Files.createDirectory(testDir);
        Files.createFile(testDir.resolve("file-in-dir.txt"));
    }

    @Test
    void deleteFile() throws Exception {
        Delete task = Delete.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(Delete.class.getName())
            .path(Property.of(testFile.toString()))
            .build();

        Delete.Output output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertTrue(output.isDeleted());
        assertFalse(Files.exists(testFile));
    }

    @Test
    void deleteNonExistentFileWithoutError() throws Exception {
        Path nonExistentFile = testFile.getParent().resolve("non-existent.txt");
        Delete task = Delete.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(Delete.class.getName())
            .path(Property.of(nonExistentFile.toUri().toString()))
            .errorOnMissing(Property.of(false))
            .build();

        Delete.Output output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertFalse(output.isDeleted());
    }

    @Test
    void deleteNonExistentFileWithError() {
        Path nonExistentFile = testFile.getParent().resolve("non-existent.txt");
        Delete task = Delete.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(Delete.class.getName())
            .path(Property.of(nonExistentFile.toUri().toString()))
            .errorOnMissing(Property.of(true))
            .build();

        assertThrows(
            java.util.NoSuchElementException.class,
            () -> task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()))
        );
    }

    @Test
    void deleteDirectoryRecursively() throws Exception {
        Delete task = Delete.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(Delete.class.getName())
            .path(Property.of(testDir.toString()))
            .recursive(Property.of(true))
            .build();

        Delete.Output output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertTrue(output.isDeleted());
        assertFalse(Files.exists(testDir));
    }
}