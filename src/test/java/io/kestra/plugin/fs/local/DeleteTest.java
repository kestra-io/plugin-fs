package io.kestra.plugin.fs.local;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.*;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

@KestraTest
class DeleteTest {
    private Path testFile;
    private Path testDir;
    private Path tempDir;

    @Inject
    private RunContextFactory runContextFactory;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory(Path.of(Paths.get("/tmp").toAbsolutePath().toUri()), "kestra-test-delete-");
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
            .from(Property.ofValue(testFile.toString()))
            .build();

        Delete.Output output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.isDeleted(), is(true));
        assertThat(Files.exists(testFile), is(false));
    }

    @Test
    void deleteNonExistentFileWithoutError() throws Exception {
        Path nonExistentFile = testFile.getParent().resolve("non-existent.txt");

        Delete task = Delete.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(Delete.class.getName())
            .from(Property.ofValue(nonExistentFile.toAbsolutePath().toString()))
            .errorOnMissing(Property.ofValue(false))
            .build();

        Delete.Output output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.isDeleted(), is(false));
    }

    @Test
    void deleteNonExistentFileWithError() {
        Path nonExistentFile = testFile.getParent().resolve("non-existent.txt");

        Delete task = Delete.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(Delete.class.getName())
            .from(Property.ofValue(nonExistentFile.toAbsolutePath().toString()))
            .errorOnMissing(Property.ofValue(true))
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
            .from(Property.ofValue(testDir.toString()))
            .recursive(Property.ofValue(true))
            .build();

        Delete.Output output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.isDeleted(), is(true));
        assertThat(Files.exists(testDir), is(false));
    }
}