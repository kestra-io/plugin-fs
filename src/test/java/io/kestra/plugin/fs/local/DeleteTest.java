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
import static org.hamcrest.Matchers.*;
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

    @Test
    void deleteWithRegExpMatchesFiles() throws Exception {
        // use a dedicated directory so no pre-existing files interfere
        Path regexpDir = tempDir.resolve("regexp-dir");
        Files.createDirectory(regexpDir);

        Path txtFile1 = regexpDir.resolve("report-2024.txt");
        Path txtFile2 = regexpDir.resolve("report-2025.txt");
        Path logFile  = regexpDir.resolve("app.log");
        Files.createFile(txtFile1);
        Files.createFile(txtFile2);
        Files.createFile(logFile);

        Delete task = Delete.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(Delete.class.getName())
            .from(Property.ofValue(regexpDir.toString()))
            .regExp(Property.ofValue(".*\\.txt"))
            .recursive(Property.ofValue(true))
            .build();

        Delete.Output output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.isDeleted(), is(true));
        // only the two .txt files are deleted; the .log file survives
        assertThat(output.getUris(), hasSize(2));
        assertThat(Files.exists(txtFile1), is(false));
        assertThat(Files.exists(txtFile2), is(false));
        assertThat(Files.exists(logFile), is(true));
    }

    @Test
    void deleteWithRegExpNoMatchReturnsNotDeleted() throws Exception {
        // no file in testDir matches *.csv
        Delete task = Delete.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(Delete.class.getName())
            .from(Property.ofValue(testDir.toString()))
            .regExp(Property.ofValue(".*\\.csv"))
            .recursive(Property.ofValue(true))
            .build();

        Delete.Output output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.isDeleted(), is(false));
        assertThat(output.getUris(), is(empty()));
    }

    @Test
    void deleteWithRegExpNonRecursiveIgnoresSubdirectoryFiles() throws Exception {
        // use a dedicated directory so no pre-existing files interfere
        Path regexpDir = tempDir.resolve("regexp-nonrecursive-dir");
        Files.createDirectory(regexpDir);

        // file directly in regexpDir (depth 1, within walk limit)
        Path rootFile = regexpDir.resolve("root.txt");
        // file in a nested subdirectory (depth 2, excluded when recursive=false)
        Path subDir  = regexpDir.resolve("sub");
        Path subFile = subDir.resolve("nested.txt");
        Files.createFile(rootFile);
        Files.createDirectories(subDir);
        Files.createFile(subFile);

        Delete task = Delete.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(Delete.class.getName())
            .from(Property.ofValue(regexpDir.toString()))
            .regExp(Property.ofValue(".*\\.txt"))
            .recursive(Property.ofValue(false))
            .build();

        Delete.Output output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        // only the root-level file is deleted; nested file survives
        assertThat(output.isDeleted(), is(true));
        assertThat(output.getUris(), hasSize(1));
        assertThat(Files.exists(rootFile), is(false));
        assertThat(Files.exists(subFile), is(true));
    }

    @Test
    void deleteWithRegExpMissingDirectoryWithoutError() throws Exception {
        Path missing = tempDir.resolve("no-such-dir");

        Delete task = Delete.builder()
            .id(DeleteTest.class.getSimpleName())
            .type(Delete.class.getName())
            .from(Property.ofValue(missing.toString()))
            .regExp(Property.ofValue(".*\\.txt"))
            .errorOnMissing(Property.ofValue(false))
            .build();

        Delete.Output output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.isDeleted(), is(false));
        assertThat(output.getUris(), is(empty()));
    }

}