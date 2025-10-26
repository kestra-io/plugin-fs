package io.kestra.plugin.fs.nfs;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


class NfsTasksTest extends AbstractNfsTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @TempDir
    private Path tempDir;

    private Path testDir;
    private Path testFile;

    @BeforeEach
    void setUp() throws IOException {
        
        testDir = tempDir.resolve("nfs_tasks_test");
        Files.createDirectories(testDir);
        testFile = testDir.resolve("test_file.txt");
        Files.writeString(testFile, "Hello Kestra NFS");
    }

    private RunContext runContext() {
        return runContextFactory.of();
    }

    @Test
    void checkMount() throws Exception {
        RunContext runContext = runContext();
        CheckMount task = CheckMount.builder()
            .path(testDir.toString())
            .build();

        CheckMount.Output output = task.run(runContext);

        assertThat(output.getFileStoreType(), not(nullValue()));
        
    }

    @Test
    void list() throws Exception {
        RunContext runContext = runContext();
        List task = List.builder()
            .from(testDir.toString())
            .regExp(".*\\.txt$")
            .build();

        List.Output output = task.run(runContext);

        assertThat(output.getFiles(), hasSize(1));
        assertThat(output.getFiles().get(0).getName(), is("test_file.txt"));
    }

    @Test
    void copy() throws Exception {
        RunContext runContext = runContext();
        String destPath = testDir.resolve("copy_dest.txt").toString();

        Copy task = Copy.builder()
            .from(testFile.toString())
            .to(destPath)
            .build();

        Copy.Output output = task.run(runContext);

        assertThat(output.getTo(), is(Path.of(destPath).toUri()));
        assertTrue(Files.exists(Path.of(destPath)));
    }

    @Test
    void move() throws Exception {
        RunContext runContext = runContext();
        String destPath = testDir.resolve("move_dest.txt").toString();

        Move task = Move.builder()
            .from(testFile.toString())
            .to(destPath)
            .build();

        Move.Output output = task.run(runContext);

        assertThat(output.getTo(), is(Path.of(destPath).toUri()));
        assertTrue(Files.exists(Path.of(destPath)));
        assertTrue(Files.notExists(testFile)); // Original file should be gone
    }

    @Test
    void delete() throws Exception {
        RunContext runContext = runContext();
        Delete task = Delete.builder()
            .uri(testFile.toString())
            .build();

        Delete.Output output = task.run(runContext);

        assertThat(output.isDeleted(), is(true));
        assertTrue(Files.notExists(testFile));
    }

    @Test
    void deleteErrorOnMissing() {
        RunContext runContext = runContext();
        Delete task = Delete.builder()
            .uri(testDir.resolve("non_existent_file.txt").toString())
            .errorOnMissing(true)
            .build();

        
        assertThrows(IOException.class, () -> {
            task.run(runContext);
        });
    }
}

