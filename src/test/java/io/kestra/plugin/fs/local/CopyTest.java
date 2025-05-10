package io.kestra.plugin.fs.local;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@KestraTest
class CopyTest {
    private Path sourceFile;
    private Path targetFile;
    private Path tempDir;

    @Inject
    private RunContextFactory runContextFactory;

    @BeforeEach
    void setUp() throws IOException {
        RunContext runContext = runContextFactory.of();
        tempDir = Files.createTempDirectory(Path.of(runContext.workingDir().path().toUri()), "kestra-test-copy-");
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
            .allowedPaths(Property.of(List.of(tempDir.toRealPath().toString())))
            .overwrite(Property.of(true))
            .build();

        VoidOutput output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

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
            .allowedPaths(Property.of(List.of(tempDir.toRealPath().toString())))
            .overwrite(Property.of(false))
            .build();

        assertThrows(IllegalArgumentException.class, () -> task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of())));
    }
}
