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
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@KestraTest
class MoveTest {
    private Path sourceFile;
    private Path targetFile;

    @Inject
    private RunContextFactory runContextFactory;

    @BeforeEach
    void setUp() throws IOException {
        RunContext runContext = runContextFactory.of();
        Path tempDir = Files.createTempDirectory(Path.of(runContext.workingDir().path().toUri()), "kestra-test-move-");
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
    void moveFile() throws Exception {
        Move task = Move.builder()
            .id(MoveTest.class.getSimpleName())
            .type(MoveTest.class.getName())
            .from(Property.of(sourceFile.toString()))
            .to(Property.of(targetFile.toString()))

            .overwrite(Property.of(true))
            .build();

        VoidOutput output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertTrue(Files.exists(targetFile));
        assertFalse(Files.exists(sourceFile));
    }

    @Test
    void moveFileWhenTargetExistsAndOverwriteIsFalse() throws Exception {
        Files.createFile(targetFile);

        Move task = Move.builder()
            .id(MoveTest.class.getSimpleName())
            .type(MoveTest.class.getName())
            .from(Property.of(sourceFile.toString()))
            .to(Property.of(targetFile.toString()))

            .overwrite(Property.of(false))
            .build();

        assertThrows(IllegalArgumentException.class, () -> task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of())));
    }
}
