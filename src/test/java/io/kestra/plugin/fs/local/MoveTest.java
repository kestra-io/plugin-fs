package io.kestra.plugin.fs.local;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.*;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@KestraTest
class MoveTest {
    private Path sourceFile;
    private Path targetFile;

    @Inject
    private RunContextFactory runContextFactory;

    @BeforeEach
    void setUp() throws IOException {
        Path tempDir = Files.createTempDirectory(Path.of(Paths.get("/tmp").toAbsolutePath().toUri()), "kestra-test-move-");
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
            .from(Property.of(sourceFile.toAbsolutePath().toString()))
            .to(Property.of(targetFile.toAbsolutePath().toString()))
            .overwrite(Property.of(true))
            .build();

        VoidOutput output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(Files.exists(targetFile), is(true));
        assertThat(Files.exists(sourceFile), is(false));
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

        assertThrows(io.kestra.core.exceptions.KestraRuntimeException.class, () -> task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of())));
    }
}
