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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
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
        tempDir = Files.createTempDirectory(Path.of(Paths.get("/tmp").toAbsolutePath().toUri()), "kestra-test-copy-");
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
            .from(Property.ofValue(sourceFile.toString()))
            .to(Property.ofValue(targetFile.toString()))
            .overwrite(Property.ofValue(true))
            .build();

        VoidOutput output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(Files.exists(targetFile), is(true));
        assertThat(Files.exists(sourceFile), is(true));
    }

    @Test
    void copyFileWhenTargetExistsAndOverwriteIsFalse() throws Exception {
        Files.createFile(targetFile);

        Copy task = Copy.builder()
            .id(CopyTest.class.getSimpleName())
            .type(Copy.class.getName())
            .from(Property.ofValue(sourceFile.toString()))
            .to(Property.ofValue(targetFile.toString()))
            .overwrite(Property.ofValue(false))
            .build();

        assertThrows(IllegalArgumentException.class, () -> task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of())));
    }
}
