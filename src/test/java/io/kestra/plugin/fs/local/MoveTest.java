package io.kestra.plugin.fs.local;

import io.kestra.core.exceptions.KestraRuntimeException;
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
import java.util.Comparator;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@KestraTest
class MoveTest {
    private Path tempDir;
    private Path sourceFile;
    private Path targetFile;

    @Inject
    private RunContextFactory runContextFactory;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory(Path.of(Paths.get("/tmp").toAbsolutePath().toUri()), "kestra-test-move-");
        sourceFile = tempDir.resolve("file1.csv");
        targetFile = tempDir.resolve("file2.csv");

        Files.createFile(sourceFile);
    }

    @AfterEach
    void tearDown() throws IOException {
        try (var paths = Files.walk(tempDir)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    @Test
    void moveFile() throws Exception {
        Move task = Move.builder()
            .id(MoveTest.class.getSimpleName())
            .type(Move.class.getName())
            .from(Property.ofValue(sourceFile.toAbsolutePath().toString()))
            .to(Property.ofValue(targetFile.toAbsolutePath().toString()))
            .overwrite(Property.ofValue(true))
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
            .type(Move.class.getName())
            .from(Property.ofValue(sourceFile.toString()))
            .to(Property.ofValue(targetFile.toString()))
            .overwrite(Property.ofValue(false))
            .build();

        assertThrows(KestraRuntimeException.class, () -> task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of())));
    }

    @Test
    void moveDirectoryWhenTargetFileExistsAndOverwriteIsFalseFailsWithoutDeletingSource() throws Exception {
        Path sourceDirectory = Files.createDirectories(tempDir.resolve("source"));
        Path targetDirectory = Files.createDirectories(tempDir.resolve("target"));
        Path sourceConflict = sourceDirectory.resolve("conflict.txt");
        Path targetConflict = targetDirectory.resolve("conflict.txt");
        Path sourceOk = sourceDirectory.resolve("ok.txt");
        Path sourceNested = Files.createDirectories(sourceDirectory.resolve("sub")).resolve("a.txt");
        Files.writeString(sourceConflict, "incoming source that must survive");
        Files.writeString(targetConflict, "existing destination that must survive");
        Files.writeString(sourceOk, "non conflicting file");
        Files.writeString(sourceNested, "non conflicting nested file");

        Move task = Move.builder()
            .id(MoveTest.class.getSimpleName())
            .type(Move.class.getName())
            .from(Property.ofValue(sourceDirectory.toString()))
            .to(Property.ofValue(targetDirectory.toString()))
            .overwrite(Property.ofValue(false))
            .build();

        KestraRuntimeException exception = assertThrows(
            KestraRuntimeException.class,
            () -> task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()))
        );

        assertThat(exception.getMessage(), containsString(targetConflict.toString()));
        assertThat(Files.readString(sourceConflict), is("incoming source that must survive"));
        assertThat(Files.readString(targetConflict), is("existing destination that must survive"));
        // nothing moved: the non conflicting files stay in the source and were not copied to the target
        assertThat(Files.readString(sourceOk), is("non conflicting file"));
        assertThat(Files.readString(sourceNested), is("non conflicting nested file"));
        assertThat(Files.exists(targetDirectory.resolve("ok.txt")), is(false));
        assertThat(Files.exists(targetDirectory.resolve("sub")), is(false));
    }
}
