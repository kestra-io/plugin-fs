package io.kestra.plugin.fs.local;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.*;
import java.util.Comparator;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class ListTest {
    private Path tempDir;

    @Inject
    private RunContextFactory runContextFactory;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory(Path.of(Paths.get("/tmp").toAbsolutePath().toUri()), "kestra-test-list-");
        Files.createFile(tempDir.resolve("file1.csv"));
        Files.createFile(tempDir.resolve("file2.csv"));
        Files.createDirectory(tempDir.resolve("nested"));
        Files.createFile(tempDir.resolve("nested").resolve("nested_file.csv"));
    }

    @AfterEach
    void tearDown() throws IOException {
        Files.walk(tempDir)
            .sorted(Comparator.reverseOrder())
            .forEach(ListTest::sneakyDelete);
    }

    @SneakyThrows
    private static void sneakyDelete(Path path) {
        Files.deleteIfExists(path);
    }

    @Test
    void listFiles() throws Exception {
        List task = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(List.class.getName())
            .from(Property.ofValue(tempDir.toString()))
            .regExp(Property.ofValue(".*\\.csv"))
            .recursive(Property.ofValue(true))
            .build();

        List.Output output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.getCount(), is(3));
    }

    @Test
    void listFilesWithMaxFiles() throws Exception {
        List task = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(List.class.getName())
            .from(Property.ofValue(tempDir.toString()))
            .regExp(Property.ofValue(".*\\.csv"))
            .recursive(Property.ofValue(true))
            .maxFiles(Property.ofValue(2))
            .build();

        List.Output output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.getFiles(), is(empty()));
        assertThat(output.getCount(), is(0));
    }
}
