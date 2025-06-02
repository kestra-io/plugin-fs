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
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.*;

@KestraTest
class DownloadTest {

    private Path sourceFile;
    private final String fileContent = "test content";

    @Inject
    private RunContextFactory runContextFactory;

    @BeforeEach
    void setUp() throws IOException {
        Path tempDir = Files.createTempDirectory(Path.of(Paths.get("/tmp").toAbsolutePath().toUri()), "kestra-test-download-");
        sourceFile = tempDir.resolve("source-file.txt");

        Files.writeString(sourceFile, fileContent);
    }

    @Test
    void download() throws Exception {
        Download task = Download.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Download.class.getName())
            .from(Property.ofValue(sourceFile.toString()))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        Download.Output output = task.run(runContext);

        assertThat(output.getUri(), notNullValue());
        assertThat((long) fileContent.length(), is(output.getSize()));

        String storedContent = new String(
            runContext.storage().getFile(output.getUri()).readAllBytes(),
            StandardCharsets.UTF_8
        );
        assertThat(fileContent, is(storedContent));
    }

    @Test
    void downloadNonExistentFile() throws IOException {
        Path nonExistentFile = sourceFile.getParent().resolve("non-existent.txt");
        Download task = Download.builder()
            .id(DownloadTest.class.getSimpleName())
            .type(Download.class.getName())
            .from(Property.ofValue(nonExistentFile.toString()))
            .build();

        assertThrows(
            IllegalArgumentException.class,
            () -> task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()))
        );
    }
}