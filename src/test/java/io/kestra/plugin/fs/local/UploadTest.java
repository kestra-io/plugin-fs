package io.kestra.plugin.fs.local;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class UploadTest {
    private Path tempDir;
    private Path destinationFile;
    private URI sourceUri;
    private final String fileContent = "test content for upload";

    @Inject
    private RunContextFactory runContextFactory;

    @BeforeEach
    void setUp() throws IOException {

        RunContext runContext = runContextFactory.of();

        tempDir = Files.createTempDirectory(Path.of(Paths.get("/tmp").toAbsolutePath().toUri()), "kestra-test-upload-");
        destinationFile = tempDir.resolve("destination-file.txt");

        File tempFile = Files.createTempFile(tempDir, "kestra-test-upload-", ".txt").toFile();
        Files.write(tempFile.toPath(), fileContent.getBytes(StandardCharsets.UTF_8));

        sourceUri = runContext.storage().putFile(tempFile);
    }

    @AfterEach
    void tearDown() throws IOException {
        Files.deleteIfExists(destinationFile);
        Files.deleteIfExists(tempDir);
    }

    @Test
    void upload() throws Exception {
        Upload task = Upload.builder()
            .id(UploadTest.class.getSimpleName())
            .type(Upload.class.getName())
            .from(Property.of(sourceUri.toString()))
            .to(Property.of(destinationFile.toString()))
            .overwrite(Property.of(true))
            .build();

        Upload.Output output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(Files.exists(destinationFile), is(true));
        assertThat((long) fileContent.length(), is(output.getSize()));
        String uploadedContent = Files.readString(destinationFile);
        assertThat(fileContent, is(uploadedContent));
    }

    @Test
    void uploadWithoutOverwrite() throws Exception {
        Files.writeString(destinationFile, "existing content");

        Upload task = Upload.builder()
            .id(UploadTest.class.getSimpleName())
            .type(Upload.class.getName())
            .from(Property.of(sourceUri.toString()))
            .to(Property.of(destinationFile.toString()))
            .overwrite(Property.of(false))
            .build();

        assertThrows(
            io.kestra.core.exceptions.KestraRuntimeException.class,
            () -> task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()))
        );
    }

    @Test
    void uploadWithDefaultName() throws Exception {
        Upload task = Upload.builder()
            .id(UploadTest.class.getSimpleName())
            .type(Upload.class.getName())
            .from(Property.of(sourceUri.toString()))
            .build();

        Upload.Output output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        Path expectedPath = runContextFactory.of().workingDir().path();
        assertThat(output.getSize(), greaterThan(0L));
        assertThat(Files.exists(expectedPath), is(true));
    }
}