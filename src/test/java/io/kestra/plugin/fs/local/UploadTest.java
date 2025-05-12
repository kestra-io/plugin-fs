package io.kestra.plugin.fs.local;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@KestraTest
@Disabled("Cannot work on CI")
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

        tempDir = Files.createTempDirectory(Path.of(runContext.workingDir().path().toUri()), "kestra-test-upload-");
        destinationFile = tempDir.resolve("destination-file.txt");

        File tempFile = runContext.workingDir().createTempFile().toFile();
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

        assertTrue(Files.exists(destinationFile));
        assertEquals(fileContent.length(), output.getSize());
        String uploadedContent = new String(Files.readAllBytes(destinationFile), StandardCharsets.UTF_8);
        assertEquals(fileContent, uploadedContent);
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

        String fileName = sourceUri.toString().substring(sourceUri.toString().lastIndexOf('/') + 1);
        Path expectedPath = Paths.get(fileName);

        assertTrue(Files.exists(expectedPath));
    }
}