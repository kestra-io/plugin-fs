package io.kestra.plugin.fs.local;

import com.devskiller.friendly_id.FriendlyId;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class UploadsTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    private final String random = IdUtils.create();
    private Path destDir;

    @BeforeEach
    void setUp() throws IOException {
        destDir = Files.createTempDirectory(Paths.get("/tmp"), "kestra-test-uploads-" + random + "-");
    }

    @AfterEach
    void tearDown() throws IOException {
        if (destDir != null && Files.exists(destDir)) {
            try (var paths = Files.walk(destDir)) {
                paths.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException ignored) {
                    }
                });
            }
        }
    }

    private URI uploadWithSuffix(String suffix) throws Exception {
        return storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + FriendlyId.createFriendlyId() + suffix),
            new ByteArrayInputStream("test".getBytes(StandardCharsets.UTF_8))
        );
    }

    @Test
    void shouldUploadListOfFiles() throws Exception {
        URI uri1 = uploadWithSuffix(".txt");
        URI uri2 = uploadWithSuffix(".txt");

        Uploads task = Uploads.builder()
            .id(UploadsTest.class.getSimpleName())
            .type(Uploads.class.getName())
            .from(List.of(uri1.toString(), uri2.toString()))
            .to(Property.ofValue(destDir.toString()))
            .build();

        Uploads.Output output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.getFiles().size(), is(2));
        try (var paths = Files.list(destDir)) {
            assertThat(paths.count(), is(2L));
        }
    }

    @Test
    void shouldUploadMapWithCustomNames() throws Exception {
        URI uri1 = uploadWithSuffix(".bin");
        URI uri2 = uploadWithSuffix(".bin");

        Uploads task = Uploads.builder()
            .id(UploadsTest.class.getSimpleName())
            .type(Uploads.class.getName())
            .from(Map.of("report.csv", uri1.toString(), "data.json", uri2.toString()))
            .to(Property.ofValue(destDir.toString()))
            .build();

        Uploads.Output output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.getFiles().size(), is(2));
        assertThat(Files.exists(destDir.resolve("report.csv")), is(true));
        assertThat(Files.exists(destDir.resolve("data.json")), is(true));
    }

    @Test
    void shouldFilterFilesByRegExp() throws Exception {
        URI sql = uploadWithSuffix(".sql");
        URI yaml = uploadWithSuffix(".yaml");
        URI txt = uploadWithSuffix(".txt");

        Uploads task = Uploads.builder()
            .id(UploadsTest.class.getSimpleName())
            .type(Uploads.class.getName())
            .from(List.of(sql.toString(), yaml.toString(), txt.toString()))
            .to(Property.ofValue(destDir.toString()))
            .regExp(Property.ofValue(".*\\.sql$"))
            .build();

        Uploads.Output output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.getFiles().size(), is(1));
        assertThat(output.getFiles().getFirst().getPath(), Matchers.endsWith(".sql"));
    }

    @Test
    void shouldRespectMaxFiles() throws Exception {
        URI uri1 = uploadWithSuffix(".txt");
        URI uri2 = uploadWithSuffix(".txt");

        Uploads task = Uploads.builder()
            .id(UploadsTest.class.getSimpleName())
            .type(Uploads.class.getName())
            .from(List.of(uri1.toString(), uri2.toString()))
            .to(Property.ofValue(destDir.toString()))
            .maxFiles(Property.ofValue(1))
            .build();

        Uploads.Output output = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.getFiles().size(), is(1));
    }

    @Test
    void shouldFailWhenOverwriteFalseAndFileExists() throws Exception {
        URI uri = uploadWithSuffix(".txt");
        String fileName = uri.toString().substring(uri.toString().lastIndexOf('/') + 1);
        Files.writeString(destDir.resolve(fileName), "existing");

        Uploads task = Uploads.builder()
            .id(UploadsTest.class.getSimpleName())
            .type(Uploads.class.getName())
            .from(List.of(uri.toString()))
            .to(Property.ofValue(destDir.toString()))
            .overwrite(Property.ofValue(false))
            .build();

        assertThrows(
            io.kestra.core.exceptions.KestraRuntimeException.class,
            () -> task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()))
        );
    }

    @Test
    void shouldDenyDestinationOutsideAllowedPaths() throws Exception {
        URI uri = uploadWithSuffix(".txt");

        Uploads task = Uploads.builder()
            .id(UploadsTest.class.getSimpleName())
            .type(Uploads.class.getName())
            .from(List.of(uri.toString()))
            .to(Property.ofValue("/etc/forbidden-target-" + random))
            .build();

        assertThrows(
            SecurityException.class,
            () -> task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()))
        );
    }
}
