package io.kestra.plugin.fs.nfs;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class DeleteTest {

    @Inject
    private RunContextFactory runContextFactory;

    @TempDir
    private Path tempDirectory;
    private Path nfsMountPoint;

    @BeforeEach
    void setup() throws IOException {
        nfsMountPoint = tempDirectory.resolve("nfs_share");
        Files.createDirectories(nfsMountPoint);
    }

    @Test
    void delete_file() throws Exception {
        Path fileToDelete = nfsMountPoint.resolve("delete.txt");
        Files.writeString(fileToDelete, "delete me");

        Delete task = Delete.builder()
            .id("delete-task")
            .type(Delete.class.getName())
            .uri(Property.ofValue(fileToDelete.toString()))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        Delete.Output run = task.run(runContext);

        assertThat(Files.exists(fileToDelete), is(false));
        assertThat(run.isDeleted(), is(true));
        assertThat(run.getUri(), is(fileToDelete.toUri()));
    }
}
