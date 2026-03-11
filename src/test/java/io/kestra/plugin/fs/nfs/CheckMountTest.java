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
class CheckMountTest {

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
    void checkMount_validPath() throws Exception {
        CheckMount task = CheckMount.builder()
            .id("check-mount")
            .type(CheckMount.class.getName())
            .path(Property.ofValue(nfsMountPoint.toString()))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        CheckMount.Output run = task.run(runContext);

        assertThat(run, notNullValue());
        assertThat(run.getFileStoreType(), not(containsStringIgnoringCase("nfs")));
        assertThat(run.isNfsMount(), is(false));
        assertThat(run.getPath(), is(nfsMountPoint.toString()));
    }
}
