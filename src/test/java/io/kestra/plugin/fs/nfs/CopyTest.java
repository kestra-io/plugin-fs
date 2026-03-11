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
class CopyTest {

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
    void copy_file() throws Exception {
        Path sourceFile = nfsMountPoint.resolve("source.txt");
        Files.writeString(sourceFile, "copy me");
        Path copyDest = nfsMountPoint.resolve("copied.txt");

        Copy task = Copy.builder()
            .id("copy-task")
            .type(Copy.class.getName())
            .from(Property.ofValue(sourceFile.toString()))
            .to(Property.ofValue(copyDest.toString()))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        Copy.Output run = task.run(runContext);

        assertThat(Files.exists(copyDest), is(true));
        assertThat(Files.readString(copyDest), is("copy me"));
        assertThat(run.getTo(), is(copyDest.toUri()));
    }
}
