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
class MoveTest {

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
    void move_file() throws Exception {
        Path sourceFile = nfsMountPoint.resolve("source.txt");
        Files.writeString(sourceFile, "move me");
        Path destFile = nfsMountPoint.resolve("moved.txt");

        Move task = Move.builder()
            .id("move-task")
            .type(Move.class.getName())
            .from(Property.ofValue(sourceFile.toString()))
            .to(Property.ofValue(destFile.toString()))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        Move.Output run = task.run(runContext);

        assertThat(Files.exists(sourceFile), is(false));
        assertThat(Files.exists(destFile), is(true));
        assertThat(Files.readString(destFile), is("move me"));
        assertThat(run.getTo(), is(destFile.toUri()));
    }
}