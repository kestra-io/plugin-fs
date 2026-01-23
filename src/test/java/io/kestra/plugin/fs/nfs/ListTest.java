package io.kestra.plugin.fs.nfs;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class ListTest {

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
    void list_files() throws Exception {
        Path file1 = nfsMountPoint.resolve("file1.txt");
        Path file2 = nfsMountPoint.resolve("file2.csv");
        Path subdir = nfsMountPoint.resolve("subdir");
        Path file3 = subdir.resolve("file3.txt");

        Files.createFile(file1);
        Files.createFile(file2);
        Files.createDirectory(subdir);
        Files.createFile(file3);

        io.kestra.plugin.fs.nfs.List baseTask = io.kestra.plugin.fs.nfs.List.builder()
           .id("test-list")
           .type(io.kestra.plugin.fs.nfs.List.class.getName())
           .from(Property.ofValue(nfsMountPoint.toString()))
           .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, baseTask, Map.of());

        io.kestra.plugin.fs.nfs.List task = io.kestra.plugin.fs.nfs.List.builder()
            .id("list-task")
            .type(io.kestra.plugin.fs.nfs.List.class.getName())
            .from(Property.ofValue(nfsMountPoint.toString()))
            .recursive(Property.ofValue(false))
            .build();

        io.kestra.plugin.fs.nfs.List.Output run = task.run(runContext);
        assertThat(run.getFiles(), hasSize(3));

        io.kestra.plugin.fs.nfs.List recursiveTask = io.kestra.plugin.fs.nfs.List.builder()
            .id("list-recursive")
            .type(io.kestra.plugin.fs.nfs.List.class.getName())
            .from(Property.ofValue(nfsMountPoint.toString()))
            .recursive(Property.ofValue(true))
            .build();

        List<io.kestra.plugin.fs.nfs.List.File> files;
        try (var stream = Files.walk(nfsMountPoint)) {
            files = stream
                .filter(path -> !path.equals(nfsMountPoint))
                .map(throwFunction(recursiveTask::mapToFile))
                .toList();
        }
        assertThat(files, hasSize(4));

        task = io.kestra.plugin.fs.nfs.List.builder()
            .id("list-regexp")
            .type(io.kestra.plugin.fs.nfs.List.class.getName())
            .from(Property.ofValue(nfsMountPoint.toString()))
            .recursive(Property.ofValue(true))
            .regExp(Property.ofValue(".*\\.txt$"))
            .build();

        run = task.run(runContext);
        assertThat(run.getFiles(), hasSize(2));
        List<String> foundNames = run.getFiles().stream()
            .map(io.kestra.plugin.fs.nfs.List.File::getName)
            .toList();
        assertThat(foundNames, Matchers.containsInAnyOrder("file1.txt", "file3.txt"));
    }

    @Test
    void list_files_with_max_files() throws Exception {
        Files.createFile(nfsMountPoint.resolve("file1.txt"));
        Files.createFile(nfsMountPoint.resolve("file2.txt"));

        io.kestra.plugin.fs.nfs.List task = io.kestra.plugin.fs.nfs.List.builder()
            .id("list-max-files")
            .type(io.kestra.plugin.fs.nfs.List.class.getName())
            .from(Property.ofValue(nfsMountPoint.toString()))
            .maxFiles(Property.ofValue(1))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        io.kestra.plugin.fs.nfs.List.Output run = task.run(runContext);

        assertThat(run.getFiles(), is(List.of()));
    }
}
