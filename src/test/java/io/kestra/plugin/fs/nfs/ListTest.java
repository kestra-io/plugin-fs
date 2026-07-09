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
import java.nio.file.attribute.FileTime;
import java.time.Instant;
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

        assertThat(run.getFiles(), hasSize(1));
    }

    @Test
    void sortByNameAscAndDesc() throws Exception {
        Files.createFile(nfsMountPoint.resolve("b.txt"));
        Files.createFile(nfsMountPoint.resolve("a.txt"));
        Files.createFile(nfsMountPoint.resolve("c.txt"));

        io.kestra.plugin.fs.nfs.List task = io.kestra.plugin.fs.nfs.List.builder()
            .id("sort-name-asc")
            .type(io.kestra.plugin.fs.nfs.List.class.getName())
            .from(Property.ofValue(nfsMountPoint.toString()))
            .sort(Property.ofValue(io.kestra.plugin.fs.nfs.List.Sort.NAME_ASC))
            .build();

        io.kestra.plugin.fs.nfs.List.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
        assertThat(run.getFiles().stream().map(io.kestra.plugin.fs.nfs.List.File::getName).toList(), contains("a.txt", "b.txt", "c.txt"));

        task = io.kestra.plugin.fs.nfs.List.builder()
            .id("sort-name-desc")
            .type(io.kestra.plugin.fs.nfs.List.class.getName())
            .from(Property.ofValue(nfsMountPoint.toString()))
            .sort(Property.ofValue(io.kestra.plugin.fs.nfs.List.Sort.NAME_DESC))
            .build();

        run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
        assertThat(run.getFiles().stream().map(io.kestra.plugin.fs.nfs.List.File::getName).toList(), contains("c.txt", "b.txt", "a.txt"));
    }

    @Test
    void sortByLastModifiedAppliesBeforeMaxFilesTruncation() throws Exception {
        Path older = nfsMountPoint.resolve("older.txt");
        Path middle = nfsMountPoint.resolve("middle.txt");
        Path newer = nfsMountPoint.resolve("newer.txt");
        Files.createFile(older);
        Files.createFile(middle);
        Files.createFile(newer);

        Files.setLastModifiedTime(older, FileTime.from(Instant.now().minusSeconds(60)));
        Files.setLastModifiedTime(middle, FileTime.from(Instant.now().minusSeconds(30)));
        Files.setLastModifiedTime(newer, FileTime.from(Instant.now()));

        io.kestra.plugin.fs.nfs.List task = io.kestra.plugin.fs.nfs.List.builder()
            .id("sort-last-modified")
            .type(io.kestra.plugin.fs.nfs.List.class.getName())
            .from(Property.ofValue(nfsMountPoint.toString()))
            .sort(Property.ofValue(io.kestra.plugin.fs.nfs.List.Sort.LAST_MODIFIED_DESC))
            .maxFiles(Property.ofValue(2))
            .build();

        io.kestra.plugin.fs.nfs.List.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles(), hasSize(2));
        assertThat(run.getFiles().stream().map(io.kestra.plugin.fs.nfs.List.File::getName).toList(), contains("newer.txt", "middle.txt"));
    }

    @Test
    void sortShouldNotThrowOnEmptyList() throws Exception {
        io.kestra.plugin.fs.nfs.List task = io.kestra.plugin.fs.nfs.List.builder()
            .id("sort-empty")
            .type(io.kestra.plugin.fs.nfs.List.class.getName())
            .from(Property.ofValue(nfsMountPoint.toString()))
            .regExp(Property.ofValue(".*\\.doesnotexist"))
            .sort(Property.ofValue(io.kestra.plugin.fs.nfs.List.Sort.NAME_ASC))
            .build();

        io.kestra.plugin.fs.nfs.List.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles(), hasSize(0));
    }
}
