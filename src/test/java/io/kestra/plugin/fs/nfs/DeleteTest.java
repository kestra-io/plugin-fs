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
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
        var fileToDelete = nfsMountPoint.resolve("delete.txt");
        Files.writeString(fileToDelete, "delete me");

        var task = Delete.builder()
            .id("delete-task")
            .type(Delete.class.getName())
            .uri(Property.ofValue(fileToDelete.toString()))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        var output = task.run(runContext);

        assertThat(Files.exists(fileToDelete), is(false));
        assertThat(output.isDeleted(), is(true));
        assertThat(output.getUri(), is(fileToDelete.toUri()));
        assertThat(output.getUris(), is(empty()));
    }

    @Test
    void delete_file_error_on_missing() {
        var missing = nfsMountPoint.resolve("ghost.txt");

        var task = Delete.builder()
            .id("delete-missing-error")
            .type(Delete.class.getName())
            .uri(Property.ofValue(missing.toString()))
            .errorOnMissing(Property.ofValue(true))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        assertThrows(NoSuchFileException.class, () -> task.run(runContext));
    }

    @Test
    void delete_file_no_error_on_missing() throws Exception {
        var missing = nfsMountPoint.resolve("ghost.txt");

        var task = Delete.builder()
            .id("delete-missing-no-error")
            .type(Delete.class.getName())
            .uri(Property.ofValue(missing.toString()))
            .errorOnMissing(Property.ofValue(false))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        var output = task.run(runContext);

        assertThat(output.isDeleted(), is(false));
        assertThat(output.getUris(), is(empty()));
    }

    @Test
    void delete_regexp_flat() throws Exception {
        // Create files: two .log and one .txt - only .log files should be deleted
        Files.writeString(nfsMountPoint.resolve("a.log"), "log a");
        Files.writeString(nfsMountPoint.resolve("b.log"), "log b");
        Files.writeString(nfsMountPoint.resolve("c.txt"), "keep me");

        var task = Delete.builder()
            .id("delete-regexp-flat")
            .type(Delete.class.getName())
            .uri(Property.ofValue(nfsMountPoint.toString()))
            .regExp(Property.ofValue(".*\\.log"))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        var output = task.run(runContext);

        assertThat(output.isDeleted(), is(true));
        assertThat(output.getUris(), hasSize(2));
        assertThat(Files.exists(nfsMountPoint.resolve("a.log")), is(false));
        assertThat(Files.exists(nfsMountPoint.resolve("b.log")), is(false));
        assertThat(Files.exists(nfsMountPoint.resolve("c.txt")), is(true));
    }

    @Test
    void delete_regexp_no_match_returns_not_deleted() throws Exception {
        Files.writeString(nfsMountPoint.resolve("keep.txt"), "keep me");

        var task = Delete.builder()
            .id("delete-regexp-no-match")
            .type(Delete.class.getName())
            .uri(Property.ofValue(nfsMountPoint.toString()))
            .regExp(Property.ofValue(".*\\.log"))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        var output = task.run(runContext);

        assertThat(output.isDeleted(), is(false));
        assertThat(output.getUris(), is(empty()));
        assertThat(Files.exists(nfsMountPoint.resolve("keep.txt")), is(true));
    }

    @Test
    void delete_regexp_non_recursive_skips_subdirs() throws Exception {
        var subDir = nfsMountPoint.resolve("sub");
        Files.createDirectories(subDir);
        Files.writeString(nfsMountPoint.resolve("top.log"), "top");
        Files.writeString(subDir.resolve("nested.log"), "nested");

        var task = Delete.builder()
            .id("delete-regexp-non-recursive")
            .type(Delete.class.getName())
            .uri(Property.ofValue(nfsMountPoint.toString()))
            .regExp(Property.ofValue(".*\\.log"))
            // recursive defaults to false
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        var output = task.run(runContext);

        assertThat(output.isDeleted(), is(true));
        assertThat(output.getUris(), hasSize(1));
        assertThat(Files.exists(nfsMountPoint.resolve("top.log")), is(false));
        // nested file must be untouched
        assertThat(Files.exists(subDir.resolve("nested.log")), is(true));
    }

    @Test
    void delete_regexp_recursive() throws Exception {
        var subDir = nfsMountPoint.resolve("sub");
        Files.createDirectories(subDir);
        Files.writeString(nfsMountPoint.resolve("top.log"), "top");
        Files.writeString(subDir.resolve("nested.log"), "nested");
        Files.writeString(nfsMountPoint.resolve("keep.txt"), "keep");

        var task = Delete.builder()
            .id("delete-regexp-recursive")
            .type(Delete.class.getName())
            .uri(Property.ofValue(nfsMountPoint.toString()))
            .regExp(Property.ofValue(".*\\.log"))
            .recursive(Property.ofValue(true))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        var output = task.run(runContext);

        assertThat(output.isDeleted(), is(true));
        assertThat(output.getUris(), hasSize(2));
        assertThat(Files.exists(nfsMountPoint.resolve("top.log")), is(false));
        assertThat(Files.exists(subDir.resolve("nested.log")), is(false));
        assertThat(Files.exists(nfsMountPoint.resolve("keep.txt")), is(true));
    }

    @Test
    void delete_regexp_missing_dir_error_on_missing() {
        var missing = nfsMountPoint.resolve("no_such_dir");

        var task = Delete.builder()
            .id("delete-regexp-missing-dir-error")
            .type(Delete.class.getName())
            .uri(Property.ofValue(missing.toString()))
            .regExp(Property.ofValue(".*\\.log"))
            .errorOnMissing(Property.ofValue(true))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        assertThrows(NoSuchFileException.class, () -> task.run(runContext));
    }

    @Test
    void delete_regexp_missing_dir_no_error_on_missing() throws Exception {
        var missing = nfsMountPoint.resolve("no_such_dir");

        var task = Delete.builder()
            .id("delete-regexp-missing-dir-no-error")
            .type(Delete.class.getName())
            .uri(Property.ofValue(missing.toString()))
            .regExp(Property.ofValue(".*\\.log"))
            .errorOnMissing(Property.ofValue(false))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        var output = task.run(runContext);

        assertThat(output.isDeleted(), is(false));
        assertThat(output.getUris(), is(empty()));
    }
}
