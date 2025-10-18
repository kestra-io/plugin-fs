package io.kestra.plugin.fs.local;

import com.devskiller.friendly_id.FriendlyId;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.vfs.models.File;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TriggerTest extends AbstractTriggerTest {
    @Inject
    private LocalUtils localUtils;

    @Override
    protected String triggeringFlowId() {
        return "local-listen";
    }

    @Override
    protected LocalUtils utils() {
        return localUtils;
    }

    @Test
    void move() throws Exception {
        Files.createDirectories(Paths.get("/tmp/trigger"));
        Files.createDirectories(Paths.get("/tmp/trigger-move"));

        io.kestra.plugin.fs.local.Trigger trigger = io.kestra.plugin.fs.local.Trigger.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(io.kestra.plugin.fs.local.Trigger.class.getName())
            .from(Property.ofValue("/tmp/trigger/"))
            .action(Property.ofValue(Downloads.Action.MOVE))
            .moveDirectory(Property.ofValue("/tmp/trigger-move/"))
            .recursive(Property.ofValue(true))
            .build();

        String out = FriendlyId.createFriendlyId();
        Upload.Output upload = utils().upload("/tmp/trigger/" + out + ".yml");

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        java.util.List<File> urls = (java.util.List<File>) execution.get().getTrigger().getVariables().get("files");
        assertThat(urls.size(), greaterThanOrEqualTo(1));

        assertThrows(java.lang.IllegalArgumentException.class, () -> {
            io.kestra.plugin.fs.local.Download task = io.kestra.plugin.fs.local.Download.builder()
                .id(TriggerTest.class.getSimpleName())
                .type(io.kestra.plugin.fs.local.Download.class.getName())
                .from(Property.ofValue(upload.getUri().getPath()))
                .build();

            task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
        });

        io.kestra.plugin.fs.local.List.Output moveContents = utils().list("/tmp/trigger-move");

        Optional<io.kestra.plugin.fs.local.models.File> maybeFile = moveContents.getFiles().stream()
            .filter(f -> !f.isDirectory())
            .findFirst();

        assertThat("moved file should be found", maybeFile.isPresent(), is(true));

        Path actualMovedPath = maybeFile.get().getLocalPath().toAbsolutePath();
        assertThat("moved file must exist", Files.exists(actualMovedPath), is(true));

        Download task = Download.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(Download.class.getName())
            .from(Property.ofValue(actualMovedPath.toString()))
            .build();

        io.kestra.plugin.fs.local.Download.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
        assertThat(run.getUri().toString(), containsString("kestra://"));

        utils().delete("/tmp/trigger-move/");
    }

    @Test
    void moveWithRegexShouldNotDeleteSourceDirectory() throws Exception {
        Path sourceDir = Paths.get("/tmp/test");
        Path targetDir = Paths.get("/tmp/output");

        Files.createDirectories(sourceDir);
        Files.createDirectories(targetDir);

        try {
            String matchingFile1 = "user.txt";
            String matchingFile2 = "upload.csv";
            String nonMatchingFile = "config.yml";

            Files.write(sourceDir.resolve(matchingFile1), "test content 1".getBytes());
            Files.write(sourceDir.resolve(matchingFile2), "test content 2".getBytes());
            Files.write(sourceDir.resolve(nonMatchingFile), "test content 3".getBytes());

            io.kestra.plugin.fs.local.Trigger trigger = io.kestra.plugin.fs.local.Trigger.builder()
                .id(TriggerTest.class.getSimpleName())
                .type(io.kestra.plugin.fs.local.Trigger.class.getName())
                .from(Property.ofValue("/tmp/test/"))
                .regExp(Property.ofValue(".*u.*"))
                .action(Property.ofValue(Downloads.Action.MOVE))
                .moveDirectory(Property.ofValue("/tmp/output/"))
                .recursive(Property.ofValue(false))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

            assertThat(execution.isPresent(), is(true));

            @SuppressWarnings("unchecked")
            java.util.List<io.kestra.plugin.fs.local.models.File> files =
                (java.util.List<io.kestra.plugin.fs.local.models.File>) execution.get().getTrigger().getVariables().get("files");

            assertThat(files.size(), is(2));

            assertThat(Files.exists(sourceDir), is(true));
            assertThat(Files.exists(sourceDir.resolve(nonMatchingFile)), is(true));
            assertThat("Matching file 1 should be moved to target", Files.exists(targetDir.resolve(matchingFile1)), is(true));
            assertThat("Matching file 2 should be moved to target", Files.exists(targetDir.resolve(matchingFile2)), is(true));
            assertThat("Matching file 1 should be removed from source", Files.exists(sourceDir.resolve(matchingFile1)), is(false));
            assertThat("Matching file 2 should be removed from source", Files.exists(sourceDir.resolve(matchingFile2)), is(false));

            Optional<Execution> secondExecution = trigger.evaluate(context.getKey(), context.getValue());

            assertThat("Second execution should be empty since no files match regex", secondExecution.isPresent(), is(false));
        } finally {
            cleanup(sourceDir);
            cleanup(targetDir);
        }
    }

    @Test
    void moveWithRegexAndRecursiveShouldPreserveDirectoryStructure() throws Exception {
        Path sourceDir = Paths.get("/tmp/test-recursive");
        Path subDir1 = sourceDir.resolve("subdir1");
        Path subDir2 = sourceDir.resolve("subdir2");
        Path targetDir = Paths.get("/tmp/output-recursive");

        Files.createDirectories(subDir1);
        Files.createDirectories(subDir2);
        Files.createDirectories(targetDir);

        try {
            Files.write(sourceDir.resolve("user.txt"), "root user file".getBytes());
            Files.write(sourceDir.resolve("config.yml"), "root config file".getBytes());

            Files.write(subDir1.resolve("upload.csv"), "subdir1 upload file".getBytes());
            Files.write(subDir1.resolve("download.txt"), "subdir1 download file".getBytes());
            Files.write(subDir1.resolve("user-data.json"), "subdir1 user data".getBytes());

            Files.write(subDir2.resolve("archive.zip"), "subdir2 archive file".getBytes());
            Files.write(subDir2.resolve("update.log"), "subdir2 update log".getBytes());
            Files.write(subDir2.resolve("settings.ini"), "subdir2 settings".getBytes());
            Files.write(subDir2.resolve("report.pdf"), "subdir2 report".getBytes());

            io.kestra.plugin.fs.local.Trigger trigger = io.kestra.plugin.fs.local.Trigger.builder()
                .id(TriggerTest.class.getSimpleName())
                .type(io.kestra.plugin.fs.local.Trigger.class.getName())
                .from(Property.ofValue("/tmp/test-recursive/"))
                .regExp(Property.ofValue(".*u.*"))
                .action(Property.ofValue(Downloads.Action.MOVE))
                .moveDirectory(Property.ofValue("/tmp/output-recursive/"))
                .recursive(Property.ofValue(true))
                .build();

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

            assertThat(execution.isPresent(), is(true));

            @SuppressWarnings("unchecked")
            java.util.List<Object> rawFiles =
                (java.util.List<Object>) execution.get().getTrigger().getVariables().get("files");

            java.util.List<String> processedFileNames = rawFiles.stream()
                .filter(f -> f instanceof java.util.Map)
                .map(f -> (java.util.Map<String, Object>) f)
                .filter(map -> !Boolean.TRUE.equals(map.get("directory")))
                .map(map -> {
                    Object localPath = map.get("localPath");
                    if (localPath != null) {
                        return Paths.get(localPath.toString()).getFileName().toString();
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .sorted()
                .toList();

            java.util.Set<String> expectedMatches = java.util.Set.of(
                "user.txt",        // root file: path contains user
                "config.yml",      // root file: path contains "recursive" -> 'u'
                "upload.csv",      // subdir1: path contains "subdir" -> 'u'
                "download.txt",    // subdir1: path contains "subdir" -> 'u'
                "user-data.json",  // subdir1: path contains "subdir" -> 'u'
                "archive.zip",     // subdir2: path contains "subdir" -> 'u'
                "update.log",      // subdir2: path contains "subdir" -> 'u'
                "settings.ini",    // subdir2: path contains "subdir" -> 'u'
                "report.pdf"       // subdir2: path contains "subdir" -> 'u'
            );


            java.util.Set<String> processedSet = new java.util.HashSet<>(processedFileNames);
            assertThat(processedSet, is(expectedMatches));

            assertThat("root should only contain directories", Files.list(sourceDir).allMatch(path -> Files.isDirectory(path)), is(true));
            assertThat("subdir1 should be empty", Files.list(subDir1).count(), is(0L));
            assertThat("subdir2 should be empty", Files.list(subDir2).count(), is(0L));

            java.util.Set<String> movedFiles = new java.util.HashSet<>();

            if (Files.exists(targetDir)) {
                Files.list(targetDir).forEach(path -> {
                    String fileName = path.getFileName().toString();
                    movedFiles.add(fileName);
                });
            }

            assertThat("moved files should match processed files", movedFiles, is(processedSet));
        } finally {
            cleanup(sourceDir);
            cleanup(targetDir);
        }
    }

    @Test
    void regexFilterAppliesToFullPaths() throws Exception {
        Path sourceDir = Paths.get("/tmp/test-dir-regex");
        Path userDir = sourceDir.resolve("user-data");
        Path uploadDir = sourceDir.resolve("uploads");
        Path configDir = sourceDir.resolve("config");
        Path targetDir = Paths.get("/tmp/output-dir-regex");

        Files.createDirectories(userDir);
        Files.createDirectories(uploadDir);
        Files.createDirectories(configDir);
        Files.createDirectories(targetDir);

        try {
            Files.write(userDir.resolve("profile.xml"), "user profile".getBytes());
            Files.write(uploadDir.resolve("temp.xml"), "temp upload".getBytes());
            Files.write(configDir.resolve("settings.xml"), "config settings".getBytes());

            io.kestra.plugin.fs.local.Trigger trigger1 = io.kestra.plugin.fs.local.Trigger.builder()
                .id(TriggerTest.class.getSimpleName())
                .type(io.kestra.plugin.fs.local.Trigger.class.getName())
                .from(Property.ofValue("/tmp/test-dir-regex/"))
                .regExp(Property.ofValue(".*u.*"))
                .action(Property.ofValue(Downloads.Action.MOVE))
                .moveDirectory(Property.ofValue("/tmp/output-dir-regex/"))
                .recursive(Property.ofValue(true))
                .build();

            var context1 = TestsUtils.mockTrigger(runContextFactory, trigger1);
            Optional<Execution> execution1 = trigger1.evaluate(context1.getKey(), context1.getValue());

            assertThat("files should be processed when regex matches directory names in the path", execution1.isPresent(), is(true));

            @SuppressWarnings("unchecked")
            java.util.List<Object> rawFiles1 = (java.util.List<Object>) execution1.get().getTrigger().getVariables().get("files");
            assertThat("Should find 2 files (from user-data and uploads directories)", rawFiles1.size(), is(2));

            cleanup(targetDir);
            Files.createDirectories(targetDir);

            io.kestra.plugin.fs.local.Trigger trigger2 = io.kestra.plugin.fs.local.Trigger.builder()
                .id(TriggerTest.class.getSimpleName())
                .type(io.kestra.plugin.fs.local.Trigger.class.getName())
                .from(Property.ofValue("/tmp/test-dir-regex/"))
                .regExp(Property.ofValue(".*\\.xml"))
                .action(Property.ofValue(Downloads.Action.MOVE))
                .moveDirectory(Property.ofValue("/tmp/output-dir-regex/"))
                .recursive(Property.ofValue(true))
                .build();

            var context2 = TestsUtils.mockTrigger(runContextFactory, trigger2);
            Optional<Execution> execution2 = trigger2.evaluate(context2.getKey(), context2.getValue());

            assertThat(execution2.isPresent(), is(true));

            @SuppressWarnings("unchecked")
            java.util.List<Object> rawFiles =
                (java.util.List<Object>) execution2.get().getTrigger().getVariables().get("files");

            assertThat("should find 1 remaining XML file", rawFiles.size(), is(1));

            java.util.Set<String> movedFiles = new java.util.HashSet<>();
            if (Files.exists(targetDir)) {
                Files.list(targetDir).forEach(path -> movedFiles.add(path.getFileName().toString()));
            }

            java.util.Set<String> expectedFiles = java.util.Set.of("settings.xml");
            assertThat("remaining XML file should be moved", movedFiles, is(expectedFiles));

        } finally {
            cleanup(sourceDir);
            cleanup(targetDir);
        }
    }

    @Test
    void shouldExecuteOnCreate() throws Exception {
        Path dir = Paths.get("/tmp/local-on-create");
        Files.createDirectories(dir);

        try {
            var trigger = io.kestra.plugin.fs.local.Trigger.builder()
                .id("local-" + FriendlyId.createFriendlyId())
                .type(io.kestra.plugin.fs.local.Trigger.class.getName())
                .from(Property.ofValue(dir.toString()))
                .on(Property.ofValue(StatefulTriggerInterface.On.CREATE))
                .interval(Duration.ofSeconds(5))
                .build();

            Path file = dir.resolve("file.txt");
            Files.writeString(file, "hello create");

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

            assertThat(execution.isPresent(), is(true));
        } finally {
            cleanup(dir);
        }
    }

    @Test
    void shouldExecuteOnUpdate() throws Exception {
        Path dir = Paths.get("/tmp/local-on-update");
        Files.createDirectories(dir);

        try {
            var trigger = io.kestra.plugin.fs.local.Trigger.builder()
                .id("local-" + FriendlyId.createFriendlyId())
                .type(io.kestra.plugin.fs.local.Trigger.class.getName())
                .from(Property.ofValue(dir.toString()))
                .on(Property.ofValue(StatefulTriggerInterface.On.UPDATE))
                .interval(Duration.ofSeconds(5))
                .build();

            Path file = dir.resolve("file.txt");
            Files.writeString(file, "initial content");

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);
            trigger.evaluate(context.getKey(), context.getValue());

            Thread.sleep(1000);
            Files.writeString(file, "updated content");

            Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());
            assertThat(execution.isPresent(), is(true));
        } finally {
            cleanup(dir);
        }
    }

    @Test
    void shouldExecuteOnCreateOrUpdate() throws Exception {
        Path dir = Paths.get("/tmp/local-on-create-or-update");
        Files.createDirectories(dir);

        try {
            var trigger = io.kestra.plugin.fs.local.Trigger.builder()
                .id("local-" + FriendlyId.createFriendlyId())
                .type(io.kestra.plugin.fs.local.Trigger.class.getName())
                .from(Property.ofValue(dir.toString()))
                .on(Property.ofValue(io.kestra.core.models.triggers.StatefulTriggerInterface.On.CREATE_OR_UPDATE))
                .interval(Duration.ofSeconds(5))
                .build();

            Path file = dir.resolve("file.txt");
            Files.writeString(file, "hello world");

            var context = TestsUtils.mockTrigger(runContextFactory, trigger);

            Optional<Execution> createExecution = trigger.evaluate(context.getKey(), context.getValue());
            assertThat(createExecution.isPresent(), is(true));

            Files.writeString(file, "new content");

            Optional<Execution> updateExecution = trigger.evaluate(context.getKey(), context.getValue());
            assertThat(updateExecution.isPresent(), is(true));
        } finally {
            cleanup(dir);
        }
    }

    private void cleanup(Path directory) {
        if (Files.exists(directory)) {
            try {
                Files.walk(directory)
                    .sorted((a, b) -> b.compareTo(a))
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (Exception ignored) {}
                    });
            } catch (Exception ignored) {}
        }
    }
}