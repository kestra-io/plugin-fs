package io.kestra.plugin.fs.sftp;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.vfs.models.File;
import jakarta.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static io.kestra.plugin.fs.sftp.SftpUtils.PASSWORD;
import static io.kestra.plugin.fs.sftp.SftpUtils.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

// #405: names with '[' / ']' were mangled by VFS; fixtures are written and checked with a raw client
@KestraTest
class BracketFileNameTest {
    private static final String CONTENT = "bracket content";

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private SftpUtils sftpUtils;

    @Inject
    private StorageInterface storageInterface;

    @ParameterizedTest
    @ValueSource(strings = {"report[1].txt", "report[100%].txt", "report[%41]+1.txt", "report[100%.txt", "x]y[z%.txt"})
    void shouldListAndMatchRegExpWhenNameContainsBrackets(String fileName) throws Exception {
        var dir = "/upload/" + IdUtils.create();
        sftpUtils.putRaw(dir, fileName, CONTENT);
        sftpUtils.putRaw(dir, "other.txt", CONTENT);

        var task = List.builder()
            .id(IdUtils.create())
            .type(List.class.getName())
            .from(Property.ofValue(dir))
            .regExp(Property.ofValue(".*/" + Pattern.quote(fileName)))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        var run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().stream().map(File::getName).toList(), contains(fileName));
        assertThat(run.getFiles().getFirst().getPath().getPath(), is(dir + "/" + fileName));
        assertThat(run.getFiles().getFirst().getSize(), is((long) CONTENT.length()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"report[1].txt", "report[100%].txt", "report[%41]+1.txt", "report[100%.txt", "x]y[z%.txt"})
    void shouldDownloadWhenNameContainsBrackets(String fileName) throws Exception {
        var dir = "/upload/" + IdUtils.create();
        sftpUtils.putRaw(dir, fileName, CONTENT);

        var task = Download.builder()
            .id(IdUtils.create())
            .type(Download.class.getName())
            .from(Property.ofValue(dir + "/" + fileName))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        var run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(IOUtils.toString(storageInterface.get(TenantService.MAIN_TENANT, null, run.getTo()), StandardCharsets.UTF_8), is(CONTENT));
        assertThat(run.getFrom().getPath(), is(dir + "/" + fileName));
    }

    @ParameterizedTest
    @ValueSource(strings = {"report[1].txt", "report[100%].txt", "report[%41]+1.txt", "report[100%.txt", "x]y[z%.txt"})
    void shouldDownloadsAndDeleteWhenNameContainsBrackets(String fileName) throws Exception {
        var dir = "/upload/" + IdUtils.create();
        sftpUtils.putRaw(dir, fileName, CONTENT);

        var task = downloads(dir, Downloads.Action.DELETE, null, false);

        var run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().stream().map(File::getName).toList(), contains(fileName));
        assertThat(run.getOutputFiles().size(), is(1));
        var downloaded = run.getOutputFiles().values().iterator().next();
        assertThat(IOUtils.toString(storageInterface.get(TenantService.MAIN_TENANT, null, downloaded), StandardCharsets.UTF_8), is(CONTENT));
        assertThat(sftpUtils.listRaw(dir), is(empty()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"report[1].txt", "report[100%].txt", "report[%41]+1.txt", "report[100%.txt", "x]y[z%.txt"})
    void shouldDownloadsAndMoveWhenNameContainsBrackets(String fileName) throws Exception {
        var dir = "/upload/" + IdUtils.create();
        var moveDir = "/upload/" + IdUtils.create() + "-moved";
        sftpUtils.putRaw(dir, fileName, CONTENT);

        var task = downloads(dir, Downloads.Action.MOVE, moveDir + "/", false);

        var run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().stream().map(File::getName).toList(), contains(fileName));
        assertThat(sftpUtils.listRaw(dir), is(empty()));
        assertThat(sftpUtils.listRaw(moveDir), contains(fileName));
    }

    @Test
    void shouldDownloadsRecursiveAndMoveWhenDirectoryContainsBrackets() throws Exception {
        var dir = "/upload/" + IdUtils.create();
        var subDir = "sub[100%]";
        var moveDir = "/upload/" + IdUtils.create() + "-moved";
        sftpUtils.putRaw(dir + "/" + subDir, "report[1].txt", CONTENT);

        var task = downloads(dir, Downloads.Action.MOVE, moveDir + "/", true);

        var run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().stream().map(File::getName).toList(), contains("report[1].txt"));
        assertThat(sftpUtils.listRaw(dir + "/" + subDir), is(empty()));
        assertThat(sftpUtils.listRaw(moveDir + "/" + subDir), contains("report[1].txt"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"report[1].txt", "report[100%].txt", "report[%41]+1.txt", "report[100%.txt", "x]y[z%.txt"})
    void shouldDeleteWhenNameContainsBrackets(String fileName) throws Exception {
        var dir = "/upload/" + IdUtils.create();
        sftpUtils.putRaw(dir, fileName, CONTENT);

        var task = Delete.builder()
            .id(IdUtils.create())
            .type(Delete.class.getName())
            .uri(Property.ofValue(dir + "/" + fileName))
            .errorOnMissing(Property.ofValue(true))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        var run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.isDeleted(), is(true));
        assertThat(sftpUtils.listRaw(dir), is(empty()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"report[1].txt", "report[100%].txt", "report[%41]+1.txt", "report[100%.txt", "x]y[z%.txt"})
    void shouldDeleteByRegExpWhenNameContainsBrackets(String fileName) throws Exception {
        var dir = "/upload/" + IdUtils.create();
        sftpUtils.putRaw(dir, fileName, CONTENT);
        sftpUtils.putRaw(dir, "other.txt", CONTENT);

        var task = Delete.builder()
            .id(IdUtils.create())
            .type(Delete.class.getName())
            .uri(Property.ofValue(dir))
            .regExp(Property.ofValue(".*/" + Pattern.quote(fileName)))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        var run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getUris().size(), is(1));
        assertThat(run.getUris().getFirst().getPath(), is(dir + "/" + fileName));
        assertThat(sftpUtils.listRaw(dir), contains("other.txt"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"report[1].txt", "report[100%].txt", "report[%41]+1.txt", "report[100%.txt", "x]y[z%.txt"})
    void shouldMoveWhenNameContainsBrackets(String fileName) throws Exception {
        var dir = "/upload/" + IdUtils.create();
        var target = "/upload/" + IdUtils.create() + "/moved " + fileName;
        sftpUtils.putRaw(dir, fileName, CONTENT);

        var task = Move.builder()
            .id(IdUtils.create())
            .type(Move.class.getName())
            .from(Property.ofValue(dir + "/" + fileName))
            .to(Property.ofValue(target))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(sftpUtils.listRaw(dir), is(empty()));
        assertThat(sftpUtils.listRaw(target.substring(0, target.lastIndexOf('/'))), contains("moved " + fileName));
    }

    @ParameterizedTest
    @ValueSource(strings = {"report[1].txt", "report[100%].txt", "report[%41]+1.txt", "report[100%.txt", "x]y[z%.txt"})
    void shouldUploadWithExactNameWhenNameContainsBrackets(String fileName) throws Exception {
        var dir = "/upload/" + IdUtils.create();

        sftpUtils.upload(dir + "/" + fileName);

        assertThat(sftpUtils.listRaw(dir), contains(fileName));
    }

    @ParameterizedTest
    @ValueSource(strings = {"report[1].txt", "report[100%].txt"})
    void shouldTriggerAndMoveWhenNameContainsBrackets(String fileName) throws Exception {
        var dir = "/upload/" + IdUtils.create();
        var moveDir = "/upload/" + IdUtils.create() + "-moved";
        sftpUtils.putRaw(dir, fileName, CONTENT);

        var trigger = Trigger.builder()
            .id(IdUtils.create())
            .type(Trigger.class.getName())
            .from(Property.ofValue(dir + "/"))
            .action(Property.ofValue(Downloads.Action.MOVE))
            .moveDirectory(Property.ofValue(moveDir + "/"))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));
        @SuppressWarnings("unchecked")
        var files = (java.util.List<Map<String, Object>>) execution.get().getTrigger().getVariables().get("files");
        assertThat(files.stream().map(file -> file.get("name")).toList(), contains(fileName));
        assertThat(sftpUtils.listRaw(dir), is(empty()));
        assertThat(sftpUtils.listRaw(moveDir), contains(fileName));
    }

    private Downloads downloads(String dir, Downloads.Action action, String moveDirectory, boolean recursive) {
        return Downloads.builder()
            .id(IdUtils.create())
            .type(Downloads.class.getName())
            .from(Property.ofValue(dir + "/"))
            .action(Property.ofValue(action))
            .moveDirectory(moveDirectory == null ? null : Property.ofValue(moveDirectory))
            .recursive(Property.ofValue(recursive))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();
    }
}
