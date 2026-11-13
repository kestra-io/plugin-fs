package io.kestra.plugin.fs.sftp;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.vfs.models.File;
import jakarta.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.regex.Pattern;

import static io.kestra.plugin.fs.sftp.SftpUtils.PASSWORD;
import static io.kestra.plugin.fs.sftp.SftpUtils.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

/**
 * Names with square brackets (#405): Commons VFS skipped the {@code %XX} sequences between brackets
 * when decoding, so these files were listed, downloaded, moved, deleted and uploaded under the wrong name.
 * Fixtures are written and checked with a raw SFTP client so the server-side names are the real ones.
 */
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
    @ValueSource(strings = {"report[1].txt", "report[100%].txt", "report[%41]+1.txt"})
    void shouldListAndMatchRegExpWhenNameContainsBrackets(String fileName) throws Exception {
        var dir = "/upload/" + IdUtils.create();
        sftpUtils.putRaw(dir, fileName, CONTENT);
        sftpUtils.putRaw(dir, "other.txt", CONTENT);

        var task = List.builder()
            .id(BracketFileNameTest.class.getSimpleName())
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
    @ValueSource(strings = {"report[1].txt", "report[100%].txt", "report[%41]+1.txt"})
    void shouldDownloadWhenNameContainsBrackets(String fileName) throws Exception {
        var dir = "/upload/" + IdUtils.create();
        sftpUtils.putRaw(dir, fileName, CONTENT);

        var task = Download.builder()
            .id(BracketFileNameTest.class.getSimpleName())
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
    @ValueSource(strings = {"report[1].txt", "report[100%].txt", "report[%41]+1.txt"})
    void shouldDownloadsAndDeleteWhenNameContainsBrackets(String fileName) throws Exception {
        var dir = "/upload/" + IdUtils.create();
        sftpUtils.putRaw(dir, fileName, CONTENT);

        var task = downloads(dir, Downloads.Action.DELETE, null);

        var run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().stream().map(File::getName).toList(), contains(fileName));
        assertThat(run.getOutputFiles().size(), is(1));
        var downloaded = run.getOutputFiles().values().iterator().next();
        assertThat(IOUtils.toString(storageInterface.get(TenantService.MAIN_TENANT, null, downloaded), StandardCharsets.UTF_8), is(CONTENT));
        assertThat(sftpUtils.listRaw(dir), is(empty()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"report[1].txt", "report[100%].txt", "report[%41]+1.txt"})
    void shouldDownloadsAndMoveWhenNameContainsBrackets(String fileName) throws Exception {
        var dir = "/upload/" + IdUtils.create();
        var moveDir = "/upload/" + IdUtils.create() + "-moved";
        sftpUtils.putRaw(dir, fileName, CONTENT);

        var task = downloads(dir, Downloads.Action.MOVE, moveDir + "/");

        var run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().stream().map(File::getName).toList(), contains(fileName));
        assertThat(sftpUtils.listRaw(dir), is(empty()));
        assertThat(sftpUtils.listRaw(moveDir), contains(fileName));
    }

    @ParameterizedTest
    @ValueSource(strings = {"report[1].txt", "report[100%].txt", "report[%41]+1.txt"})
    void shouldDeleteWhenNameContainsBrackets(String fileName) throws Exception {
        var dir = "/upload/" + IdUtils.create();
        sftpUtils.putRaw(dir, fileName, CONTENT);

        var task = Delete.builder()
            .id(BracketFileNameTest.class.getSimpleName())
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
    @ValueSource(strings = {"report[1].txt", "report[100%].txt", "report[%41]+1.txt"})
    void shouldDeleteByRegExpWhenNameContainsBrackets(String fileName) throws Exception {
        var dir = "/upload/" + IdUtils.create();
        sftpUtils.putRaw(dir, fileName, CONTENT);
        sftpUtils.putRaw(dir, "other.txt", CONTENT);

        var task = Delete.builder()
            .id(BracketFileNameTest.class.getSimpleName())
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
        assertThat(sftpUtils.listRaw(dir), contains("other.txt"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"report[1].txt", "report[100%].txt", "report[%41]+1.txt"})
    void shouldUploadWithExactNameWhenNameContainsBrackets(String fileName) throws Exception {
        var dir = "/upload/" + IdUtils.create();

        sftpUtils.upload(dir + "/" + fileName);

        assertThat(sftpUtils.listRaw(dir), contains(fileName));
    }

    private Downloads downloads(String dir, Downloads.Action action, String moveDirectory) {
        return Downloads.builder()
            .id(BracketFileNameTest.class.getSimpleName())
            .type(Downloads.class.getName())
            .from(Property.ofValue(dir + "/"))
            .action(Property.ofValue(action))
            .moveDirectory(moveDirectory == null ? null : Property.ofValue(moveDirectory))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();
    }
}
