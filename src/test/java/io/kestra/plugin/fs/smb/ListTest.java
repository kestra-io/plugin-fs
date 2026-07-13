package io.kestra.plugin.fs.smb;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.vfs.models.File;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.kestra.plugin.fs.smb.SmbUtils.PASSWORD;
import static io.kestra.plugin.fs.smb.SmbUtils.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class ListTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private SmbUtils smbUtils;

    @Test
    void all() throws Exception {
        String dir = "/" + IdUtils.create();
        String lastFile = null;
        for (int i = 0; i < 6; i++) {
            lastFile = IdUtils.create();
            smbUtils.upload(SmbUtils.SHARE_NAME + dir + "/" + lastFile + ".yaml");
            smbUtils.upload(SmbUtils.SHARE_NAME + dir + "/subfolder/" + lastFile + ".yaml");
        }
        smbUtils.upload(SmbUtils.SHARE_NAME + dir + "/file with space.yaml");

        // List task
        List.ListBuilder<?, ?> builder = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(ListTest.class.getName())
            .from(Property.ofValue(SmbUtils.SHARE_NAME + dir))
            .host(Property.ofValue("localhost"))
            .username(USERNAME)
            .password(PASSWORD);

        List task = builder.build();

        io.kestra.plugin.fs.vfs.List.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(7));

        task = builder
            .regExp(Property.ofValue(".*\\" + dir + "\\/" + lastFile + "\\.(yml|yaml)"))
            .build();

        run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(1));

        task = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(ListTest.class.getName())
            .from(Property.ofValue(SmbUtils.SHARE_NAME + dir))
            .host(Property.ofValue("localhost"))
            .username(USERNAME)
            .password(PASSWORD)
            .recursive(Property.ofValue(true)).build();

        run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(13));

        task = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(ListTest.class.getName())
            .from(Property.ofValue(SmbUtils.SECOND_SHARE_NAME + dir))
            .host(Property.ofValue("localhost"))
            .username(USERNAME)
            .password(PASSWORD)
            .recursive(Property.ofValue(true))
            .build();

        run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(0));
    }

    @Test
    void shouldHandleHashInPath() throws Exception {
        String dir = "/" + IdUtils.create();

        smbUtils.upload(SmbUtils.SHARE_NAME + dir + "/Run #1/Sub Folder/file.txt");

        List task = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(ListTest.class.getName())
            .from(Property.ofValue(SmbUtils.SHARE_NAME + dir))
            .host(Property.ofValue("localhost"))
            .username(USERNAME)
            .password(PASSWORD)
            .recursive(Property.ofValue(true))
            .build();

        io.kestra.plugin.fs.vfs.List.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(1));

        String path = run.getFiles().getFirst().getPath().getRawPath();

        assertThat(path.contains("%23"), is(true));
    }

    @Test
    void sortAppliesBeforeMaxFilesTruncation() throws Exception {
        String dir = "/" + IdUtils.create();
        smbUtils.upload(SmbUtils.SHARE_NAME + dir + "/b.txt");
        smbUtils.upload(SmbUtils.SHARE_NAME + dir + "/a.txt");
        smbUtils.upload(SmbUtils.SHARE_NAME + dir + "/c.txt");

        List.ListBuilder<?, ?> builder = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(ListTest.class.getName())
            .from(Property.ofValue(SmbUtils.SHARE_NAME + dir))
            .host(Property.ofValue("localhost"))
            .username(USERNAME)
            .password(PASSWORD);

        List task = builder.sort(Property.ofValue(io.kestra.plugin.fs.vfs.List.Sort.NAME_ASC)).build();
        io.kestra.plugin.fs.vfs.List.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
        assertThat(run.getFiles().stream().map(File::getName).toList(), contains("a.txt", "b.txt", "c.txt"));

        task = builder.sort(Property.ofValue(io.kestra.plugin.fs.vfs.List.Sort.NAME_DESC)).maxFiles(Property.ofValue(2)).build();
        run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
        assertThat(run.getFiles().stream().map(File::getName).toList(), contains("c.txt", "b.txt"));
    }

    @Test
    void sortByLastModified() throws Exception {
        String dir = "/" + IdUtils.create();
        smbUtils.upload(SmbUtils.SHARE_NAME + dir + "/older.txt");
        Thread.sleep(1200);
        smbUtils.upload(SmbUtils.SHARE_NAME + dir + "/newer.txt");

        List.ListBuilder<?, ?> builder = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(ListTest.class.getName())
            .from(Property.ofValue(SmbUtils.SHARE_NAME + dir))
            .host(Property.ofValue("localhost"))
            .username(USERNAME)
            .password(PASSWORD);

        List task = builder.sort(Property.ofValue(io.kestra.plugin.fs.vfs.List.Sort.LAST_MODIFIED_ASC)).build();
        io.kestra.plugin.fs.vfs.List.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
        run.getFiles().forEach(file -> assertThat(file.getUpdatedDate(), is(notNullValue())));
        assertThat(run.getFiles().stream().map(File::getName).toList(), contains("older.txt", "newer.txt"));

        task = builder.sort(Property.ofValue(io.kestra.plugin.fs.vfs.List.Sort.LAST_MODIFIED_DESC)).build();
        run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
        assertThat(run.getFiles().stream().map(File::getName).toList(), contains("newer.txt", "older.txt"));
    }
}
