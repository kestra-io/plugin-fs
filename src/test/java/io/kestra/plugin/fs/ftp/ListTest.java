package io.kestra.plugin.fs.ftp;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static io.kestra.plugin.fs.ftp.FtpUtils.PASSWORD;
import static io.kestra.plugin.fs.ftp.FtpUtils.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class ListTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private FtpUtils ftpUtils;

    @Test
    void all() throws Exception {
        String dir = "/" + IdUtils.create();
        String lastFile = null;
        for (int i = 0; i < 6; i++) {
            lastFile = IdUtils.create();
            ftpUtils.upload("upload" + dir + "/" + lastFile + ".yaml");
            ftpUtils.upload("upload" + dir + "/subfolder/" + lastFile + ".yaml");
        }
        ftpUtils.upload("upload" + dir + "/file with space.yaml");

        // List task
        List.ListBuilder<?, ?> builder = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(ListTest.class.getName())
            .from(Property.ofValue("/upload" + dir))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6621"))
            .username(USERNAME)
            .password(PASSWORD);

        List task = builder.build();

        List.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(7));

        task = builder
            .regExp(Property.ofValue(".*\\" + dir + "\\/" + lastFile + "\\.(yml|yaml)"))
            .build();

        run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(1));

        task = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(ListTest.class.getName())
            .from(Property.ofValue("/upload" + dir))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6621"))
            .username(USERNAME)
            .password(PASSWORD)
            .recursive(Property.ofValue(true)).build();

        run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(13));

        task = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(ListTest.class.getName())
            .from(Property.ofValue("/" + dir))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6621"))
            .username(USERNAME)
            .password(PASSWORD)
            .recursive(Property.ofValue(true)).build();

        run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(0));
    }

    @Test
    void shouldMatchFileWithWhitespaceInName() throws Exception {
        String dir = "/" + IdUtils.create();
        String filenameWithSpace = "test Test_nbs_issuers_20250717.csv";
        ftpUtils.upload("upload" + dir + "/" + filenameWithSpace);

        List.ListBuilder<?, ?> builder = List.builder()
            .id("ftp-list-" + UUID.randomUUID())
            .type(List.class.getName())
            .from(Property.ofValue("/upload" + dir))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6621"))
            .username(USERNAME)
            .password(PASSWORD);

        // here we check using regex with a whitespace in file name
        List task = builder
            .regExp(Property.ofValue(".*test Test_nbs_issuers_.+\\.csv"))
            .build();

        List.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(1));
        assertThat(run.getFiles().getFirst().getName(), is(filenameWithSpace));
    }

    @Test
    void shouldMatchFileWithStandardName() throws Exception {
        String dir = "/" + IdUtils.create();
        String filename = "test_Test_nbs_issuers_20250717.csv";
        ftpUtils.upload("upload" + dir + "/" + filename);

        List.ListBuilder<?, ?> builder = List.builder()
            .id("ftp-list-" + UUID.randomUUID())
            .type(List.class.getName())
            .from(Property.ofValue("/upload" + dir))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6621"))
            .username(USERNAME)
            .password(PASSWORD);

        List task = builder
            .regExp(Property.ofValue(".*test_Test_nbs_issuers_.+\\.csv"))
            .build();

        List.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
        assertThat(run.getFiles().size(), is(1));
        assertThat(run.getFiles().getFirst().getName(), is(filename));
    }

    @Test
    void maxFilesShouldSkip() throws Exception {
        String dir = "/" + IdUtils.create();
        ftpUtils.upload("upload" + dir + "/file1.yaml");
        ftpUtils.upload("upload" + dir + "/file2.yaml");

        List task = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(ListTest.class.getName())
            .from(Property.ofValue("/upload" + dir))
            .maxFiles(Property.ofValue(1))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6621"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        List.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(0));
    }
}
