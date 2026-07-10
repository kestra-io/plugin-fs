package io.kestra.plugin.fs.ftp;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.vfs.models.File;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static io.kestra.plugin.fs.ftp.FtpUtils.PASSWORD;
import static io.kestra.plugin.fs.ftp.FtpUtils.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

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
    void maxFilesShouldLimit() throws Exception {
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

        assertThat(run.getFiles().size(), is(1));
    }

    @Test
    void sortByNameAscAndDesc() throws Exception {
        String dir = "/" + IdUtils.create();
        ftpUtils.upload("upload" + dir + "/b.txt");
        ftpUtils.upload("upload" + dir + "/a.txt");
        ftpUtils.upload("upload" + dir + "/c.txt");

        List.ListBuilder<?, ?> builder = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(ListTest.class.getName())
            .from(Property.ofValue("/upload" + dir))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6621"))
            .username(USERNAME)
            .password(PASSWORD);

        List task = builder.sort(Property.ofValue(List.Sort.NAME_ASC)).build();
        List.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
        assertThat(run.getFiles().stream().map(File::getName).toList(), contains("a.txt", "b.txt", "c.txt"));

        task = builder.sort(Property.ofValue(List.Sort.NAME_DESC)).build();
        run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
        assertThat(run.getFiles().stream().map(File::getName).toList(), contains("c.txt", "b.txt", "a.txt"));
    }

    @Test
    void sortAppliesBeforeMaxFilesTruncation() throws Exception {
        String dir = "/" + IdUtils.create();
        ftpUtils.upload("upload" + dir + "/b.txt");
        ftpUtils.upload("upload" + dir + "/a.txt");
        ftpUtils.upload("upload" + dir + "/c.txt");

        List task = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(ListTest.class.getName())
            .from(Property.ofValue("/upload" + dir))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6621"))
            .username(USERNAME)
            .password(PASSWORD)
            .sort(Property.ofValue(List.Sort.NAME_DESC))
            .maxFiles(Property.ofValue(2))
            .build();

        List.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().stream().map(File::getName).toList(), contains("c.txt", "b.txt"));
    }

    @Test
    void sortByLastModifiedAlsoAssertsUpdatedDateIsPopulated() throws Exception {
        // Regression: updatedDate used to be populated via reflection into a jsch-specific field that
        // only existed for the SFTP provider, so it stayed null for FTP. Assert it is now set here too.
        // Note: vsftpd's LIST output only has minute-level precision by default, so we assert the result
        // is sorted (ties allowed) rather than an exact upload-order match, which would be flaky.
        String dir = "/" + IdUtils.create();
        ftpUtils.upload("upload" + dir + "/older.txt");
        ftpUtils.upload("upload" + dir + "/newer.txt");

        List.ListBuilder<?, ?> builder = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(ListTest.class.getName())
            .from(Property.ofValue("/upload" + dir))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6621"))
            .username(USERNAME)
            .password(PASSWORD);

        List task = builder.sort(Property.ofValue(List.Sort.LAST_MODIFIED_ASC)).build();
        List.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
        java.util.List<Instant> ascDates = run.getFiles().stream().map(File::getUpdatedDate).toList();
        ascDates.forEach(date -> assertThat(date, is(notNullValue())));
        assertThat(isNonDecreasing(ascDates), is(true));

        task = builder.sort(Property.ofValue(List.Sort.LAST_MODIFIED_DESC)).build();
        run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
        java.util.List<Instant> descDates = run.getFiles().stream().map(File::getUpdatedDate).toList();
        assertThat(isNonIncreasing(descDates), is(true));
    }

    private static boolean isNonDecreasing(java.util.List<Instant> dates) {
        for (int i = 1; i < dates.size(); i++) {
            if (dates.get(i - 1).isAfter(dates.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isNonIncreasing(java.util.List<Instant> dates) {
        for (int i = 1; i < dates.size(); i++) {
            if (dates.get(i - 1).isBefore(dates.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Test
    void sortShouldNotThrowOnEmptyList() throws Exception {
        String dir = "/" + IdUtils.create();
        ftpUtils.upload("upload" + dir + "/only-file.txt");

        List task = List.builder()
            .id(ListTest.class.getSimpleName())
            .type(ListTest.class.getName())
            .from(Property.ofValue("/upload" + dir))
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6621"))
            .username(USERNAME)
            .password(PASSWORD)
            .regExp(Property.ofValue(".*\\.doesnotexist"))
            .sort(Property.ofValue(List.Sort.NAME_ASC))
            .build();

        List.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(0));
    }
}
