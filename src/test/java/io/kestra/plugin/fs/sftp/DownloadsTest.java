package io.kestra.plugin.fs.sftp;

import com.devskiller.friendly_id.FriendlyId;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.vfs.models.File;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.kestra.plugin.fs.sftp.SftpUtils.PASSWORD;
import static io.kestra.plugin.fs.sftp.SftpUtils.USERNAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class DownloadsTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private SftpUtils sftpUtils;

    private final String random = IdUtils.create();

    @Test
    void run_DeleteAfterDownloads() throws Exception {
        String out1 = FriendlyId.createFriendlyId();
        String toUploadDir = "/upload/" + random;
        sftpUtils.upload(toUploadDir + "/" + out1 + ".txt");
        String out2 = FriendlyId.createFriendlyId();
        sftpUtils.upload(toUploadDir + "/" + out2 + ".txt");

        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(DownloadsTest.class.getName())
            .from(Property.of(toUploadDir + "/"))
            .action(Property.of(Downloads.Action.DELETE))
            .host(Property.of("localhost"))
            .port(Property.of("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        Downloads.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(2));
        assertThat(run.getFiles().getFirst().getPath().getPath(), endsWith(".txt"));
        assertThat(run.getOutputFiles().size(), is(2));

        assertThat(sftpUtils.list(toUploadDir).getFiles().isEmpty(), is(true));
    }

    @Test
    void run_NoneAfterDownloads() throws Exception {
        String out1 = FriendlyId.createFriendlyId();
        String toUploadDir = "/upload/" + random;
        sftpUtils.upload(toUploadDir + "/" + out1 + ".txt");
        String out2 = FriendlyId.createFriendlyId();
        sftpUtils.upload(toUploadDir + "/" + out2 + ".txt");

        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(DownloadsTest.class.getName())
            .from(Property.of(toUploadDir + "/"))
            .action(Property.of(Downloads.Action.NONE))
            .host(Property.of("localhost"))
            .port(Property.of("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        Downloads.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(2));
        assertThat(run.getFiles().getFirst().getPath().getPath(), endsWith(".txt"));
        assertThat(run.getOutputFiles().size(), is(2));

        assertThat(sftpUtils.list(toUploadDir).getFiles().size(), is(2));
    }

    @Test
    void run_shouldDownloadFileWithNameContainingDotsAndSpaces() throws Exception {
        String toUploadDir = "/upload/" + random;

        final String fileName1 = IdUtils.create() + "file 1 name with spaces .and some dots.txt";
        final String fileName2 = IdUtils.create() + "file 2 name with spaces .and some dots.txt";

        sftpUtils.upload(toUploadDir + "/" + fileName1);
        sftpUtils.upload(toUploadDir + "/" + fileName2);

        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(DownloadsTest.class.getName())
            .from(Property.of(toUploadDir + "/"))
            .action(Property.of(Downloads.Action.NONE))
            .host(Property.of("localhost"))
            .port(Property.of("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .build();

        Downloads.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(2));
        assertThat(run.getFiles().getFirst().getPath().getPath(), endsWith(".txt"));
        assertThat(run.getOutputFiles().size(), is(2));

        List<File> files = sftpUtils.list(toUploadDir).getFiles();
        assertThat(files.size(), is(2));
        assertThat(files.stream().map(File::getName).toList().toArray(), arrayContainingInAnyOrder(fileName1, fileName2));
    }
}
