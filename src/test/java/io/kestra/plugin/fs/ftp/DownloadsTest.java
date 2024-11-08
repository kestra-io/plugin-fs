package io.kestra.plugin.fs.ftp;

import com.devskiller.friendly_id.FriendlyId;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

@KestraTest
class DownloadsTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private FtpUtils ftpUtils;

    private final String random = IdUtils.create();

    @Test
    void run_DeleteAfterDownloads() throws Exception {
        String toUploadDir = "/upload/" + random;

        String out1 = FriendlyId.createFriendlyId();
        ftpUtils.upload(toUploadDir + "/" + out1 + ".txt");
        String out2 = FriendlyId.createFriendlyId();
        ftpUtils.upload(toUploadDir + "/" + out2 + ".txt");

        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(DownloadsTest.class.getName())
            .from(Property.of(toUploadDir + "/"))
            .action(Property.of(Downloads.Action.DELETE))
            .host(Property.of("localhost"))
            .port(Property.of("6621"))
            .username(Property.of("guest"))
            .password(Property.of("guest"))
            .build();

        Downloads.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(2));
        assertThat(run.getFiles().getFirst().getPath().getPath(), endsWith(".txt"));
        assertThat(run.getOutputFiles().size(), is(2));

        assertThat(ftpUtils.list(toUploadDir).getFiles().isEmpty(), is(true));
    }

    @Test
    void run_NoneAfterDownloads() throws Exception {
        String toUploadDir = "/upload/" + random;
        String out1 = FriendlyId.createFriendlyId();
        ftpUtils.upload(toUploadDir + "/" + out1 + ".txt");
        String out2 = FriendlyId.createFriendlyId();
        ftpUtils.upload(toUploadDir + "/" + out2 + ".txt");

        Downloads task = Downloads.builder()
            .id(DownloadsTest.class.getSimpleName())
            .type(DownloadsTest.class.getName())
            .from(Property.of(toUploadDir + "/"))
            .action(Property.of(Downloads.Action.NONE))
            .host(Property.of("localhost"))
            .port(Property.of("6621"))
            .username(Property.of("guest"))
            .password(Property.of("guest"))
            .build();

        Downloads.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(run.getFiles().size(), is(2));
        assertThat(run.getFiles().getFirst().getPath().getPath(), endsWith(".txt"));
        assertThat(run.getOutputFiles().size(), is(2));

        assertThat(ftpUtils.list(toUploadDir).getFiles().size(), is(2));
    }
}
