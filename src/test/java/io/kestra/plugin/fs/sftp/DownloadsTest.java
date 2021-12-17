package io.kestra.plugin.fs.sftp;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
class DownloadsTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private SftpUtils sftpUtils;

    @Value("${kestra.variables.globals.random}")
    private String random;

    @Test
    void run() throws Exception {
        String out1 = FriendlyId.createFriendlyId();
        sftpUtils.upload("/upload/" + random + "/" + out1);
        String out2 = FriendlyId.createFriendlyId();
        sftpUtils.upload("/upload/" + random + "/" + out2);

        Downloads task = Downloads.builder()
            .id(MoveTest.class.getSimpleName())
            .type(Move.class.getName())
            .from("/upload/" + random + "/")
            .action(Downloads.Action.DELETE)
            .host("localhost")
            .port("6622")
            .username("foo")
            .password("pass")
            .build();

        Downloads.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));

        assertThat(run.getFiles().size(), is(2));
    }
}
