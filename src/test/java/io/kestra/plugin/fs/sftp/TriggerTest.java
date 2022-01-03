package io.kestra.plugin.fs.sftp;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.AbstractTriggerTest;
import io.kestra.plugin.fs.vfs.Upload;
import io.kestra.plugin.fs.vfs.models.File;
import jakarta.inject.Inject;
import org.apache.commons.vfs2.FileSystemException;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TriggerTest extends AbstractTriggerTest {
    @Inject
    private SftpUtils sftpUtils;

    @Override
    public Upload.Output upload(String to) throws Exception {
        return this.sftpUtils.upload(to);
    }

    @Test
    void move() throws Exception {
        Trigger trigger = Trigger.builder()
            .id(AbstractTriggerTest.class.getSimpleName())
            .type(Trigger.class.getName())
            .host("localhost")
            .port("6622")
            .username("foo")
            .password("pass")
            .from("/upload/" + random + "/")
            .action(Downloads.Action.MOVE)
            .moveDirectory("/upload/" + random + "-move/")
            .build();

        String out = FriendlyId.createFriendlyId();
        Upload.Output upload = upload("/upload/" + random + "/" + out + ".yml");

        Map.Entry<ConditionContext, TriggerContext> context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        java.util.List<File> urls = (java.util.List<File>) execution.get().getTrigger().getVariables().get("files");
        assertThat(urls.size(), is(1));

        assertThrows(FileSystemException.class, () -> {
            Download task = Download.builder()
                .id(AbstractTriggerTest.class.getSimpleName())
                .type(Download.class.getName())
                .host("localhost")
                .port("6622")
                .username("foo")
                .password("pass")
                .from(upload.getTo().toString())
                .build();

            task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
        });

        Download task = Download.builder()
            .id(AbstractTriggerTest.class.getSimpleName())
            .type(Download.class.getName())
            .host("localhost")
            .port("6622")
            .username("foo")
            .password("pass")
            .from("/upload/" + random + "-move/" + out + ".yml")
            .build();

        io.kestra.plugin.fs.vfs.Download.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
        assertThat(run.getTo().toString(), containsString("kestra://"));
    }
}
