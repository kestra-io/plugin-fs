package io.kestra.plugin.fs.sftp;

import com.devskiller.friendly_id.FriendlyId;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.AbstractFileTriggerTest;
import io.kestra.plugin.fs.AbstractUtils;
import io.kestra.plugin.fs.vfs.List;
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

public class TriggerTest extends AbstractFileTriggerTest {
    @Inject
    private SftpUtils sftpUtils;

    @Override
    protected String triggeringFlowId() {
        return "sftp-listen";
    }

    @Override
    protected AbstractUtils utils() {
        return sftpUtils;
    }

    @Test
    void move() throws Exception {
        Trigger trigger = Trigger.builder()
            .id(AbstractFileTriggerTest.class.getSimpleName())
            .type(Trigger.class.getName())
            .host("localhost")
            .port("6622")
            .username("foo")
            .password("pass")
            .from("/upload/trigger/")
            .action(Downloads.Action.MOVE)
            .moveDirectory("/upload/trigger-move/")
            .build();

        String out = FriendlyId.createFriendlyId();
        Upload.Output upload = utils().upload("/upload/trigger/" + out + ".yml");

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        java.util.List<File> urls = (java.util.List<File>) execution.get().getTrigger().getVariables().get("files");
        assertThat(urls.size(), is(1));

        assertThrows(FileSystemException.class, () -> {
            Download task = Download.builder()
                .id(AbstractFileTriggerTest.class.getSimpleName())
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
            .id(AbstractFileTriggerTest.class.getSimpleName())
            .type(Download.class.getName())
            .host("localhost")
            .port("6622")
            .username("foo")
            .password("pass")
            .from("/upload/trigger" + "-move/" + out + ".yml")
            .build();

        io.kestra.plugin.fs.vfs.Download.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
        assertThat(run.getTo().toString(), containsString("kestra://"));
    }
}
