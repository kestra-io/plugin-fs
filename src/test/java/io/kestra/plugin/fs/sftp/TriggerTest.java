package io.kestra.plugin.fs.sftp;

import com.devskiller.friendly_id.FriendlyId;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.AbstractFileTriggerTest;
import io.kestra.plugin.fs.AbstractUtils;
import io.kestra.plugin.fs.vfs.Upload;
import io.kestra.plugin.fs.vfs.models.File;
import jakarta.inject.Inject;
import org.apache.commons.vfs2.FileSystemException;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static io.kestra.plugin.fs.sftp.SftpUtils.PASSWORD;
import static io.kestra.plugin.fs.sftp.SftpUtils.USERNAME;
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
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .from(Property.ofValue("/upload/trigger/"))
            .action(Property.ofValue(Downloads.Action.MOVE))
            .moveDirectory(Property.ofValue("/upload/trigger-move/"))
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
                .host(Property.ofValue("localhost"))
                .port(Property.ofValue("6622"))
                .username(USERNAME)
                .password(PASSWORD)
                .from(Property.ofValue(upload.getTo().toString()))
                .build();

            task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
        });

        Download task = Download.builder()
            .id(AbstractFileTriggerTest.class.getSimpleName())
            .type(Download.class.getName())
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .from(Property.ofValue("/upload/trigger" + "-move/" + out + ".yml"))
            .build();

        io.kestra.plugin.fs.vfs.Download.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
        assertThat(run.getTo().toString(), containsString("kestra://"));
    }
}
