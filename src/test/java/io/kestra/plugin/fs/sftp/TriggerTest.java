package io.kestra.plugin.fs.sftp;

import com.devskiller.friendly_id.FriendlyId;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.AbstractFileTriggerTest;
import io.kestra.plugin.fs.AbstractUtils;
import io.kestra.plugin.fs.vfs.Upload;
import io.kestra.plugin.fs.vfs.models.File;
import jakarta.inject.Inject;
import org.apache.commons.vfs2.FileSystemException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
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

    @Test
    void shouldTriggerOnCreate() throws Exception {
        var trigger = Trigger.builder()
            .id("sftp-create")
            .type(Trigger.class.getName())
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .action(Property.ofValue(Downloads.Action.NONE))
            .from(Property.ofValue("/upload/trigger/on-create/"))
            .on(Property.ofValue(StatefulTriggerInterface.On.CREATE))
            .interval(Duration.ofSeconds(5))
            .build();

        String file = FriendlyId.createFriendlyId();
        utils().upload("/upload/trigger/on-create/" + file + ".yml");

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(execution.isPresent(), is(true));
    }

    @Test
    void shouldTriggerOnUpdate() throws Exception {
        String file = FriendlyId.createFriendlyId();
        utils().upload("/upload/trigger/on-update/" + file + ".yml");

        var trigger = Trigger.builder()
            .id("sftp-update" + IdUtils.create())
            .type(Trigger.class.getName())
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .action(Property.ofValue(Downloads.Action.NONE))
            .from(Property.ofValue("/upload/trigger/on-update/"))
            .on(Property.ofValue(StatefulTriggerInterface.On.UPDATE))
            .interval(Duration.ofSeconds(5))
            .build();

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        trigger.evaluate(context.getKey(), context.getValue());

        utils().update("/upload/trigger/on-update/" + file + ".yml");

        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(execution.isPresent(), is(true));
    }

    @Test
    void shouldTriggerOnCreateOrUpdate() throws Exception {
        String file = FriendlyId.createFriendlyId();

        var trigger = Trigger.builder()
            .id("sftp-create-or-update" + IdUtils.create())
            .type(Trigger.class.getName())
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .action(Property.ofValue(Downloads.Action.NONE))
            .from(Property.ofValue("/upload/trigger/on-create-or-update/"))
            .on(Property.ofValue(StatefulTriggerInterface.On.CREATE_OR_UPDATE))
            .interval(Duration.ofSeconds(5))
            .build();

        utils().upload("/upload/trigger/on-create-or-update/" + file + ".yml");

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> first = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(first.isPresent(), is(true));

        utils().upload("/upload/trigger/on-create-or-update/" + file + ".txt");

        Optional<Execution> second = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(second.isPresent(), is(true));
    }

    @Test
    void shouldNotTriggerWhenTooManyFiles() throws Exception {
        var trigger = Trigger.builder()
            .id("sftp-too-many-files-" + IdUtils.create())
            .type(Trigger.class.getName())
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue("6622"))
            .username(USERNAME)
            .password(PASSWORD)
            .action(Property.ofValue(Downloads.Action.NONE))
            .from(Property.ofValue("/upload/trigger/too-many-files/"))
            .maxFiles(Property.ofValue(10)) // important
            .interval(Duration.ofSeconds(5))
            .build();

        // upload 26 files > maxFiles (10 as defined above)
        for (int i = 0; i < 26; i++) {
            utils().upload("/upload/trigger/too-many-files/" + FriendlyId.createFriendlyId() + "-" + i + ".yml");
        }

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        // should then skip the execution
        assertThat(execution.isPresent(), is(false));
    }
}
