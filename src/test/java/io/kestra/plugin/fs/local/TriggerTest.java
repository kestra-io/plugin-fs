package io.kestra.plugin.fs.local;

import com.devskiller.friendly_id.FriendlyId;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.sftp.Download;
import io.kestra.plugin.fs.sftp.Trigger;
import io.kestra.plugin.fs.vfs.models.File;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class TriggerTest extends AbstractTriggerTest {
    @Inject
    private LocalUtils localUtils;

    @Override
    protected String triggeringFlowId() {
        return "local-listen";
    }

    @Override
    protected LocalUtils utils() {
        return localUtils;
    }

    @Test
    void move() throws Exception {
        Path path = Paths.get("/tmp/trigger");
        Files.createDirectories(path);

        io.kestra.plugin.fs.local.Trigger trigger = io.kestra.plugin.fs.local.Trigger.builder()
            .id(AbstractTriggerTest.class.getSimpleName())
            .type(Trigger.class.getName())
            .from(Property.of("/tmp/trigger/"))
            .action(Property.of(Downloads.Action.MOVE))
            .allowedPaths(Property.of(List.of("/")))
            .moveDirectory(Property.of("/tmp/trigger-move/"))
            .recursive(Property.of(true))
            .build();

        String out = FriendlyId.createFriendlyId();
        Upload.Output upload = utils().upload("/tmp/trigger/" + out + ".yml");

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        java.util.List<File> urls = (java.util.List<File>) execution.get().getTrigger().getVariables().get("files");
        assertThat(urls.size(), is(1));

        assertThrows(java.lang.IllegalArgumentException.class, () -> {
            io.kestra.plugin.fs.local.Download task = io.kestra.plugin.fs.local.Download.builder()
                .id(AbstractTriggerTest.class.getSimpleName())
                .type(io.kestra.plugin.fs.local.Download.class.getName())
                .from(Property.of(upload.getUri().toString()))
                .build();

            task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
        });

        io.kestra.plugin.fs.local.Download task = io.kestra.plugin.fs.local.Download.builder()
            .id(AbstractTriggerTest.class.getSimpleName())
            .type(Download.class.getName())
            .from(Property.of("/tmp/trigger-move/" + out + ".yml"))
            .build();

        io.kestra.plugin.fs.local.Download.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
        assertThat(run.getUri().toString(), containsString("kestra://"));

        utils().delete("/tmp/trigger-move/");
    }
}
