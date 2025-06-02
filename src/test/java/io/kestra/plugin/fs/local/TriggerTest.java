package io.kestra.plugin.fs.local;

import com.devskiller.friendly_id.FriendlyId;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.vfs.models.File;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;


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
            .id(TriggerTest.class.getSimpleName())
            .type(io.kestra.plugin.fs.local.Trigger.class.getName())
            .from(Property.ofValue("/tmp/trigger/"))
            .action(Property.ofValue(Downloads.Action.MOVE))
            .moveDirectory(Property.ofValue("/tmp/trigger-move/"))
            .recursive(Property.ofValue(true))
            .build();

        String out = FriendlyId.createFriendlyId();
        Upload.Output upload = utils().upload("/tmp/trigger/" + out + ".yml");

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        java.util.List<File> urls = (java.util.List<File>) execution.get().getTrigger().getVariables().get("files");
        assertThat(urls.size(), greaterThanOrEqualTo(1));

        assertThrows(java.lang.IllegalArgumentException.class, () -> {
            io.kestra.plugin.fs.local.Download task = io.kestra.plugin.fs.local.Download.builder()
                .id(TriggerTest.class.getSimpleName())
                .type(io.kestra.plugin.fs.local.Download.class.getName())
                .from(Property.ofValue(upload.getUri().getPath()))
                .build();

            task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
        });

        Download task = Download.builder()
            .id(TriggerTest.class.getSimpleName())
            .type(Download.class.getName())
            .from(Property.ofValue("/tmp/trigger-move/" + out + ".yml"))
            .build();

        io.kestra.plugin.fs.local.Download.Output run = task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
        assertThat(run.getUri().toString(), containsString("kestra://"));

        utils().delete("/tmp/trigger-move/");
    }
}
