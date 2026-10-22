package io.kestra.plugin.fs.local;

import com.devskiller.friendly_id.FriendlyId;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.vfs.models.File;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
public abstract class AbstractTriggerTest {
    @Inject
    protected RunContextFactory runContextFactory;

    abstract protected String triggeringFlowId();

    abstract protected LocalUtils utils();

    private void cleanDirectory(String directory) throws Exception {
        Path path = Paths.get(directory);

        if (Files.exists(path)) {
            try (var paths = Files.walk(path)) {
                for (Path entry : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                    Files.delete(entry);
                }
            }
        }

        Files.createDirectories(path);
    }

    @Test
    void moveAction() throws Exception {
        String toUploadDir = "/tmp/local-listen";
        String moveDir = toUploadDir + "-move";

        cleanDirectory(toUploadDir);
        cleanDirectory(moveDir);

        String out1 = FriendlyId.createFriendlyId();
        utils().upload(toUploadDir + "/" + out1);

        String out2 = FriendlyId.createFriendlyId();
        utils().upload(toUploadDir + "/" + out2);

        var trigger = io.kestra.plugin.fs.local.Trigger.builder()
            .id(AbstractTriggerTest.class.getSimpleName())
            .type(io.kestra.plugin.fs.local.Trigger.class.getName())
            .from(Property.ofValue(toUploadDir))
            .action(Property.ofValue(Downloads.Action.MOVE))
            .moveDirectory(Property.ofValue(moveDir))
            .recursive(Property.ofValue(true))
            .build();

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        java.util.List<File> files = (java.util.List<File>) execution.get().getTrigger().getVariables().get("files");
        assertThat(files.size(), is(2));

        assertThat(utils().list(toUploadDir).getFiles().size(), is(0));

        assertThat(utils().list(moveDir).getFiles().size(), is(2));
    }

    @Test
    void noneAction() throws Exception {
        String toUploadDir = "/tmp/local-listen-none-action";

        cleanDirectory(toUploadDir);

        String out1 = FriendlyId.createFriendlyId();
        utils().upload(toUploadDir + "/" + out1);

        String out2 = FriendlyId.createFriendlyId();
        utils().upload(toUploadDir + "/" + out2);

        var trigger = io.kestra.plugin.fs.local.Trigger.builder()
            .id(AbstractTriggerTest.class.getSimpleName())
            .type(io.kestra.plugin.fs.local.Trigger.class.getName())
            .from(Property.ofValue(toUploadDir))
            .action(Property.ofValue(Downloads.Action.NONE))
            .recursive(Property.ofValue(true))
            .build();

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        java.util.List<File> files = (java.util.List<File>) execution.get().getTrigger().getVariables().get("files");
        assertThat(files.size(), is(2));

        assertThat(utils().list(toUploadDir).getFiles().size(), is(2));
    }
}
