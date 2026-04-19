package io.kestra.plugin.fs;

import com.devskiller.friendly_id.FriendlyId;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.vfs.Downloads;
import io.kestra.plugin.fs.vfs.models.File;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
public abstract class AbstractFileTriggerTest {
    @Inject
    protected RunContextFactory runContextFactory;

    abstract protected String triggeringFlowId();

    abstract protected AbstractUtils utils();

    abstract protected io.kestra.core.models.triggers.AbstractTrigger createTrigger(String from, Downloads.Action action, String moveDirectory);

    @Test
    void moveAction() throws Exception {
        String toUploadDir = "/upload/trigger";
        cleanupRemoteDir(toUploadDir);
        cleanupRemoteDir(toUploadDir + "-move");

        String out1 = FriendlyId.createFriendlyId();
        utils().upload(toUploadDir + "/" + out1);
        String out2 = FriendlyId.createFriendlyId();
        utils().upload(toUploadDir + "/" + out2);

        var trigger = createTrigger(toUploadDir + "/", Downloads.Action.MOVE, toUploadDir + "-move/");
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = ((io.kestra.core.models.triggers.PollingTriggerInterface) trigger).evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        java.util.List<File> files = (java.util.List<File>) execution.get().getTrigger().getVariables().get("files");
        assertThat(files.size(), is(2));

        assertThat(utils().list(toUploadDir).getFiles().isEmpty(), is(true));
        assertThat(utils().list(toUploadDir + "-move").getFiles().size(), is(2));

        utils().delete(toUploadDir + "-move/" + out1);
        utils().delete(toUploadDir + "-move/" + out2);
    }

    @Test
    void noneAction() throws Exception {
        String toUploadDir = "/upload/trigger-none";
        cleanupRemoteDir(toUploadDir);

        String out1 = FriendlyId.createFriendlyId();
        utils().upload(toUploadDir + "/" + out1);
        String out2 = FriendlyId.createFriendlyId();
        utils().upload(toUploadDir + "/" + out2);

        var trigger = createTrigger(toUploadDir + "/", Downloads.Action.NONE, null);
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = ((io.kestra.core.models.triggers.PollingTriggerInterface) trigger).evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        java.util.List<File> files = (java.util.List<File>) execution.get().getTrigger().getVariables().get("files");
        assertThat(files.size(), is(2));

        assertThat(utils().list(toUploadDir).getFiles().size(), is(2));

        utils().delete(toUploadDir + "/" + out1);
        utils().delete(toUploadDir + "/" + out2);
    }

    @Test
    void missing() throws Exception {
        cleanupRemoteDir("/upload/trigger-missing");

        String file = FriendlyId.createFriendlyId();
        utils().upload("/upload/trigger-missing/" + file);

        var trigger = createTrigger("/upload/trigger-missing/", Downloads.Action.MOVE, "/upload/trigger-move-missing/");
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        Optional<Execution> execution = ((io.kestra.core.models.triggers.PollingTriggerInterface) trigger).evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        java.util.List<URI> files = (java.util.List<URI>) execution.get().getTrigger().getVariables().get("files");
        assertThat(files.size(), is(1));

        utils().delete("/upload/trigger-move-missing/" + file);
    }

    private void cleanupRemoteDir(String dir) {
        try {
            var list = utils().list(dir);
            for (File file : list.getFiles()) {
                String deletePath = file.getServerPath() != null ?
                    file.getServerPath().getPath() :
                    file.getPath().toString();
                utils().delete(deletePath);
            }
        } catch (Exception ignored) {
        }
    }
}
