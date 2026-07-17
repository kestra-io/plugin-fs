package io.kestra.plugin.fs;

import com.devskiller.friendly_id.FriendlyId;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.junit.annotations.LoadFlows;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.runners.TestRunnerUtils;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.queues.DispatchQueueInterface;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.Scheduler;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.vfs.Downloads;
import io.kestra.plugin.fs.vfs.models.File;
import jakarta.inject.Inject;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.is;
import static io.kestra.core.tenant.TenantService.MAIN_TENANT;

@KestraTest(startRunner = true, startScheduler = true)
public abstract class AbstractFileTriggerTest {
    @Inject
    private DispatchQueueInterface<Execution> executionQueue;

    @Inject
    protected TestRunnerUtils runnerUtils;

    @Inject
    protected Scheduler scheduler;

    @Inject
    protected RunContextFactory runContextFactory;

    abstract protected String triggeringFlowId();

    abstract protected AbstractUtils utils();

    abstract protected AbstractTrigger createTrigger(String from, Downloads.Action action, String moveDirectory);

    @Test
    @LoadFlows({
        "flows/ftp-listen.yaml",
        "flows/sftp-listen.yaml",
        "flows/smb-listen.yaml"
    })
    void moveAction() throws Exception {
        Awaitility.await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(100)).until(() -> scheduler.isActive());

        AtomicReference<Execution> last = new AtomicReference<>();
        String awaitFlowId = triggeringFlowId();

        String out1 = FriendlyId.createFriendlyId();
        String toUploadDir = "/upload/trigger";
        cleanupRemoteDir(toUploadDir);
        cleanupRemoteDir(toUploadDir + "-move");
        utils().upload(toUploadDir + "/" + out1);
        String out2 = FriendlyId.createFriendlyId();
        utils().upload(toUploadDir + "/" + out2);

        last.set(runnerUtils.awaitFlowExecution(e -> true, MAIN_TENANT, "io.kestra.tests", awaitFlowId, Duration.ofMinutes(1)));
        assertThat(last.get(), notNullValue());

        @SuppressWarnings("unchecked")
        java.util.List<File> trigger = (java.util.List<File>) last.get().getTrigger().getVariables().get("files");
        assertThat(trigger.size(), is(2));

        assertThat(utils().list(toUploadDir).getFiles().isEmpty(), is(true));
        assertThat(utils().list(toUploadDir + "-move").getFiles().size(), is(2));

        utils().delete(toUploadDir + "/" + out1);
        utils().delete(toUploadDir + "/" + out2);
    }

    @Test
    void deleteActionRefiresSamePath() throws Exception {
        // Regression: with action DELETE the processed file is removed from the watched directory,
        // so a file re-appearing at the SAME path is genuinely new and must fire on every poll.
        // The stateful trigger previously remembered the path and silently suppressed re-uploads.
        String toUploadDir = "/upload/trigger-delete-refire";
        cleanupRemoteDir(toUploadDir);

        // Fixed filename so the same remote path is reused across polls (the customer's case).
        String fileName = "recurring-file";

        var trigger = createTrigger(toUploadDir + "/", Downloads.Action.DELETE, null);
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        var polling = (PollingTriggerInterface) trigger;

        // First arrival -> fires and deletes the file.
        utils().upload(toUploadDir + "/" + fileName);
        Optional<Execution> first = polling.evaluate(context.getKey(), context.getValue().context());
        assertThat(first.isPresent(), is(true));
        assertThat(utils().list(toUploadDir).getFiles().isEmpty(), is(true));

        // Same file uploaded again at the same path -> must fire again (state must not suppress it).
        utils().upload(toUploadDir + "/" + fileName);
        Optional<Execution> second = polling.evaluate(context.getKey(), context.getValue().context());
        assertThat(second.isPresent(), is(true));

        @SuppressWarnings("unchecked")
        java.util.List<File> files = (java.util.List<File>) second.get().getTrigger().getVariables().get("files");
        assertThat(files.size(), is(1));
        assertThat(utils().list(toUploadDir).getFiles().isEmpty(), is(true));
    }

    @Test
    @LoadFlows({
        "flows/ftp-listen-none-action.yaml",
        "flows/sftp-listen-none-action.yaml",
        "flows/smb-listen-none-action.yaml"
    })
    void noneAction() throws Exception {
        Awaitility.await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(100)).until(() -> scheduler.isActive());

        AtomicReference<Execution> last = new AtomicReference<>();
        String awaitFlowId = triggeringFlowId() + "-none-action";

        String out1 = FriendlyId.createFriendlyId();
        String toUploadDir = "/upload/trigger-none";
        cleanupRemoteDir(toUploadDir);
        utils().upload(toUploadDir + "/" + out1);
        String out2 = FriendlyId.createFriendlyId();
        utils().upload(toUploadDir + "/" + out2);

        last.set(runnerUtils.awaitFlowExecution(e -> true, MAIN_TENANT, "io.kestra.tests", awaitFlowId, Duration.ofMinutes(1)));
        assertThat(last.get(), notNullValue());

        @SuppressWarnings("unchecked")
        java.util.List<File> trigger = (java.util.List<File>) last.get().getTrigger().getVariables().get("files");
        assertThat(trigger.size(), is(2));

        assertThat(utils().list(toUploadDir).getFiles().size(), is(2));

        utils().delete(toUploadDir + "/" + out1);
        utils().delete(toUploadDir + "/" + out2);
    }

    @Test
    @LoadFlows({
        "flows/ftp-listen-missing.yaml",
        "flows/sftp-listen-missing.yaml",
        "flows/smb-listen-missing.yaml"
    })
    void missing() throws Exception {
        Awaitility.await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(100)).until(() -> scheduler.isActive());

        AtomicReference<Execution> last = new AtomicReference<>();
        String awaitFlowId = triggeringFlowId() + "-missing";

        String file = FriendlyId.createFriendlyId();
        cleanupRemoteDir("/upload/trigger-missing");
        utils().upload("/upload/trigger-missing/" + file);

        last.set(runnerUtils.awaitFlowExecution(e -> true, MAIN_TENANT, "io.kestra.tests", awaitFlowId, Duration.ofMinutes(1)));
        assertThat(last.get(), notNullValue());

        @SuppressWarnings("unchecked")
        java.util.List<URI> trigger = (java.util.List<URI>) last.get().getTrigger().getVariables().get("files");

        assertThat(trigger.size(), is(1));

        utils().delete("/upload/trigger-missing/" + file);
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
