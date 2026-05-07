package io.kestra.plugin.fs;

import com.devskiller.friendly_id.FriendlyId;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.junit.annotations.LoadFlows;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.DispatchQueueInterface;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.Scheduler;
import io.kestra.plugin.fs.vfs.models.File;
import jakarta.inject.Inject;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest(startRunner = true, startScheduler = true)
public abstract class AbstractFileTriggerTest {
    @Inject
    private DispatchQueueInterface<Execution> executionQueue;

    @Inject
    protected Scheduler scheduler;

    @Inject
    protected RunContextFactory runContextFactory;

    abstract protected String triggeringFlowId();

    abstract protected AbstractUtils utils();

    @Test
    @LoadFlows({
        "flows/ftp-listen.yaml",
        "flows/sftp-listen.yaml",
        "flows/smb-listen.yaml"
    })
    void moveAction() throws Exception {
        Awaitility.await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(100)).until(() -> scheduler.isActive());

        CountDownLatch queueCount = new CountDownLatch(1);
        AtomicReference<Execution> last = new AtomicReference<>();

        executionQueue.addListener(execution -> {
            if (execution.getFlowId().equals(triggeringFlowId())) {
                last.set(execution);
                queueCount.countDown();
            }
        });

        String out1 = FriendlyId.createFriendlyId();
        String toUploadDir = "/upload/trigger";
        cleanupRemoteDir(toUploadDir);
        cleanupRemoteDir(toUploadDir + "-move");
        utils().upload(toUploadDir + "/" + out1);
        String out2 = FriendlyId.createFriendlyId();
        utils().upload(toUploadDir + "/" + out2);

        boolean await = queueCount.await(1, TimeUnit.MINUTES);
        assertThat(await, is(true));

        @SuppressWarnings("unchecked")
        java.util.List<File> trigger = (java.util.List<File>) last.get().getTrigger().getVariables().get("files");
        assertThat(trigger.size(), is(2));

        assertThat(utils().list(toUploadDir).getFiles().isEmpty(), is(true));
        assertThat(utils().list(toUploadDir + "-move").getFiles().size(), is(2));

        utils().delete(toUploadDir + "/" + out1);
        utils().delete(toUploadDir + "/" + out2);
    }

    @Test
    @LoadFlows({
        "flows/ftp-listen-none-action.yaml",
        "flows/sftp-listen-none-action.yaml",
        "flows/smb-listen-none-action.yaml"
    })
    void noneAction() throws Exception {
        Awaitility.await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(100)).until(() -> scheduler.isActive());

        CountDownLatch queueCount = new CountDownLatch(1);
        AtomicReference<Execution> last = new AtomicReference<>();

        executionQueue.addListener(execution -> {
            if (execution.getFlowId().equals(triggeringFlowId() + "-none-action")) {
                last.set(execution);
                queueCount.countDown();
            }
        });

        String out1 = FriendlyId.createFriendlyId();
        String toUploadDir = "/upload/trigger-none";
        cleanupRemoteDir(toUploadDir);
        utils().upload(toUploadDir + "/" + out1);
        String out2 = FriendlyId.createFriendlyId();
        utils().upload(toUploadDir + "/" + out2);

        boolean await = queueCount.await(1, TimeUnit.MINUTES);
        assertThat(await, is(true));

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

        CountDownLatch queueCount = new CountDownLatch(1);
        AtomicReference<Execution> last = new AtomicReference<>();

        executionQueue.addListener(execution -> {
            if (execution.getFlowId().equals(triggeringFlowId() + "-missing")) {
                last.set(execution);
                queueCount.countDown();
            }
        });

        String file = FriendlyId.createFriendlyId();
        cleanupRemoteDir("/upload/trigger-missing");
        utils().upload("/upload/trigger-missing/" + file);

        boolean await = queueCount.await(1, TimeUnit.MINUTES);
        assertThat(await, is(true));

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
