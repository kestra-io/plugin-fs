package io.kestra.plugin.fs.local;

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

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest(startRunner = true, startScheduler = true)
public abstract class AbstractTriggerTest {
    @Inject
    private DispatchQueueInterface<Execution> executionQueue;

    @Inject
    protected Scheduler scheduler;

    @Inject
    protected RunContextFactory runContextFactory;

    abstract protected String triggeringFlowId();

    abstract protected LocalUtils utils();

    @Test
    @LoadFlows({"flows/local-listen.yaml"})
    void moveAction() throws Exception {
        Awaitility.await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(100)).until(() -> scheduler.isActive());

        String toUploadDir = "/tmp/local-listen";
        Files.createDirectories(Paths.get(toUploadDir));

        CountDownLatch queueCount = new CountDownLatch(1);
        AtomicReference<Execution> last = new AtomicReference<>();

        executionQueue.addListener(execution -> {
            if (execution.getFlowId().equals(triggeringFlowId())) {
                last.set(execution);
                queueCount.countDown();
            }
        });

        String out1 = FriendlyId.createFriendlyId();
        utils().upload(toUploadDir + "/" + out1);

        String out2 = FriendlyId.createFriendlyId();
        utils().upload(toUploadDir + "/" + out2);

        boolean await = queueCount.await(1, TimeUnit.MINUTES);
        assertThat(await, is(true));

        @SuppressWarnings("unchecked")
        java.util.List<File> trigger = (java.util.List<File>) last.get().getTrigger().getVariables().get("files");
        assertThat(trigger.size(), greaterThanOrEqualTo(2));

        assertThat(utils().list(toUploadDir).getFiles().size(), is(0));

        assertThat(utils().list(toUploadDir + "-move").getFiles().size(), greaterThanOrEqualTo(2));
        utils().delete(toUploadDir + "-move");
    }

    @Test
    @LoadFlows({"flows/local-listen-none-action.yaml"})
    void noneAction() throws Exception {
        Awaitility.await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(100)).until(() -> scheduler.isActive());

        String toUploadDir = "/tmp/local-listen-none-action";
        Files.createDirectories(Paths.get(toUploadDir));

        CountDownLatch queueCount = new CountDownLatch(1);
        AtomicReference<Execution> last = new AtomicReference<>();

        executionQueue.addListener(execution -> {
            if (execution.getFlowId().equals(triggeringFlowId() + "-none-action")) {
                last.set(execution);
                queueCount.countDown();
            }
        });

        String out1 = FriendlyId.createFriendlyId();
        utils().upload(toUploadDir + "/" + out1);

        String out2 = FriendlyId.createFriendlyId();
        utils().upload(toUploadDir + "/" + out2);

        boolean await = queueCount.await(1, TimeUnit.MINUTES);
        assertThat(await, is(true));

        @SuppressWarnings("unchecked")
        java.util.List<File> trigger = (java.util.List<File>) last.get().getTrigger().getVariables().get("files");
        assertThat(trigger.size(), greaterThanOrEqualTo(2));

        assertThat(utils().list(toUploadDir).getFiles().size(), greaterThanOrEqualTo(2));

        utils().delete(toUploadDir);
    }
}
