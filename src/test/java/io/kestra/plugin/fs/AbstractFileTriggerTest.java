package io.kestra.plugin.fs;

import com.devskiller.friendly_id.FriendlyId;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.services.FlowListenersInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.jdbc.runner.JdbcScheduler;
import io.kestra.plugin.fs.vfs.models.File;
import io.kestra.scheduler.AbstractScheduler;
import io.kestra.worker.DefaultWorker;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.net.URI;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
public abstract class AbstractFileTriggerTest {
    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private FlowListenersInterface flowListenersService;

    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    private QueueInterface<Execution> executionQueue;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @Inject
    protected RunContextFactory runContextFactory;

    abstract protected String triggeringFlowId();

    abstract protected AbstractUtils utils();

    @Test
    void moveAction() throws Exception {
        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        try (
            AbstractScheduler scheduler = new JdbcScheduler(
                this.applicationContext,
                this.flowListenersService
            );
            DefaultWorker worker = applicationContext.createBean(DefaultWorker.class, IdUtils.create(), 8, null)
        ) {
            AtomicReference<Execution> last = new AtomicReference<>();

            // wait for execution
            Flux<Execution> receive = TestsUtils.receive(executionQueue, execution -> {
                if (execution.getLeft().getFlowId().equals(triggeringFlowId())) {
                    last.set(execution.getLeft());

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

            worker.run();
            scheduler.run();
            repositoryLoader.load(Objects.requireNonNull(AbstractFileTriggerTest.class.getClassLoader().getResource("flows")));

            boolean await = queueCount.await(10, TimeUnit.SECONDS);
            assertThat(await, is(true));
            receive.blockLast();

            @SuppressWarnings("unchecked")
            java.util.List<File> trigger = (java.util.List<File>) last.get().getTrigger().getVariables().get("files");
            assertThat(trigger.size(), is(2));

            assertThat(utils().list(toUploadDir).getFiles().isEmpty(), is(true));
            assertThat(utils().list(toUploadDir + "-move").getFiles().size(), is(2));

            utils().delete(toUploadDir + "/" + out1);
            utils().delete(toUploadDir + "/" + out2);
        }
    }

    @Test
    void noneAction() throws Exception {
        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        try (
            AbstractScheduler scheduler = new JdbcScheduler(
                this.applicationContext,
                this.flowListenersService
            );
            DefaultWorker worker = applicationContext.createBean(DefaultWorker.class, IdUtils.create(), 8, null)
        ) {
            AtomicReference<Execution> last = new AtomicReference<>();

            // wait for execution
            Flux<Execution> receive = TestsUtils.receive(executionQueue, execution -> {
                if (execution.getLeft().getFlowId().equals(triggeringFlowId() + "-none-action")) {
                    last.set(execution.getLeft());

                    queueCount.countDown();
                }
            });

            String out1 = FriendlyId.createFriendlyId();
            String toUploadDir = "/upload/trigger-none";
            cleanupRemoteDir(toUploadDir);
            utils().upload(toUploadDir + "/" + out1);
            String out2 = FriendlyId.createFriendlyId();
            utils().upload(toUploadDir + "/" + out2);

            worker.run();
            scheduler.run();
            repositoryLoader.load(Objects.requireNonNull(AbstractFileTriggerTest.class.getClassLoader().getResource("flows")));

            boolean await = queueCount.await(10, TimeUnit.SECONDS);
            assertThat(await, is(true));
            receive.blockLast();

            @SuppressWarnings("unchecked")
            java.util.List<File> trigger = (java.util.List<File>) last.get().getTrigger().getVariables().get("files");
            assertThat(trigger.size(), is(2));

            assertThat(utils().list(toUploadDir).getFiles().size(), is(2));

            utils().delete(toUploadDir + "/" + out1);
            utils().delete(toUploadDir + "/" + out2);
        }
    }

    @Test
    void missing() throws Exception {
        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);

        AtomicReference<Execution> last = new AtomicReference<>();
        // scheduler
        try (
            AbstractScheduler scheduler = new JdbcScheduler(
                this.applicationContext,
                this.flowListenersService
            );
            DefaultWorker worker = applicationContext.createBean(DefaultWorker.class, IdUtils.create(), 8, null)
        ) {
            // wait for execution
            Flux<Execution> receive = TestsUtils.receive(executionQueue, execution -> {
                if (execution.getLeft().getFlowId().equals(triggeringFlowId() + "-missing")) {
                    last.set(execution.getLeft());

                    queueCount.countDown();
                }
            });

            String file = FriendlyId.createFriendlyId();
            cleanupRemoteDir("/upload/trigger-missing");
            utils().upload("/upload/trigger-missing/" + file);

            worker.run();
            scheduler.run();
            repositoryLoader.load(Objects.requireNonNull(AbstractFileTriggerTest.class.getClassLoader().getResource("flows")));

            boolean await = queueCount.await(10, TimeUnit.SECONDS);
            assertThat(await, is(true));
            receive.blockLast();

            @SuppressWarnings("unchecked")
            java.util.List<URI> trigger = (java.util.List<URI>) last.get().getTrigger().getVariables().get("files");

            assertThat(trigger.size(), is(1));

            utils().delete("/upload/trigger-missing/" + file);
        }
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
