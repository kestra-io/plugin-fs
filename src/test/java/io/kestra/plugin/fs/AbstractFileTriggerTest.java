package io.kestra.plugin.fs;

import com.devskiller.friendly_id.FriendlyId;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.Worker;
import io.kestra.core.schedulers.AbstractScheduler;
import io.kestra.core.schedulers.DefaultScheduler;
import io.kestra.core.schedulers.SchedulerTriggerStateInterface;
import io.kestra.core.services.FlowListenersInterface;
import io.kestra.plugin.fs.vfs.List;
import io.kestra.plugin.fs.vfs.Upload;
import io.kestra.plugin.fs.vfs.models.File;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

// FIXME Remove once Worker closing has been reworked (Micronaut 4 PR)
//  We need to rebuild the context for each tests as currently Workers can't be closed properly (they keep listening to queues they shouldn't)
@MicronautTest(rebuildContext = true)
public abstract class AbstractFileTriggerTest {
    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private SchedulerTriggerStateInterface triggerState;


    @Inject
    private FlowListenersInterface flowListenersService;

    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    private QueueInterface<Execution> executionQueue;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;

    @Inject
    protected RunContextFactory runContextFactory;

    @Value("${kestra.variables.globals.random}")
    protected String random;

    abstract protected String triggeringFlowId();

    abstract protected AbstractUtils utils();

    @Test
    void moveAction() throws Exception {
        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        Worker worker = new Worker(applicationContext, 8, null);
        try (
            AbstractScheduler scheduler = new DefaultScheduler(
            this.applicationContext,
            this.flowListenersService,
            this.triggerState
        );
        ) {
            AtomicReference<Execution> last = new AtomicReference<>();

            // wait for execution
            executionQueue.receive(AbstractFileTriggerTest.class, execution -> {
                if (execution.getLeft().getFlowId().equals(triggeringFlowId())){
                    last.set(execution.getLeft());

                    queueCount.countDown();
                }
            });


            String out1 = FriendlyId.createFriendlyId();
            String toUploadDir = "/upload/" + random;
            utils().upload(toUploadDir + "/" + out1);
            String out2 = FriendlyId.createFriendlyId();
            utils().upload(toUploadDir + "/" + out2);

            worker.run();
            scheduler.run();
            repositoryLoader.load(Objects.requireNonNull(AbstractFileTriggerTest.class.getClassLoader().getResource("flows")));

            boolean await = queueCount.await(10, TimeUnit.SECONDS);
            assertThat(await, is(true));

            @SuppressWarnings("unchecked")
            java.util.List<File> trigger = (java.util.List<File>) last.get().getTrigger().getVariables().get("files");
            assertThat(trigger.size(), is(2));

            assertThat(utils().list(toUploadDir).getFiles().isEmpty(), is(true));
            assertThat(utils().list(toUploadDir + "-move").getFiles().size(), is(2));
        }
    }

    @Test
    void noneAction() throws Exception {
        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        Worker worker = new Worker(applicationContext, 8, null);
        try (
            AbstractScheduler scheduler = new DefaultScheduler(
            this.applicationContext,
            this.flowListenersService,
            this.triggerState
        );
        ) {
            AtomicReference<Execution> last = new AtomicReference<>();

            // wait for execution
            executionQueue.receive(AbstractFileTriggerTest.class, execution -> {
                if (execution.getLeft().getFlowId().equals(triggeringFlowId() + "-none-action")){
                    last.set(execution.getLeft());

                    queueCount.countDown();
                }
            });


            String out1 = FriendlyId.createFriendlyId();
            String toUploadDir = "/upload/" + random + "-none";
            utils().upload(toUploadDir + "/" + out1);
            String out2 = FriendlyId.createFriendlyId();
            utils().upload(toUploadDir + "/" + out2);

            worker.run();
            scheduler.run();
            repositoryLoader.load(Objects.requireNonNull(AbstractFileTriggerTest.class.getClassLoader().getResource("flows")));

            boolean await = queueCount.await(10, TimeUnit.SECONDS);
            assertThat(await, is(true));

            @SuppressWarnings("unchecked")
            java.util.List<File> trigger = (java.util.List<File>) last.get().getTrigger().getVariables().get("files");
            assertThat(trigger.size(), is(2));

            assertThat(utils().list(toUploadDir).getFiles().size(), is(2));
        }
    }

    @Test
    @Disabled("don't work on github action")
    void missing() throws Exception {
        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        try (
            AbstractScheduler scheduler = new DefaultScheduler(
                this.applicationContext,
                this.flowListenersService,
                this.triggerState
            );
            Worker worker = new Worker(applicationContext, 8, null);
        ) {
            AtomicReference<Execution> last = new AtomicReference<>();

            // wait for execution
            executionQueue.receive(AbstractFileTriggerTest.class, execution -> {
                last.set(execution.getLeft());

                queueCount.countDown();
                assertThat(execution.getLeft().getFlowId(), is("sftp-listen"));
            });


            worker.run();
            scheduler.run();
            repositoryLoader.load(Objects.requireNonNull(AbstractFileTriggerTest.class.getClassLoader().getResource("flows")));

            Thread.sleep(1000);

            String out1 = FriendlyId.createFriendlyId();
            utils().upload("/upload/" + random + "/" + out1);

            queueCount.await(10, TimeUnit.SECONDS);

            @SuppressWarnings("unchecked")
            java.util.List<URI> trigger = (java.util.List<URI>) last.get().getTrigger().getVariables().get("files");

            assertThat(trigger.size(), is(1));
        }
    }
}
