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

@MicronautTest
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

    abstract public Upload.Output upload(String to) throws Exception;

    abstract protected String triggeringFlowId();

    @Test
    void flow() throws Exception {
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
            upload("/upload/" + random + "/" + out1);
            String out2 = FriendlyId.createFriendlyId();
            upload("/upload/" + random + "/" + out2);

            worker.run();
            scheduler.run();
            repositoryLoader.load(Objects.requireNonNull(AbstractFileTriggerTest.class.getClassLoader().getResource("flows")));

            queueCount.await(1, TimeUnit.MINUTES);

            @SuppressWarnings("unchecked")
            java.util.List<File> trigger = (java.util.List<File>) last.get().getTrigger().getVariables().get("files");
            assertThat(trigger.size(), is(2));
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
            upload("/upload/" + random + "/" + out1);

            queueCount.await(1, TimeUnit.MINUTES);

            @SuppressWarnings("unchecked")
            java.util.List<URI> trigger = (java.util.List<URI>) last.get().getTrigger().getVariables().get("files");

            assertThat(trigger.size(), is(1));
        }
    }
}
