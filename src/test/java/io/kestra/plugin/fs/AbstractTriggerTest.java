package io.kestra.plugin.fs;

import com.devskiller.friendly_id.FriendlyId;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.schedulers.AbstractScheduler;
import io.kestra.core.schedulers.DefaultScheduler;
import io.kestra.core.schedulers.SchedulerExecutionStateInterface;
import io.kestra.core.schedulers.SchedulerTriggerStateInterface;
import io.kestra.core.services.FlowListenersInterface;
import io.kestra.plugin.fs.vfs.Upload;
import io.kestra.plugin.fs.vfs.models.File;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

@MicronautTest
public abstract class AbstractTriggerTest {
    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private SchedulerTriggerStateInterface triggerState;

    @Inject
    private SchedulerExecutionStateInterface executionState;

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

    @BeforeEach
    private void init() throws IOException, URISyntaxException {
        repositoryLoader.load(Objects.requireNonNull(AbstractTriggerTest.class.getClassLoader().getResource("flows")));
    }

    abstract public Upload.Output upload(String to) throws Exception;

    @Test
    void flow() throws Exception {
        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        try (AbstractScheduler scheduler = new DefaultScheduler(
            this.applicationContext,
            this.flowListenersService,
            this.executionState,
            this.triggerState
        )) {
            AtomicReference<Execution> last = new AtomicReference<>();

            // wait for execution
            executionQueue.receive(AbstractTriggerTest.class, execution -> {
                last.set(execution);

                queueCount.countDown();
                assertThat(execution.getFlowId(), containsString("ftp-listen"));
            });


            String out1 = FriendlyId.createFriendlyId();
            upload("/upload/" + random + "/" + out1);
            String out2 = FriendlyId.createFriendlyId();
            upload("/upload/" + random + "/" + out2);

            scheduler.run();

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
        try (AbstractScheduler scheduler = new DefaultScheduler(
            this.applicationContext,
            this.flowListenersService,
            this.executionState,
            this.triggerState
        )) {
            AtomicReference<Execution> last = new AtomicReference<>();

            // wait for execution
            executionQueue.receive(AbstractTriggerTest.class, execution -> {
                last.set(execution);

                queueCount.countDown();
                assertThat(execution.getFlowId(), is("sftp-listen"));
            });


            scheduler.run();

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
