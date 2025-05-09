package io.kestra.plugin.fs.local;

import com.devskiller.friendly_id.FriendlyId;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.Worker;
import io.kestra.core.schedulers.AbstractScheduler;
import io.kestra.core.services.FlowListenersInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.jdbc.runner.JdbcScheduler;
import io.kestra.plugin.fs.vfs.models.File;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest(rebuildContext = true)
public abstract class AbstractTriggerTest {
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

    abstract protected LocalUtils utils();

    @Test
    void moveAction() throws Exception {
        String toUploadDir = "/var/tmp/local-listen";
        Files.createDirectories(Paths.get(toUploadDir));

        String out1 = FriendlyId.createFriendlyId();
        utils().upload(toUploadDir + "/" + out1);

        String out2 = FriendlyId.createFriendlyId();
        utils().upload(toUploadDir + "/" + out2);

        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        try (
            AbstractScheduler scheduler = new JdbcScheduler(
                this.applicationContext,
                this.flowListenersService
            );
            Worker worker = applicationContext.createBean(Worker.class, IdUtils.create(), 8, null)
        ) {
            AtomicReference<Execution> last = new AtomicReference<>();

            // wait for execution
            Flux<Execution> receive = TestsUtils.receive(executionQueue, execution -> {
                if (execution.getLeft().getFlowId().equals(triggeringFlowId())) {
                    last.set(execution.getLeft());

                    queueCount.countDown();
                }
            });

            worker.run();
            scheduler.run();
            repositoryLoader.load(Objects.requireNonNull(io.kestra.plugin.fs.local.AbstractTriggerTest.class.getClassLoader().getResource("flows")));

            boolean await = queueCount.await(10, TimeUnit.SECONDS);
            assertThat(await, is(true));
            receive.blockLast();

            @SuppressWarnings("unchecked")
            java.util.List<File> trigger = (java.util.List<File>) last.get().getTrigger().getVariables().get("files");
            assertThat(trigger.size(), greaterThanOrEqualTo(2));

            assertThrows(java.nio.file.NoSuchFileException.class, () -> utils().list(toUploadDir));
            assertThat(utils().list(toUploadDir + "-move").getFiles().size(), greaterThanOrEqualTo(2));

            utils().delete(toUploadDir + "-move");
        }
    }

    @Test
    void noneAction() throws Exception {
        String toUploadDir = "/var/tmp/local-listen-none-action";
        Files.createDirectories(Paths.get(toUploadDir));

        String out1 = FriendlyId.createFriendlyId();
        utils().upload(toUploadDir + "/" + out1);

        String out2 = FriendlyId.createFriendlyId();
        utils().upload(toUploadDir + "/" + out2);

        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        try (
            AbstractScheduler scheduler = new JdbcScheduler(
                this.applicationContext,
                this.flowListenersService
            );
            Worker worker = applicationContext.createBean(Worker.class, IdUtils.create(), 8, null)
        ) {
            AtomicReference<Execution> last = new AtomicReference<>();

            // wait for execution
            Flux<Execution> receive = TestsUtils.receive(
                executionQueue,
                execution -> {
                    if (execution.getLeft().getFlowId().equals(triggeringFlowId() + "-none-action")) {
                        last.set(execution.getLeft());
                        queueCount.countDown();
                    }
                }
            );

            worker.run();
            scheduler.run();
            repositoryLoader.load(Objects.requireNonNull(io.kestra.plugin.fs.local.AbstractTriggerTest.class.getClassLoader().getResource("flows")));

            boolean await = queueCount.await(10, TimeUnit.SECONDS);
            assertThat(await, is(true));
            receive.blockLast();

            @SuppressWarnings("unchecked")
            java.util.List<File> trigger = (java.util.List<File>) last.get().getTrigger().getVariables().get("files");
            assertThat(trigger.size(), greaterThanOrEqualTo(2));

            assertThat(utils().list(toUploadDir).getFiles().size(), greaterThanOrEqualTo(2));

            utils().delete(toUploadDir);
        }
    }
}

