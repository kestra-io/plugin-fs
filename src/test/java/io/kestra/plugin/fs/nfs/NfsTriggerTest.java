package io.kestra.plugin.fs.nfs;

import com.devskiller.friendly_id.FriendlyId;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;


class NfsTriggerTest extends AbstractNfsTest {

    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private RunContextFactory runContextFactory;

    @TempDir
    Path tempDirectory;

    private Path watchDirectory;

    @BeforeEach
    void setUp() throws IOException {
        watchDirectory = tempDirectory.resolve(FriendlyId.createFriendlyId());
        Files.createDirectories(watchDirectory);
    }

    @AfterEach
    void tearDown() {
        
    }


    

    @Test
    void trigger_onFileCreate() throws Exception {
        
        TriggerContext triggerContext = TriggerContext.builder()
            .namespace("test.namespace")
            .flowId("test_flow")
            .triggerId("test_trigger")
            .build();

        
        Trigger nfsTrigger = Trigger.builder()
            .id(Trigger.class.getSimpleName())
            .type(Trigger.class.getName())
            .from(Property.ofValue(watchDirectory.toString()))
            .interval(Duration.ofMillis(100))
            .on(Property.ofValue(StatefulTriggerInterface.On.CREATE))
            .build();

        
        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, nfsTrigger);

        
        Optional<Execution> initialExecution = nfsTrigger.evaluate(context.getKey(), triggerContext); // Pass TriggerContext here
        assertThat(initialExecution.isPresent(), is(false));

        
        Path newFile = watchDirectory.resolve("new_file.txt");
        Files.writeString(newFile, "Hello Kestra!");
        
        Thread.sleep(500);

        
        Optional<Execution> execution = nfsTrigger.evaluate(context.getKey(), triggerContext); // Pass TriggerContext here
        assertThat(execution.isPresent(), is(true));

        
        Optional<Execution> repeatedExecution = nfsTrigger.evaluate(context.getKey(), triggerContext); // Pass TriggerContext here
        assertThat(repeatedExecution.isPresent(), is(false));

        
         Files.writeString(newFile, "Updated content");
         
         Thread.sleep(500);
         Optional<Execution> updateExecution = nfsTrigger.evaluate(context.getKey(), triggerContext); // Pass TriggerContext here
         assertThat(updateExecution.isPresent(), is(false));

         Execution triggeredExec = execution.get();
         assertThat(triggeredExec.getTrigger().getVariables().get("files"), is(notNullValue()));
    }


    @Test
    void trigger_onFileUpdate() throws Exception {
        
        TriggerContext triggerContext = TriggerContext.builder()
            .namespace("test.namespace")
            .flowId("test_flow_update")
            .triggerId("test_trigger_update")
            .build();

        
        Trigger nfsTrigger = Trigger.builder()
            .id(Trigger.class.getSimpleName())
            .type(Trigger.class.getName())
            .from(Property.ofValue(watchDirectory.toString()))
            .interval(Duration.ofMillis(100))
            .on(Property.ofValue(StatefulTriggerInterface.On.UPDATE))
            .build();

        
        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> context = TestsUtils.mockTrigger(runContextFactory, nfsTrigger);

        
        Path fileToUpdate = watchDirectory.resolve("update_me.txt");
        Files.writeString(fileToUpdate, "Initial content");
        
        Thread.sleep(500);

        
        Optional<Execution> initialExecution = nfsTrigger.evaluate(context.getKey(), triggerContext); // Pass TriggerContext here
        assertThat(initialExecution.isPresent(), is(false));

        
        
        Thread.sleep(1000);
        Files.writeString(fileToUpdate, "Updated content!");
        Thread.sleep(500);

        
        Optional<Execution> updateExecution = nfsTrigger.evaluate(context.getKey(), triggerContext); // Pass TriggerContext here
        assertThat(updateExecution.isPresent(), is(true));

        
        Optional<Execution> repeatedExecution = nfsTrigger.evaluate(context.getKey(), triggerContext); // Pass TriggerContext here
        assertThat(repeatedExecution.isPresent(), is(false));
    }
}

