package io.kestra.plugin.fs.nfs;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


class NfsTriggerTest extends AbstractNfsTest {

    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private RunContextFactory runContextFactory;

    @TempDir
    Path tempDir; 

    private Path triggerDir;

    @BeforeEach
    void setup() throws IOException {
        
        triggerDir = tempDir.resolve("trigger_input");
        Files.createDirectories(triggerDir);
    }

    @Test
    void nfsTriggerTest() throws Exception {
        // Create the trigger
        Trigger nfsTrigger = Trigger.builder()
            .id("nfsTrigger")
            .type(Trigger.class.getName())
            .from(triggerDir.toUri().getPath())
            .interval(Duration.ofMillis(100))
            .on(Property.ofValue(StatefulTriggerInterface.On.CREATE))
            .build();

    var context = TestsUtils.mockTrigger(runContextFactory, nfsTrigger);

    Optional<Execution> initialExecution = nfsTrigger.evaluate(context.getKey(), context.getValue());
        assertFalse(initialExecution.isPresent(), "Trigger should not fire when directory is empty");

        
        Path testFile = triggerDir.resolve("newfile.txt");
        Files.writeString(testFile, "Hello Kestra");

        
        Optional<Execution> execution = nfsTrigger.evaluate(context.getKey(), context.getValue());
        assertTrue(execution.isPresent(), "Trigger should fire on file creation");

        
        Optional<Execution> repeatedExecution = nfsTrigger.evaluate(context.getKey(), context.getValue());
        assertFalse(repeatedExecution.isPresent(), "Trigger should not fire for the same file twice");

        
        Files.writeString(testFile, "Updated content");
        Optional<Execution> updateExecution = nfsTrigger.evaluate(context.getKey(), context.getValue());
        assertFalse(updateExecution.isPresent(), "Trigger set to ON.CREATE should not fire on update");
    }

    @Test
    void nfsTriggerOnUpdateTest() throws Exception {
        
        Trigger nfsTrigger = Trigger.builder()
            .id("nfsTriggerUpdate")
            .type(Trigger.class.getName())
            .from(triggerDir.toUri().getPath())
            .interval(Duration.ofMillis(100))
            .on(Property.ofValue(StatefulTriggerInterface.On.UPDATE)) // <-- FIX 1
            .build();

    var context = TestsUtils.mockTrigger(runContextFactory, nfsTrigger);

        
        Path testFile = triggerDir.resolve("update-test.txt");
        Files.writeString(testFile, "Initial content");

    Optional<Execution> initialExecution = nfsTrigger.evaluate(context.getKey(), context.getValue());
        assertFalse(initialExecution.isPresent(), "Trigger set to ON.UPDATE should not fire on creation");

    // We must sleep to ensure modification time is different
        Thread.sleep(1000);
        Files.writeString(testFile, "Updated content");

        
        Optional<Execution> updateExecution = nfsTrigger.evaluate(context.getKey(), context.getValue());
        assertTrue(updateExecution.isPresent(), "Trigger should fire on file update");

        
        Optional<Execution> repeatedExecution = nfsTrigger.evaluate(context.getKey(), context.getValue());
        assertFalse(repeatedExecution.isPresent(), "Trigger should not fire again when file is unchanged");
    }
}

