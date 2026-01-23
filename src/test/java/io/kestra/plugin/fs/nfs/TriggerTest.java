package io.kestra.plugin.fs.nfs;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.models.triggers.Trigger;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class TriggerTest {

    @Inject
    private RunContextFactory runContextFactory;

    private static final ObjectMapper objectMapper = JacksonMapper.ofJson();

    @TempDir
    private Path tempDirectory;
    private Path nfsMountPoint;

    @BeforeEach
    void setUpNfsSim() throws IOException {
        nfsMountPoint = tempDirectory.resolve("nfs_trigger_share");
        Files.createDirectories(nfsMountPoint);
    }

    @Test
    void trigger_onFileCreate() throws Exception {
        io.kestra.plugin.fs.nfs.Trigger nfsTrigger = io.kestra.plugin.fs.nfs.Trigger.builder()
            .id(IdUtils.create())
            .type(io.kestra.plugin.fs.nfs.Trigger.class.getName())
            .from(Property.ofValue(nfsMountPoint.toString()))
            .on(Property.ofValue(StatefulTriggerInterface.On.CREATE))
            .build();

        Map.Entry<ConditionContext, Trigger> context = TestsUtils.mockTrigger(runContextFactory, nfsTrigger);

        Optional<Execution> initialExecution = nfsTrigger.evaluate(context.getKey(), context.getValue());
        assertThat(initialExecution.isEmpty(), is(true));

        Path newFile = nfsMountPoint.resolve("file.txt");
        Files.writeString(newFile, "content");

        Optional<Execution> execution = nfsTrigger.evaluate(context.getKey(), context.getValue());
        assertThat(execution.isPresent(), is(true));

        List<Map<String, Object>> rawFiles = (List<Map<String, Object>>) execution.get().getTrigger().getVariables().get("files");
        List<io.kestra.plugin.fs.nfs.Trigger.TriggeredFile> files = rawFiles.stream()
            .map(map -> objectMapper.convertValue(map, io.kestra.plugin.fs.nfs.Trigger.TriggeredFile.class))
            .toList();

        assertThat(files.size(), is(1));
        assertThat(files.getFirst().getFile().getName(), is("file.txt"));
        assertThat(files.getFirst().getChangeType(), is(io.kestra.plugin.fs.nfs.Trigger.ChangeType.CREATE));

        Optional<Execution> repeatedExecution = nfsTrigger.evaluate(context.getKey(), context.getValue());
        assertThat(repeatedExecution.isEmpty(), is(true));

        Thread.sleep(500);
        Files.writeString(newFile, "new content");
        Optional<Execution> updateExecution = nfsTrigger.evaluate(context.getKey(), context.getValue());
        assertThat(updateExecution.isEmpty(), is(true));

        Path newFile2 = nfsMountPoint.resolve("file2.log");
        Files.writeString(newFile2, "log content");
        Optional<Execution> secondCreateExecution = nfsTrigger.evaluate(context.getKey(), context.getValue());
        assertThat(secondCreateExecution.isPresent(), is(true));

        List<Map<String, Object>> secondRawFiles = (List<Map<String, Object>>) secondCreateExecution.get().getTrigger().getVariables().get("files");
        List<io.kestra.plugin.fs.nfs.Trigger.TriggeredFile> secondFiles = secondRawFiles.stream()
            .map(map -> objectMapper.convertValue(map, io.kestra.plugin.fs.nfs.Trigger.TriggeredFile.class))
            .toList();

        assertThat(secondFiles.size(), is(1));
        assertThat(secondFiles.getFirst().getFile().getName(), is("file2.log"));
    }

    @Test
    void trigger_onFileUpdate() throws Exception {
        io.kestra.plugin.fs.nfs.Trigger nfsTrigger = io.kestra.plugin.fs.nfs.Trigger.builder()
            .id(IdUtils.create())
            .type(io.kestra.plugin.fs.nfs.Trigger.class.getName())
            .from(Property.ofValue(nfsMountPoint.toString()))
            .on(Property.ofValue(StatefulTriggerInterface.On.UPDATE))
            .build();

        Map.Entry<ConditionContext, Trigger> context = TestsUtils.mockTrigger(runContextFactory, nfsTrigger);

        Path newFile = nfsMountPoint.resolve("file.txt");
        Files.writeString(newFile, "content");

        Optional<Execution> initialExecution = nfsTrigger.evaluate(context.getKey(), context.getValue());
        assertThat(initialExecution.isEmpty(), is(true));

        Optional<Execution> noChangeExecution = nfsTrigger.evaluate(context.getKey(), context.getValue());
        assertThat(noChangeExecution.isEmpty(), is(true));

        Thread.sleep(1000);
        Files.writeString(newFile, "new content");
        Thread.sleep(500);

        Optional<Execution> updateExecution = nfsTrigger.evaluate(context.getKey(), context.getValue());
        assertThat(updateExecution.isPresent(), is(true));

        List<Map<String, Object>> rawFiles = (List<Map<String, Object>>) updateExecution.get().getTrigger().getVariables().get("files");
        List<io.kestra.plugin.fs.nfs.Trigger.TriggeredFile> files = rawFiles.stream()
            .map(map -> objectMapper.convertValue(map, io.kestra.plugin.fs.nfs.Trigger.TriggeredFile.class))
            .toList();

        assertThat(files.size(), is(1));
        assertThat(files.getFirst().getFile().getName(), is("file.txt"));
        assertThat(files.getFirst().getChangeType(), is(io.kestra.plugin.fs.nfs.Trigger.ChangeType.UPDATE));

        Optional<Execution> repeatedExecution = nfsTrigger.evaluate(context.getKey(), context.getValue());
        assertThat(repeatedExecution.isEmpty(), is(true));
    }

    @Test
    void trigger_onFileCreateOrUpdate() throws Exception {
        io.kestra.plugin.fs.nfs.Trigger nfsTrigger = io.kestra.plugin.fs.nfs.Trigger.builder()
            .id(IdUtils.create())
            .type(io.kestra.plugin.fs.nfs.Trigger.class.getName())
            .from(Property.ofValue(nfsMountPoint.toString()))
            .on(Property.ofValue(StatefulTriggerInterface.On.CREATE_OR_UPDATE))
            .build();

        Map.Entry<ConditionContext, Trigger> context = TestsUtils.mockTrigger(runContextFactory, nfsTrigger);

        Optional<Execution> initialExecution = nfsTrigger.evaluate(context.getKey(), context.getValue());
        assertThat(initialExecution.isEmpty(), is(true));

        Path newFile = nfsMountPoint.resolve("file.txt");
        Files.writeString(newFile, "content");

        Optional<Execution> createExecution = nfsTrigger.evaluate(context.getKey(), context.getValue());
        assertThat(createExecution.isPresent(), is(true));

        List<Map<String, Object>> createRawFiles = (List<Map<String, Object>>) createExecution.get().getTrigger().getVariables().get("files");
        List<io.kestra.plugin.fs.nfs.Trigger.TriggeredFile> createFiles = createRawFiles.stream()
            .map(map -> objectMapper.convertValue(map, io.kestra.plugin.fs.nfs.Trigger.TriggeredFile.class))
            .toList();

        assertThat(createFiles.size(), is(1));
        assertThat(createFiles.getFirst().getChangeType(), is(io.kestra.plugin.fs.nfs.Trigger.ChangeType.CREATE));

        Optional<Execution> repeatedExecution = nfsTrigger.evaluate(context.getKey(), context.getValue());
        assertThat(repeatedExecution.isEmpty(), is(true));

        Thread.sleep(1000);
        Files.writeString(newFile, "new content");
        Thread.sleep(500);

        Optional<Execution> updateExecution = nfsTrigger.evaluate(context.getKey(), context.getValue());
        assertThat(updateExecution.isPresent(), is(true));

        List<Map<String, Object>> updateRawFiles = (List<Map<String, Object>>) updateExecution.get().getTrigger().getVariables().get("files");
        List<io.kestra.plugin.fs.nfs.Trigger.TriggeredFile> updateFiles = updateRawFiles.stream()
            .map(map -> objectMapper.convertValue(map, io.kestra.plugin.fs.nfs.Trigger.TriggeredFile.class))
            .toList();

        assertThat(updateFiles.size(), is(1));
        assertThat(updateFiles.getFirst().getChangeType(), is(io.kestra.plugin.fs.nfs.Trigger.ChangeType.UPDATE));
    }

    @Test
    void trigger_maxFiles_should_skip_execution() throws Exception {
        Files.writeString(nfsMountPoint.resolve("file1.txt"), "content1");
        Files.writeString(nfsMountPoint.resolve("file2.txt"), "content2");

        io.kestra.plugin.fs.nfs.Trigger nfsTrigger = io.kestra.plugin.fs.nfs.Trigger.builder()
            .id(IdUtils.create())
            .type(io.kestra.plugin.fs.nfs.Trigger.class.getName())
            .from(Property.ofValue(nfsMountPoint.toString()))
            .on(Property.ofValue(StatefulTriggerInterface.On.CREATE))
            .maxFiles(Property.ofValue(1))
            .build();

        Map.Entry<ConditionContext, Trigger> context = TestsUtils.mockTrigger(runContextFactory, nfsTrigger);

        Optional<Execution> execution = nfsTrigger.evaluate(context.getKey(), context.getValue());
        assertThat(execution.isEmpty(), is(true));
    }
}
