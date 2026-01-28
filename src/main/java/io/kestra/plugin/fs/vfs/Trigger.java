package io.kestra.plugin.fs.vfs;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.jcraft.jsch.JSch;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.fs.vfs.models.File;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.FileNotFolderException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;

import static io.kestra.core.models.triggers.StatefulTriggerService.*;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class Trigger extends AbstractTrigger implements PollingTriggerInterface, AbstractVfsInterface, TriggerOutput<Trigger.Output>, StatefulTriggerInterface {
    @Schema(title = "The interval between trigger checks")
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    protected Property<String> host;
    protected Property<String> username;
    protected Property<String> password;

    @Schema(title = "The directory to list")
    @NotNull
    private Property<String> from;

    @Schema(title = "The action to perform on the retrieved files. If using 'NONE' make sure to handle the files inside your flow to avoid infinite triggering.")
    @NotNull
    private Property<Downloads.Action> action;

    @Schema(title = "The destination directory in case of `MOVE`")
    private Property<String> moveDirectory;

    @Schema(title = "A regexp to filter on full path")
    private Property<String> regExp;

    @Schema(title = "List files recursively")
    @Builder.Default
    private Property<Boolean> recursive = Property.ofValue(false);

    @Builder.Default
    @Schema(title = "Enable the RSA/SHA1 algorithm (disabled by default)")
    @NotNull
    private Property<Boolean> enableSshRsa1 = Property.ofValue(false);

    @Builder.Default
    private Property<On> on = Property.ofValue(On.CREATE_OR_UPDATE);

    private Property<String> stateKey;
    private Property<Duration> stateTtl;

    @Builder.Default
    @Schema(title = "The maximum number of files to retrieve at once")
    private Property<Integer> maxFiles = Property.ofValue(25);

    private static class PendingFile {
        private final File file;
        private final Entry candidate;
        private final ChangeType changeType;

        private PendingFile(File file, Entry candidate, ChangeType changeType) {
            this.file = file;
            this.candidate = candidate;
            this.changeType = changeType;
        }
    }

    protected abstract FileSystemOptions fsOptions(RunContext runContext) throws IllegalVariableEvaluationException, IOException;

    protected abstract String scheme();

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();
        URI from = createUri(runContext);

        // connection options
        FileSystemOptions fileSystemOptions = this.fsOptions(runContext);

        var renderedHost = runContext.render(this.host).as(String.class).orElseThrow();
        var renderedPort = runContext.render(this.getPort()).as(String.class).orElseThrow();
        var jsch = new JSch();
        var session = jsch.getSession(
            runContext.render(username).as(String.class).orElse(null),
            renderedHost,
            Integer.parseInt(renderedPort)
        );

        var rOn = runContext.render(on).as(On.class).orElse(On.CREATE_OR_UPDATE);
        var rStateKey = runContext.render(stateKey)
            .as(String.class)
            .orElse(StatefulTriggerService.defaultKey(context.getNamespace(), context.getFlowId(), id));
        var rStateTtl = runContext.render(stateTtl).as(Duration.class);

        // enable disabled by default weak RSA/SHA1 algorithm
        if (runContext.render(enableSshRsa1).as(Boolean.class).orElseThrow()) {
            logger.info("RSA/SHA1 is enabled, be advised that SHA1 is no longer considered secure by the general cryptographic community.");
            session.setConfig("server_host_key", session.getConfig("server_host_key") + ",ssh-rsa");
            session.setConfig("PubkeyAcceptedAlgorithms", session.getConfig("PubkeyAcceptedAlgorithms") + ",ssh-rsa");
        }

        try (StandardFileSystemManager fsm = new KestraStandardFileSystemManager(runContext)) {
            fsm.setConfiguration(StandardFileSystemManager.class.getResource(KestraStandardFileSystemManager.CONFIG_RESOURCE));
            fsm.init();

            List.Output run;
            try {
                run = VfsService.list(
                    runContext,
                    fsm,
                    fileSystemOptions,
                    from,
                    runContext.render(this.regExp).as(String.class).orElse(null),
                    runContext.render(this.recursive).as(Boolean.class).orElse(false)
                );
            } catch (FileNotFolderException fileNotFolderException) {
                logger.debug("From path doesn't exist '{}'", String.join(", ", fileNotFolderException.getInfo()));
                return Optional.empty();
            }

            if (run.getFiles().isEmpty()) {
                return Optional.empty();
            }

            java.util.List<File> files = run.getFiles()
                .stream()
                .filter(file -> file.getFileType() == FileType.FILE)
                .toList();

            Map<String, Entry> state = readState(runContext, rStateKey, rStateTtl);

            java.util.List<PendingFile> pendingFiles = new ArrayList<>();

            for (File file : files) {
                if (file.getFileType().equals(FileType.FOLDER)) {
                    continue;
                }

                var remotePath = file.getServerPath().getPath();
                var updatedDate = Optional.ofNullable(file.getUpdatedDate()).orElse(Instant.EPOCH);
                var size = Optional.ofNullable(file.getSize()).orElse(0L);
                var version = String.format("%d_%d_%s", updatedDate.toEpochMilli(), size, remotePath);

                var candidate = Entry.candidate(remotePath, version, updatedDate);
                var prev = state.get(remotePath);

                // IMPORTANT: keep state up to date for non-fired files
                if (!shouldFire(prev, version, rOn)) {
                    computeAndUpdateState(state, candidate, rOn);
                    continue;
                }

                var changeType = prev == null ? ChangeType.CREATE : ChangeType.UPDATE;
                pendingFiles.add(new PendingFile(file, candidate, changeType));
            }

            int rMaxFiles = runContext.render(this.maxFiles).as(Integer.class).orElse(25);
            java.util.List<PendingFile> limitedPending = pendingFiles;
            if (pendingFiles.size() > rMaxFiles) {
                logger.warn("Too many files to process ({}), limiting to {}", pendingFiles.size(), rMaxFiles);
                limitedPending = pendingFiles.subList(0, rMaxFiles);
            }

            if (limitedPending.isEmpty()) {
                // still persist state for files we skipped / updated above
                writeState(runContext, rStateKey, state, rStateTtl);
                return Optional.empty();
            }

            java.util.List<File> actionFiles = new ArrayList<>();
            java.util.List<TriggeredFile> toFire = new ArrayList<>();

            // 1) Download first, do NOT update state yet.
            for (PendingFile pending : limitedPending) {
                Download.Output download = VfsService.download(
                    runContext,
                    fsm,
                    fileSystemOptions,
                    VfsService.uri(
                        runContext,
                        this.scheme(),
                        runContext.render(this.host).as(String.class).orElse(null),
                        runContext.render(this.getPort()).as(String.class).orElse(null),
                        runContext.render(this.username).as(String.class).orElse(null),
                        runContext.render(this.password).as(String.class).orElse(null),
                        pending.file.getServerPath().getPath()
                    )
                );

                logger.debug("File '{}' download to '{}'", from.getPath(), download.getTo());

                var downloaded = pending.file.withPath(download.getTo());
                actionFiles.add(downloaded);

                toFire.add(TriggeredFile.builder()
                    .file(downloaded)
                    .changeType(pending.changeType)
                    .build());
            }

            if (toFire.isEmpty()) {
                // nothing to fire; persist state updates made earlier
                writeState(runContext, rStateKey, state, rStateTtl);
                return Optional.empty();
            }

            // 2) Perform remote action BEFORE committing state.
            if (this.action != null) {
                var renderedAction = runContext.render(this.action).as(Downloads.Action.class).orElse(null);

                VfsService.performAction(
                    runContext,
                    fsm,
                    fileSystemOptions,
                    actionFiles,
                    renderedAction,
                    VfsService.uri(
                        runContext,
                        this.scheme(),
                        runContext.render(this.host).as(String.class).orElse(null),
                        runContext.render(this.getPort()).as(String.class).orElse(null),
                        runContext.render(this.username).as(String.class).orElse(null),
                        runContext.render(this.password).as(String.class).orElse(null),
                        runContext.render(this.moveDirectory).as(String.class).orElse(null)
                    )
                );
            }

            // 3) Only now that downloads + actions succeeded, commit state for fired files.
            for (PendingFile pending : limitedPending) {
                computeAndUpdateState(state, pending.candidate, rOn);
            }

            writeState(runContext, rStateKey, state, rStateTtl);

            Execution execution = TriggerService.generateExecution(
                this,
                conditionContext,
                context,
                Output.builder().files(toFire).build()
            );

            return Optional.of(execution);
        }
    }

    private URI createUri(RunContext runContext) throws IllegalVariableEvaluationException, URISyntaxException {
        return VfsService.uri(
            runContext,
            this.scheme(),
            runContext.render(this.host).as(String.class).orElse(null),
            runContext.render(this.getPort()).as(String.class).orElse(null),
            runContext.render(this.username).as(String.class).orElse(null),
            runContext.render(this.password).as(String.class).orElse(null),
            runContext.render(this.from).as(String.class).orElseThrow()
        );
    }

    public enum ChangeType {
        CREATE,
        UPDATE
    }

    @Getter
    @AllArgsConstructor
    @Builder
    public static class TriggeredFile {
        @JsonUnwrapped
        private final File file;
        private final ChangeType changeType;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "List of files that triggered the flow, each with its change type.")
        private final java.util.List<TriggeredFile> files;
    }
}
