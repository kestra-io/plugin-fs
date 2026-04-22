package io.kestra.plugin.fs.smb;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.fs.vfs.Downloads;
import io.kestra.plugin.fs.vfs.models.File;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.FileType;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;

import static io.kestra.core.models.triggers.StatefulTriggerService.*;
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger on new SMB files",
    description = "Polls a share path on the interval and starts a Flow when new files appear. Default port 445. Use `action` MOVE/DELETE to avoid reprocessing."
)
@Plugin(
    examples = {
        @Example(
            title = """
                Wait for one or more files in a given SMB server's directory and process each of these files sequentially.
                Then move them to another share which is used as an archive.""",
            full = true,
            code = """
                id: smb_trigger_flow
                namespace: company.team

                tasks:
                  - id: for_each_file
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.files }}"
                    tasks:
                      - id: return
                        type: io.kestra.plugin.core.debug.Return
                        format: "{{ taskrun.value | jq('.path') }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.fs.smb.Trigger
                    host: localhost
                    port: "445"
                    username: foo
                    password: "{{ secret('SMB_PASSWORD') }}"
                    from: "/my_share/in/"
                    interval: PT10S
                    action: MOVE
                    moveDirectory: "/archive_share/"
                """
        ),
        @Example(
            title = """
                Wait for one or more files in a given SMB server's directory and process each of these files sequentially.
                Then move them to another share which is used as an archive.""",
            full = true,
            code = """
                id: smb_trigger_flow
                namespace: company.team

                tasks:
                  - id: for_each_file
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.files }}"
                    tasks:
                      - id: return
                        type: io.kestra.plugin.core.debug.Return
                        format: "{{ taskrun.value | jq('.path') }}"
                      - id: delete
                        type: io.kestra.plugin.fs.smb.Delete
                        host: localhost
                        port: "445"
                        username: foo
                        password: "{{ secret('SMB_PASSWORD') }}"
                        uri: "/my_share/in/{{ taskrun.value | jq('.path') }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.fs.smb.Trigger
                    host: localhost
                    port: "445"
                    username: foo
                    password: "{{ secret('SMB_PASSWORD') }}"
                    from: "/my_share/in/"
                    interval: PT10S
                    action: NONE
                """
        ),
        @Example(
            title = """
                Wait for one or more files in a given SMB server's directory (composed of share name followed by dir path) and process each of these files sequentially.
                In this example, we restrict the trigger to only wait for CSV files in the `mydir` directory.""",
            full = true,
            code = """
                id: smb_wait_for_csv_in_my_share_my_dir
                namespace: company.team

                tasks:
                  - id: each
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.files }}"
                    tasks:
                      - id: return
                        type: io.kestra.plugin.core.debug.Return
                        format: "{{ taskrun.value | jq('.path') }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.fs.smb.Trigger
                    host: localhost
                    port: "445"
                    username: foo
                    password: "{{ secret('SMB_PASSWORD') }}"
                    from: "my_share/mydir/"
                    regExp: ".*.csv"
                    action: MOVE
                    moveDirectory: "my_share/archivedir"
                    interval: PT10S
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, SmbInterface, TriggerOutput<Trigger.Output>, StatefulTriggerInterface {
    @Schema(title = "Interval between trigger checks")
    @Builder.Default
    @PluginProperty(group = "execution")
    private final Duration interval = Duration.ofSeconds(60);

    @NotNull
    protected Property<String> host;
    @PluginProperty(secret = true)
    protected Property<String> username;
    @PluginProperty(secret = true)
    protected Property<String> password;

    @Builder.Default
    protected Property<String> port = Property.ofValue("445");

    @Schema(title = "Directory URI to watch")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> from;

    @Schema(title = "Action to perform on retrieved files", description = "If NONE, handle files in the Flow to avoid reprocessing.")
    @NotNull
    private Property<Downloads.Action> action;

    @Schema(title = "Destination directory when action is MOVE")
    @PluginProperty(group = "advanced")
    private Property<String> moveDirectory;

    @Schema(title = "Regexp filter on full path")
    @PluginProperty(group = "advanced")
    private Property<String> regExp;

    @Schema(title = "List files recursively")
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> recursive = Property.ofValue(false);

    @Schema(title = "Change event type to react to")
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<On> on = Property.ofValue(On.CREATE_OR_UPDATE);

    @Schema(title = "Custom state key for deduplication")
    @PluginProperty(group = "connection")
    private Property<String> stateKey;

    @Schema(title = "TTL for state entries")
    @PluginProperty(group = "advanced")
    private Property<Duration> stateTtl;

    @Builder.Default
    @Schema(title = "Maximum files to process per poll")
    @PluginProperty(group = "execution")
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

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        var runContext = conditionContext.getRunContext();
        var logger = runContext.logger();

        var rOn = runContext.render(on).as(On.class).orElse(On.CREATE_OR_UPDATE);
        var rStateKey = runContext.render(stateKey)
            .as(String.class)
            .orElse(StatefulTriggerService.defaultKey(context.getNamespace(), context.getFlowId(), id));
        var rStateTtl = runContext.render(stateTtl).as(Duration.class);

        var ctx = SmbService.createContext(runContext, this);
        try {
            var fromPath = runContext.render(this.from).as(String.class).orElseThrow();

            io.kestra.plugin.fs.vfs.List.Output run; // FQCN needed: naming conflict with smb.List
            try {
                run = SmbService.list(
                    runContext,
                    ctx,
                    this,
                    fromPath,
                    runContext.render(this.regExp).as(String.class).orElse(null),
                    runContext.render(this.recursive).as(Boolean.class).orElse(false)
                );
            } catch (org.codelibs.jcifs.smb.impl.SmbException e) {
                logger.debug("From path doesn't exist '{}'", fromPath);
                return Optional.empty();
            }

            if (run.getFiles().isEmpty()) {
                return Optional.empty();
            }

            var files = run.getFiles().stream()
                .filter(file -> file.getFileType() == FileType.FILE)
                .toList();

            var state = readState(runContext, rStateKey, rStateTtl);

            var pendingFiles = new ArrayList<PendingFile>();

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

                if (!shouldFire(prev, version, rOn)) {
                    computeAndUpdateState(state, candidate, rOn);
                    continue;
                }

                var changeType = prev == null ? ChangeType.CREATE : ChangeType.UPDATE;
                pendingFiles.add(new PendingFile(file, candidate, changeType));
            }

            var rMaxFiles = runContext.render(this.maxFiles).as(Integer.class).orElse(25);
            java.util.List<PendingFile> limitedPending = pendingFiles; // reassigned below
            if (pendingFiles.size() > rMaxFiles) {
                logger.warn("Too many files to process ({}), limiting to {}", pendingFiles.size(), rMaxFiles);
                limitedPending = pendingFiles.subList(0, rMaxFiles);
            }

            if (limitedPending.isEmpty()) {
                writeState(runContext, rStateKey, state, rStateTtl);
                return Optional.empty();
            }

            var actionFiles = new ArrayList<File>();
            var toFire = new ArrayList<TriggeredFile>();

            // 1) Download first, do NOT update state yet.
            for (PendingFile pending : limitedPending) {
                var download = SmbService.download(
                    runContext,
                    ctx,
                    this,
                    pending.file.getServerPath().getPath()
                );

                logger.debug("File '{}' download to '{}'", fromPath, download.getTo());

                var downloaded = pending.file.withPath(download.getTo());
                actionFiles.add(downloaded);

                toFire.add(TriggeredFile.builder()
                    .file(downloaded)
                    .changeType(pending.changeType)
                    .build());
            }

            if (toFire.isEmpty()) {
                writeState(runContext, rStateKey, state, rStateTtl);
                return Optional.empty();
            }

            // 2) Perform remote action BEFORE committing state.
            if (this.action != null) {
                var rAction = runContext.render(this.action).as(Downloads.Action.class).orElse(null);

                SmbService.performAction(
                    runContext,
                    ctx,
                    this,
                    actionFiles,
                    rAction,
                    runContext.render(this.moveDirectory).as(String.class).orElse(null)
                );
            }

            // 3) Only now that downloads + actions succeeded, commit state for fired files.
            for (PendingFile pending : limitedPending) {
                computeAndUpdateState(state, pending.candidate, rOn);
            }

            writeState(runContext, rStateKey, state, rStateTtl);

            var execution = TriggerService.generateExecution(
                this,
                conditionContext,
                context,
                Output.builder().files(toFire).build()
            );

            return Optional.of(execution);
        } finally {
            ctx.close();
        }
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
