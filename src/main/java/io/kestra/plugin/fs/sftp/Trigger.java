package io.kestra.plugin.fs.sftp;

import com.google.common.collect.ImmutableMap;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.FileNotFolderException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.VFS;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.ExecutionTrigger;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.IdUtils;
import org.slf4j.Logger;

import java.io.File;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Wait for files on Google cloud storage"
)
@Plugin(
    examples = {
        @Example(
            title = "Wait for a list of file on a GCS bucket and iterate through the files",
            full = true,
            code = {
                "id: gcs-listen",
                "namespace: io.kestra.tests",
                "",
                "tasks:",
                "  - id: each",
                "    type: io.kestra.core.tasks.flows.EachSequential",
                "    tasks:",
                "      - id: return",
                "        type: io.kestra.core.tasks.debugs.Return",
                "        format: \"{{taskrun.value}}\"",
                "    value: \"{{ jq trigger '.files[].uri' }}\"",
                "",
                "triggers:",
                "  - id: watch",
                "    type: io.kestra.plugin.fs.sftp.Trigger",
                "    host: localhost",
                "    port: 6622",
                "    username: foo",
                "    password: pass",
                "    from: \"/in/\"",
                "    interval: PT10S",
                "    action: MOVE",
                "    moveDirectory: \"/archive/\"",
            }
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, AbstractSftpInterface, TriggerOutput<Downloads.Output> {
    @Schema(
        title = "The interval between test of triggers"
    )
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @Schema(
        title = "Hostname of the remote server"
    )
    @PluginProperty(dynamic = true)
    protected String host;

    @Schema(
        title = "Port of the remote server"
    )
    @PluginProperty(dynamic = true)
    protected String port;

    @Schema(
        title = "Username on the remote server"
    )
    @PluginProperty(dynamic = true)
    protected String username;

    @Schema(
        title = "Password on the remote server"
    )
    @PluginProperty(dynamic = true)
    protected String password;

    @Schema(
        title = "Private keyfile to login on the source server with ssh",
        description = "Must be the ssh key content, not a path"
    )
    @PluginProperty(dynamic = true)
    protected String keyfile;

    @Schema(
        title = "Passphrase of the ssh key"
    )
    @PluginProperty(dynamic = true)
    protected String passphrase;

    @Schema(
        title = "The directory to list"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String from;

    @Schema(
        title = "The action to do on find files"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private Downloads.Action action;

    @Schema(
        title = "The destination directory in case off `MOVE` "
    )
    @PluginProperty(dynamic = true)
    private String moveDirectory;

    @Schema(
        title = "A regexp to filter on full path"
    )
    @PluginProperty(dynamic = true)
    private String regExp;

    @Schema(
        title = "Sftp proxy host"
    )
    @PluginProperty(dynamic = true)
    protected String proxyHost;

    @Schema(
        title = "Sftp proxy port"
    )
    @PluginProperty(dynamic = true)
    protected Integer proxyPort;

    @Schema(
        title = "Sftp proxy user"
    )
    @PluginProperty(dynamic = true)
    protected String proxyUser;

    @Schema(
        title = "Sftp proxy password"
    )
    @PluginProperty(dynamic = true)
    protected String proxyPassword;

    @Schema(
        title = "Sftp proxy type"
    )
    @PluginProperty(dynamic = true)
    protected String proxyType;

    @Schema(
        title = "Is path is relative to root dir"
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    protected Boolean rootDir = true;

    @Override
    @SuppressWarnings("resource")
    public Optional<Execution> evaluate(RunContext runContext, TriggerContext context) throws Exception {
        Logger logger = runContext.logger();

        FileSystemManager fsm = VFS.getManager();

        // path
        URI from = AbstractSftpTask.sftpUri(runContext, this.host, this.port, this.username, this.password, this.from);

        // connection options
        AbstractSftpTask.FsOptionWithCleanUp fsOptionWithCleanUp = AbstractSftpTask.fsOptions(
            runContext,
            this.keyfile,
            this.passphrase,
            this.proxyHost,
            this.proxyPassword,
            this.proxyPort,
            this.proxyUser,
            this.proxyType,
            this.rootDir
        );

        // list files
        List task = List.builder()
            .id(this.id)
            .type(List.class.getName())
            .from(this.from)
            .regExp(this.regExp)
            .host(this.host)
            .port(this.port)
            .username(this.username)
            .password(this.password)
            .keyfile(this.keyfile)
            .passphrase(this.passphrase)
            .build();

        List.Output run;
        try {
            run = task.run(runContext);
        } catch (FileNotFolderException fileNotFolderException) {
            logger.debug("From path doesn't exist '{}'", String.join(", ", fileNotFolderException.getInfo()));
            return Optional.empty();
        }

        if (run.getFiles().size() == 0) {
            return Optional.empty();
        }

        String executionId = IdUtils.create();

        java.util.List<io.kestra.plugin.fs.sftp.models.File> list = run
            .getFiles()
            .stream()
            .filter(file -> file.getFileType() == FileType.FILE)
            .map(throwFunction(file -> {
                File download = Download.download(
                    fsm,
                    fsOptionWithCleanUp.getOptions(),
                    AbstractSftpTask.sftpUri(runContext, this.host, this.port, this.username, this.password, file.getPath().toString())
                );

                URI storageUri = runContext.putTempFile(
                    download,
                    executionId,
                    this
                );

                logger.debug("File '{}' download to '{}'", from, storageUri);

                return file.withPath(storageUri);
            }))
            .collect(Collectors.toList());

        Downloads.archive(
            run.getFiles(),
            this.action,
            this.moveDirectory,
            this,
            runContext
        );

        ExecutionTrigger executionTrigger = ExecutionTrigger.of(
            this,
            Downloads.Output.builder().files(list).build()
        );

        Execution execution = Execution.builder()
            .id(executionId)
            .namespace(context.getNamespace())
            .flowId(context.getFlowId())
            .flowRevision(context.getFlowRevision())
            .state(new State())
            .trigger(executionTrigger)
            .build();

        return Optional.of(execution);
    }
}
