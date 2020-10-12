package org.kestra.task.fs.sftp;

import com.google.common.collect.ImmutableMap;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.executions.Execution;
import org.kestra.core.models.flows.State;
import org.kestra.core.models.triggers.AbstractTrigger;
import org.kestra.core.models.triggers.PollingTriggerInterface;
import org.kestra.core.models.triggers.TriggerContext;
import org.kestra.core.runners.RunContext;
import org.kestra.core.utils.IdUtils;
import org.slf4j.Logger;

import java.io.File;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.RegEx;
import javax.validation.constraints.NotNull;

import static org.kestra.core.utils.Rethrow.throwConsumer;
import static org.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Documentation(
    description = "Wait for files on Google cloud storage"
)
@Example(
    title = "Wait for a list of file on a GCS bucket and iterate through the files",
    full = true,
    code = {
        "id: gcs-listen",
        "namespace: org.kestra.tests",
        "",
        "tasks:",
        "  - id: each",
        "    type: org.kestra.core.tasks.flows.EachSequential",
        "    tasks:",
        "      - id: return",
        "        type: org.kestra.core.tasks.debugs.Return",
        "        format: \"{{taskrun.value}}\"",
        "    value: \"{{ jq trigger '.uri' true }}\"",
        "",
        "triggers:",
        "  - id: watch",
        "    type: org.kestra.task.gcp.gcs.Trigger",
        "    interval: \"PT5M\"",
        "    from: gs://my-bucket/kestra/listen/",
        "    action: MOVE",
        "    moveDirectory: gs://my-bucket/kestra/archive/",
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface {
    @InputProperty(
        description = "The interval between test of triggers"
    )
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    @InputProperty(
        description = "Hostname of the remote server",
        dynamic = true
    )
    protected String host;

    @InputProperty(
        description = "Port of the remote server",
        dynamic = true
    )
    protected String port;

    @InputProperty(
        description = "Username on the remote server",
        dynamic = true
    )
    protected String username;

    @InputProperty(
        description = "Password on the remote server",
        dynamic = true
    )
    protected String password;

    @InputProperty(
        description = "Private keyfile to login on the source server with ssh",
        body = "Must be the ssh key content, not a path",
        dynamic = true
    )
    protected String keyfile;

    @InputProperty(
        description = "Passphrase of the ssh key",
        dynamic = true
    )
    protected String passphrase;

    @InputProperty(
        description = "The directory to list",
        dynamic = true
    )
    @NotNull
    private String from;

    @InputProperty(
        description = "The action to do on find files",
        dynamic = true
    )
    @NotNull
    private Action action;

    @InputProperty(
        description = "The destination directory in case off `MOVE` ",
        dynamic = true
    )
    private String moveDirectory;

    @InputProperty(
        description = "A regexp to filter on full path"
    )
    @RegEx
    private String regExp;

    @Override
    @SuppressWarnings("resource")
    public Optional<Execution> evaluate(RunContext runContext, TriggerContext context) throws Exception {
        Logger logger = runContext.logger();

        @SuppressWarnings("resource")
        FileSystemManager fsm = VFS.getManager();

        // path
        URI from = new URI(AbstractSftpTask.sftpUri(runContext, this.host, this.port, this.username, this.password, this.from));

        // connection options
        AbstractSftpTask.FsOptionWithCleanUp fsOptionWithCleanUp = AbstractSftpTask.fsOptions(runContext, this.keyfile, this.passphrase);

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
        List.Output run = task.run(runContext);

        if (run.getFiles().size() == 0) {
            return Optional.empty();
        }

        String executionId = IdUtils.create();

        java.util.List<URI> list = run
            .getFiles()
            .stream()
            .map(throwFunction(file -> {
                File download = Download.download(
                    fsm,
                    fsOptionWithCleanUp.getOptions(),
                    new URI(AbstractSftpTask.sftpUri(runContext, this.host, this.port, this.username, this.password, file.getPath().toString()))
                );

                URI storageUri = runContext.putTempFile(
                    download,
                    executionId,
                    this
                );

                logger.debug("File '{}' download to '{}'", from, storageUri);

                return storageUri;
            }))
            .collect(Collectors.toList());

        if (this.action == Action.DELETE) {
            run
                .getFiles()
                .forEach(throwConsumer(file -> {
                    Delete delete = Delete.builder()
                        .id(this.id)
                        .type(Delete.class.getName())
                        .uri(file.getPath().toString())
                        .host(this.host)
                        .port(this.port)
                        .username(this.username)
                        .password(this.password)
                        .keyfile(this.keyfile)
                        .passphrase(this.passphrase)
                        .build();
                    delete.run(runContext);
                }));
        } else if (this.action == Action.MOVE) {
            run
                .getFiles()
                .forEach(throwConsumer(file -> {
                    Move copy = Move.builder()
                        .id(this.id)
                        .type(Move.class.getName())
                        .from(file.getPath().toString())
                        .to(StringUtils.stripEnd(runContext.render(this.moveDirectory) + "/", "/")
                            + "/" + FilenameUtils.getName(file.getName())
                        )
                        .host(this.host)
                        .port(this.port)
                        .username(this.username)
                        .password(this.password)
                        .keyfile(this.keyfile)
                        .passphrase(this.passphrase)
                        .build();
                    copy.run(runContext);
                }));
        }

        Execution execution = Execution.builder()
            .id(executionId)
            .namespace(context.getNamespace())
            .flowId(context.getFlowId())
            .flowRevision(context.getFlowRevision())
            .state(new State())
            .variables(ImmutableMap.of(
                "trigger", ImmutableMap.of(
                     "uri", list
                )
            ))
            .build();

        return Optional.of(execution);
    }

    public enum Action {
        MOVE,
        DELETE
    }
}
