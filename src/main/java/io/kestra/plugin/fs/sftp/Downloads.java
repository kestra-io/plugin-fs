package io.kestra.plugin.fs.sftp;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import java.io.File;
import java.net.URI;
import java.util.stream.Collectors;
import javax.annotation.RegEx;
import javax.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Download multiple files from sftp server"
)
@Plugin(
    examples = {
        @Example(
            title = "Download a list of files and move it to an archive folders",
            code = {
                "host: localhost",
                "port: 6622",
                "username: foo",
                "password: pass",
                "from: \"/in/\"",
                "interval: PT10S",
                "action: MOVE",
                "moveDirectory: \"/archive/\"",
            }
        )
    }
)
public class Downloads extends AbstractSftpTask implements RunnableTask<Downloads.Output> {
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

    static void archive(
        java.util.List<io.kestra.plugin.fs.sftp.models.File> blobList,
        Action action,
        String moveDirectory,
        AbstractSftpInterface abstractSftpTask,
        RunContext runContext
    ) throws Exception {
        if (action == Action.DELETE) {
            blobList
                .forEach(throwConsumer(file -> {
                    Delete delete = Delete.builder()
                        .id("delete")
                        .type(Delete.class.getName())
                        .uri(file.getPath().toString())
                        .host(abstractSftpTask.getHost())
                        .port(abstractSftpTask.getPort())
                        .username(abstractSftpTask.getUsername())
                        .password(abstractSftpTask.getPassword())
                        .keyfile(abstractSftpTask.getKeyfile())
                        .passphrase(abstractSftpTask.getPassphrase())
                        .build();
                    delete.run(runContext);
                }));
        } else if (action == Action.MOVE) {
            blobList
                .forEach(throwConsumer(file -> {
                    Move copy = Move.builder()
                        .id("archive")
                        .type(Move.class.getName())
                        .from(file.getPath().toString())
                        .to(StringUtils.stripEnd(runContext.render(moveDirectory) + "/", "/")
                            + "/" + FilenameUtils.getName(file.getName())
                        )
                        .host(abstractSftpTask.getHost())
                        .port(abstractSftpTask.getPort())
                        .username(abstractSftpTask.getUsername())
                        .password(abstractSftpTask.getPassword())
                        .keyfile(abstractSftpTask.getKeyfile())
                        .passphrase(abstractSftpTask.getPassphrase())
                        .build();
                    copy.run(runContext);
                }));
        }
    }

    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        FileSystemManager fsm = VFS.getManager();

        // path
        URI from = this.sftpUri(runContext, this.from);

        // connection options
        FsOptionWithCleanUp fsOptionWithCleanUp = this.fsOptions(runContext);

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
            .proxyHost(this.proxyHost)
            .proxyPassword(this.proxyPassword)
            .proxyUser(this.proxyUser)
            .proxyPort(this.proxyPort)
            .proxyType(this.proxyType)
            .build();
        List.Output run = task.run(runContext);

        java.util.List<io.kestra.plugin.fs.sftp.models.File> list = run
            .getFiles()
            .stream()
            .map(throwFunction(file -> {
                File download = Download.download(
                    fsm,
                    fsOptionWithCleanUp.getOptions(),
                    AbstractSftpTask.sftpUri(runContext, this.host, this.port, this.username, this.password, file.getPath().toString())
                );

                URI storageUri = runContext.putTempFile(download);

                logger.debug("File '{}' download to '{}'", from, storageUri);

                return file.withPath(storageUri);
            }))
            .collect(Collectors.toList());


        if (this.action != null) {
            Downloads.archive(
                run.getFiles(),
                this.action,
                this.moveDirectory,
                this,
                runContext
            );
        }

        // nom du fichier
        // filtre sur les reps
        //

        return Output
            .builder()
            .files(list)
            .build();
    }

    public enum Action {
        MOVE,
        DELETE
    }


    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The bucket of the downloaded file"
        )
        @PluginProperty(additionalProperties = io.kestra.plugin.fs.sftp.models.File.class)
        private final java.util.List<io.kestra.plugin.fs.sftp.models.File> files;
    }
}
