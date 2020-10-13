package org.kestra.task.fs.sftp;

import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.Example;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import java.io.File;
import java.net.URI;
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
    description = "Download multiple files from sftp server"
)
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
public class Downloads extends AbstractSftpTask implements RunnableTask<Downloads.Output> {
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
    private Downloads.Action action;

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

    static void archive(
        java.util.List<org.kestra.task.fs.sftp.models.File> blobList,
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
        URI from = new URI(this.sftpUri(runContext, this.from));

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
            .build();
        List.Output run = task.run(runContext);

        java.util.List<org.kestra.task.fs.sftp.models.File> list = run
            .getFiles()
            .stream()
            .map(throwFunction(file -> {
                File download = Download.download(
                    fsm,
                    fsOptionWithCleanUp.getOptions(),
                    new URI(AbstractSftpTask.sftpUri(runContext, this.host, this.port, this.username, this.password, file.getPath().toString()))
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
    public static class Output implements org.kestra.core.models.tasks.Output {
        @OutputProperty(
            description = "The bucket of the downloaded file"
        )
        private final java.util.List<org.kestra.task.fs.sftp.models.File> files;
    }
}
