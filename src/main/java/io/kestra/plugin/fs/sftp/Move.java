package io.kestra.plugin.fs.sftp;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.slf4j.Logger;

import java.net.URI;
import java.util.NoSuchElementException;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Move a file to a SFTP server.",
    description ="If the destination directory doesn't exist, it will be created"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "host: localhost",
                "port: 6622",
                "username: foo",
                "password: pass",
                "from: \"/upload/dir1/file.txt\"",
                "to: \"/upload/dir2/file.txt\"",
            }
        )
    }
)
public class Move extends AbstractSftpTask implements RunnableTask<Move.Output> {
    @Schema(
        title = "The file to move"
    )
    @PluginProperty(dynamic = true)
    private String from;

    @Schema(
        title = "The destination path to move",
        description = "The full destination path (with filename optionnaly)\n" +
            "If the destFile exists, it is deleted first."
    )
    @PluginProperty(dynamic = true)
    private String to;

    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        try (StandardFileSystemManager fsm = new StandardFileSystemManager()) {
            fsm.init();

            // path
            URI from = this.sftpUri(runContext, this.from);
            URI to = this.sftpUri(runContext, this.to);

            // user pass a destination without filename, we add it
            if (!isDirectory(from) && isDirectory(to)) {
                to = to.resolve(StringUtils.stripEnd(to.getPath(), "/") + "/" + FilenameUtils.getName(from.getPath()));
            }

            // connection options
            FileSystemOptions fileSystemOptions = this.fsOptions(runContext);

            // list
            try (
                FileObject local = fsm.resolveFile(from.toString(), fileSystemOptions);
                FileObject remote = fsm.resolveFile(to.toString(), fileSystemOptions);
            ) {
                if (!local.exists()) {
                    throw new NoSuchElementException("Unable to find file '" + from + "'");
                }

                if (!remote.exists()) {
                    URI pathToCreate = to.resolve("/" + FilenameUtils.getPath(to.getPath()));

                    try (FileObject directory = fsm.resolveFile(pathToCreate)) {
                        directory.createFolder();
                        logger.debug("Create directory '{}", pathToCreate);
                    }
                }

                local.moveTo(remote);

                if (local.exists()) {
                    logger.debug("Move file '{}'", from);
                } else {
                    logger.debug("File doesn't exists '{}'", from);
                }

                return Output.builder()
                    .from(output(from))
                    .to(output(to))
                    .build();
            }
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The from uri"
        )
        private final URI from;

        @Schema(
            title = "The destination uri"
        )
        private final URI to;
    }
}
