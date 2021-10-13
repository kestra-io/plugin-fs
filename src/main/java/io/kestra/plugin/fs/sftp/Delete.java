package io.kestra.plugin.fs.sftp;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.VFS;
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
    title = "Delete a file to a SFTP server."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "host: localhost",
                "port: 6622",
                "username: foo",
                "password: pass",
                "uri: \"/upload/dir1/file.txt\"",
            }
        )
    }
)
public class Delete extends AbstractSftpTask implements RunnableTask<Delete.Output> {
    @Schema(
        title = "The file to delete")
    private String uri;

    @Schema(
        title = "raise an error if the file is not found"
    )
    @Builder.Default
    private final Boolean errorOnMissing = false;

    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        try (StandardFileSystemManager fsm = new StandardFileSystemManager()) {
            fsm.init();

            // path
            URI from = this.sftpUri(runContext, this.uri);

            // connection options
            FileSystemOptions fileSystemOptions = this.fsOptions(runContext);

            // list
            try (
                FileObject local = fsm.resolveFile(from.toString(), fileSystemOptions)
            ) {
                if (!local.exists() && errorOnMissing) {
                    throw new NoSuchElementException("Unable to find file '" + from + "'");
                }

                if (local.exists()) {
                    logger.debug("Deleted file '{}'", from);
                } else {
                    logger.debug("File doesn't exists '{}'", from);
                }

                return Output.builder()
                    .uri(output(from))
                    .deleted(local.delete())
                    .build();
            }
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The deleted uri"
        )
        private final URI uri;

        @Schema(
            title = "If the files was really deleted"
        )
        private final boolean deleted;
    }
}
