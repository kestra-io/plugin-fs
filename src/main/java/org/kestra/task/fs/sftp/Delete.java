package org.kestra.task.fs.sftp;

import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import java.net.URI;
import java.util.NoSuchElementException;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Documentation(
    description = "Delete a file to a SFTP server."
)
public class Delete extends AbstractSftpTask implements RunnableTask<Delete.Output> {
    @InputProperty(
        description = "The file to delete",
        dynamic = true
    )
    private String uri;

    @InputProperty(
        description = "raise an error if the file is not found"
    )
    @Builder.Default
    private final Boolean errorOnMissing = false;

    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        //noinspection resource never close the global instance
        FileSystemManager fsm = VFS.getManager();

        // path
        URI from = new URI(this.sftpUri(runContext, this.uri));

        // connection options
        FsOptionWithCleanUp fsOptionWithCleanUp = this.fsOptions(runContext);

        // list
        try {
            try (
                FileObject local = fsm.resolveFile(from.toString(), fsOptionWithCleanUp.getOptions())
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
        } finally {
            fsOptionWithCleanUp.getCleanup().run();
        }
    }

    @Builder
    @Getter
    public static class Output implements org.kestra.core.models.tasks.Output {
        @OutputProperty(
            description = "The deleted uri"
        )
        private final URI uri;

        @OutputProperty(
            description = "If the files was really deleted"
        )
        private final boolean deleted;
    }
}
