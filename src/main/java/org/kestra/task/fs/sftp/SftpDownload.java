package org.kestra.task.fs.sftp;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.vfs2.*;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URI;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Documentation(
    description = "Download file from sftp server",
    body = "This task connects to remote sftp server and copy file to kestra file storage"
)
public class SftpDownload extends AbstractSftpTask implements RunnableTask<SftpOutput> {
    @InputProperty(
        description = "The fully-qualified URIs that point to destination path",
        dynamic = true
    )
    protected String from;

    @SuppressWarnings({"CaughtExceptionImmediatelyRethrown"})
    public SftpOutput run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger(getClass());

        //noinspection resource never close the global instance
        FileSystemManager fsm = VFS.getManager();

        // path
        URI from = new URI(this.sftpUri(runContext, this.from));

        // temp file where download will be copied
        File tempFile = File.createTempFile(
            this.getClass().getSimpleName().toLowerCase() + "_",
            "." + FilenameUtils.getExtension(from.getPath())
        );

        // connection options
        FsOptionWithCleanUp fsOptionWithCleanUp = this.fsOptions(runContext);

        // download
        try {
            try (
                FileObject local = fsm.resolveFile(tempFile.toURI());
                FileObject remote = fsm.resolveFile(from.toString(), fsOptionWithCleanUp.getOptions())
            ) {
                local.copyFrom(remote, Selectors.SELECT_SELF);
            }

            URI storageUri = runContext.putTempFile(tempFile);

            logger.debug("File '{}' download to '{}'", from, storageUri);

            return SftpOutput.builder()
                .from(from)
                .to(storageUri)
                .build();
        } catch (IOException error) {
            throw error;
        } finally {
            fsOptionWithCleanUp.getCleanup().run();
        }
    }
}
