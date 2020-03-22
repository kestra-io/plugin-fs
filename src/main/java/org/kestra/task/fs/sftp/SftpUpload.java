package org.kestra.task.fs.sftp;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.*;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.runners.RunContext;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Documentation(
    description = "Upload a file to a sftp server",
    body = "This task connects to remote sftp server and upload a file from kestra storage"
)
public class SftpUpload extends AbstractSftpTask implements RunnableTask<SftpOutput> {
    @InputProperty(
        description = "The file path to copy",
        dynamic = true
    )
    private String from;

    @InputProperty(
        description = "The destination path",
        dynamic = true
    )
    private String to;

    @SuppressWarnings({"CaughtExceptionImmediatelyRethrown"})
    public SftpOutput run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger(getClass());

        //noinspection resource never close the global instance
        FileSystemManager fsm = VFS.getManager();

        // from & to
        String toPath = runContext.render(this.to);
        URI to = new URI(this.sftpUri(runContext, toPath));
        URI from = new URI(runContext.render(this.from));

        // copy from to a temp files
        File tempFile = File.createTempFile(
            this.getClass().getSimpleName().toLowerCase() + "_",
            "." + FilenameUtils.getExtension(from.getPath())
        );

        // copy from to a temp file
        try (OutputStream outputStream = new FileOutputStream(tempFile)) {
            IOUtils.copy(runContext.uriToInputStream(from), outputStream);
        }

        // connection options
        FileSystemOptions options = this.fsOptions(runContext);

        // upload
        try {
            try (FileObject local = fsm.resolveFile(tempFile.toURI());
                 FileObject remote = fsm.resolveFile(to.toString(), options);
            ) {
                remote.copyFrom(local, Selectors.SELECT_SELF);
            }

            logger.debug("File '{}' uploaded to '{}'", from, to);

            return SftpOutput.builder()
                .from(from)
                .to(to)
                .build();
        } catch (IOException error) {
            throw error;
        } finally {
           //  cleanup.run();
        }
    }
}
