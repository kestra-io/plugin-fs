package org.kestra.task.fs.sftp;

import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.*;
import org.kestra.core.exceptions.IllegalVariableEvaluationException;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.core.runners.RunContext;
import org.kestra.task.fs.Output;
import org.kestra.task.fs.VfsTask;
import org.kestra.task.fs.VfsTaskException;
import org.kestra.task.fs.auths.Auth;
import org.slf4j.Logger;

import java.io.*;
import java.net.URI;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Documentation(
    description = "Download file from Sftp server to local file system",
    body = "This task connects to remote sftp server and copy file to local file system with given inputs"
)
public class SftpDownload extends VfsTask {
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger(getClass());
        FileSystemManager fsm = VFS.getManager();

        // auth
        auth = new Auth(runContext, this);
        options = new FileSystemOptions();

        // from
        String fromPath = runContext.render(this.getFrom());
        URI from = new URI(auth.getSftpUri(fromPath));

        // copy from to a temp files
        File tempFile = File.createTempFile(
            this.getClass().getSimpleName().toLowerCase() + "_",
            "." + FilenameUtils.getExtension(from.getPath())
        );

        SftpOptions sftpOptions = new SftpOptions(this);
        sftpOptions.addFsOptions();
        URI storageUri = null;
        try {
            FileObject local = fsm.resolveFile(tempFile.toURI());
            FileObject remote = fsm.resolveFile(from.toString(), options);
            local.copyFrom(remote, Selectors.SELECT_SELF);
            local.close();
            remote.close();
            storageUri = runContext.putTempFile(tempFile);
        } catch (IOException error) {
            throw error;
        } finally {
            sftpOptions.cleanup();
            tempFile.delete();
        }

        logger.info("file {} uploaded to sftp {}", from, storageUri);

        return Output.builder()
            .from(from)
            .to(storageUri)
            .build();
    }
}
