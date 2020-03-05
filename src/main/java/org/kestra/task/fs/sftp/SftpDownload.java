package org.kestra.task.fs.sftp;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.FileSystemException;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.task.fs.VfsDownload;

import java.io.IOException;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Documentation(
    description = "Download file from Sftp server to local file system",
    body = "This task connects to remote sftp server and copy file to local file system with given inputs"
)
public class SftpDownload extends VfsDownload {
    @Override
    protected void afterFileSync() {

    }

    @Override
    protected void beforeFileSync() {

    }

    protected String remoteUri() {
        return "sftp://" + auth.getBasicAuth() + host + ":" + port + "/" + remotePath;
    }

    @Override
    public void addFsOptions() throws IOException {
        new SftpOptions(this).addFsOptions();
    }
}
