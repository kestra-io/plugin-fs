package org.kestra.task.fs.ftp;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.FileSystemException;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.task.fs.VfsUpload;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Documentation(
    description = "Upload file from local FS to SFTP",
    body = "This task reads on local file system a file that will be uploaded to the target SFTP with given inputs"
)
public class FtpUpload extends VfsUpload {
    @Override
    protected void afterFileSync() {

    }

    @Override
    protected void beforeFileSync() {

    }

    protected String remoteUri() {
        return "ftp://" + username + ":" + password + "@" + host + ":" + port + "/" + remotePath;
    }

    @Override
    public void addFsOptions() throws FileSystemException {

    }
}
