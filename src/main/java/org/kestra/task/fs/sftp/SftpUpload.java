package org.kestra.task.fs.sftp;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.kestra.core.models.annotations.Documentation;
import org.kestra.task.fs.VfsUpload;

import java.io.IOException;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Documentation(
    description = "Upload file from local FS to SFTP",
    body = "This task reads on local file system a file that will be uploaded to the target SFTP with given inputs"
)
public class SftpUpload extends VfsUpload {
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
