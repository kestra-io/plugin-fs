package org.kestra.task.fs.sftp;

import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.sftp.IdentityInfo;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.kestra.task.fs.VfsTask;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class SftpOptions {
    private VfsTask sftp;
    private File tempFile;

    public SftpOptions(VfsTask sftp) {
        this.sftp = sftp;
    }

    public void addFsOptions() throws IOException {
        FileSystemOptions options = sftp.getOptions();
        SftpFileSystemConfigBuilder.getInstance().setStrictHostKeyChecking(options, "no");
        SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(options, true);
        SftpFileSystemConfigBuilder.getInstance().setSessionTimeoutMillis(options, 10000);

        String keyPath = sftp.getAuth().getKeyfile();
        if (keyPath != null) {
            tempFile = File.createTempFile("sftp-key-", "");
            FileWriter myWriter = new FileWriter(tempFile);
            myWriter.write(keyPath);
            myWriter.close();

            IdentityInfo identityInfo = null;
            if (sftp.getPassPhrase() != null) {
                identityInfo = new IdentityInfo(tempFile, sftp.getPassPhrase().getBytes());
            } else {
                identityInfo = new IdentityInfo(tempFile);
            }
            SftpFileSystemConfigBuilder.getInstance().setIdentityProvider(options, identityInfo);
        }
    }

    public void cleanup() {
        if (tempFile != null) {
            tempFile.delete();
        }
    }
}
