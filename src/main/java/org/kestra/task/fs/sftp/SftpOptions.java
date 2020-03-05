package org.kestra.task.fs.sftp;

import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.sftp.IdentityInfo;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.kestra.task.fs.VfsTask;

import java.io.File;
import java.io.IOException;

public class SftpOptions {
    private VfsTask sftp;

    public SftpOptions(VfsTask sftp) {
        this.sftp = sftp;
    }

    public void addFsOptions() throws IOException {
        FileSystemOptions options = sftp.getOptions();
        SftpFileSystemConfigBuilder.getInstance().setStrictHostKeyChecking(options, "no");
        SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(options, true);
        SftpFileSystemConfigBuilder.getInstance().setSessionTimeoutMillis(options, 10000);


//        File tempFile = File.createTempFile("prefix-", "-suffix");
//
//        tempFile.delete();


        String keyPath = sftp.getAuth().getKeyPath();
        if (keyPath != null) {
            IdentityInfo identityInfo = null;
            if (sftp.getPassPhrase() != null) {
                identityInfo = new IdentityInfo(new File(keyPath), sftp.getPassPhrase().getBytes());
            } else {
                identityInfo = new IdentityInfo(new File(keyPath));
            }
            SftpFileSystemConfigBuilder.getInstance().setIdentityProvider(options, identityInfo);
        }
    }
}
