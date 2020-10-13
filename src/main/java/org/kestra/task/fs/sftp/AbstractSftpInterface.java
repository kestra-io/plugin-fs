package org.kestra.task.fs.sftp;

public interface AbstractSftpInterface {
    String getHost();

    String getPort();

    String getUsername();

    String getPassword();

    String getKeyfile();

    String getPassphrase();
}
