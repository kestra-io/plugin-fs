package io.kestra.plugin.fs.sftp;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.sftp.IdentityInfo;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public abstract class SftpService {
    public static FileSystemOptions fsOptions(RunContext runContext, SftpInterface sftpInterface) throws IOException, IllegalVariableEvaluationException {
        SftpFileSystemConfigBuilder instance = SftpFileSystemConfigBuilder.getInstance();

        FileSystemOptions options = new FileSystemOptions();

        instance.setStrictHostKeyChecking(options, "no");
        instance.setUserDirIsRoot(options, sftpInterface.getRootDir());

        instance.setSessionTimeoutMillis(options, 10000);
        // see https://issues.apache.org/jira/browse/VFS-766
        instance.setDisableDetectExecChannel(options, true);

        if (sftpInterface.getProxyType() != null && sftpInterface.getProxyHost() != null) {
            if (sftpInterface.getProxyHost() != null) {
                instance.setProxyHost(options, runContext.render(sftpInterface.getProxyHost()));
            }

            if (sftpInterface.getProxyPassword() != null) {
                instance.setProxyPassword(options, runContext.render(sftpInterface.getProxyPassword()));
            }

            if (sftpInterface.getProxyPort() != null) {
                instance.setProxyPort(options, Integer.parseInt(runContext.render(sftpInterface.getProxyPort())));
            }

            if (sftpInterface.getProxyUser() != null) {
                instance.setProxyUser(options, runContext.render(sftpInterface.getProxyUser()));
            }

            if (sftpInterface.getProxyType() != null) {
                switch (sftpInterface.getProxyType()) {
                    case "SOCKS5":
                        instance.setProxyType(options, SftpFileSystemConfigBuilder.PROXY_SOCKS5);
                        break;
                    case "STREAM":
                        instance.setProxyType(options, SftpFileSystemConfigBuilder.PROXY_STREAM);
                        break;
                    case "HTTP":
                        instance.setProxyType(options, SftpFileSystemConfigBuilder.PROXY_HTTP);
                        break;
                }
            }
        }

        if (sftpInterface.getKeyfile() != null) {
            File sftpKey = runContext.tempFile(
                runContext.render(sftpInterface.getKeyfile()).getBytes(StandardCharsets.UTF_8)
            ).toFile();

            IdentityInfo identityInfo;
            if (sftpInterface.getPassphrase() != null) {
                identityInfo = new IdentityInfo(sftpKey, runContext.render(sftpInterface.getPassphrase()).getBytes());
            } else {
                identityInfo = new IdentityInfo(sftpKey);
            }

            instance.setIdentityProvider(options, identityInfo);
        }

        return options;
    }
}
