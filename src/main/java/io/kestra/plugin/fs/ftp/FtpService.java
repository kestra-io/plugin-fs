package io.kestra.plugin.fs.ftp;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;

public abstract class FtpService {
    public static FileSystemOptions fsOptions(RunContext runContext, FtpInterface ftpInterface) throws IOException, IllegalVariableEvaluationException {
        FtpFileSystemConfigBuilder instance = FtpFileSystemConfigBuilder.getInstance();

        return FtpService.fsOptions(instance, runContext, ftpInterface);
    }

    public static FileSystemOptions fsOptions(FtpFileSystemConfigBuilder instance, RunContext runContext, FtpInterface ftpInterface) throws IllegalVariableEvaluationException {
        FileSystemOptions options = new FileSystemOptions();

        instance.setUserDirIsRoot(options, ftpInterface.getRootDir());

        if (ftpInterface.getProxyType() != null && ftpInterface.getProxyHost() != null) {
            instance.setProxy(
                options,
                new Proxy(
                    ftpInterface.getProxyType(),
                    new InetSocketAddress(
                        runContext.render(ftpInterface.getProxyHost()),
                        Integer.parseInt(runContext.render(ftpInterface.getProxyPort()))
                    )
                )
            );

        }

        if (ftpInterface.getPassiveMode() != null) {
            instance.setPassiveMode(options, ftpInterface.getPassiveMode());
        }

        return options;
    }
}
