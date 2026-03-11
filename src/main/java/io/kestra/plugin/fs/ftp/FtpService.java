package io.kestra.plugin.fs.ftp;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.time.Duration;

public abstract class FtpService {
    public static FileSystemOptions fsOptions(RunContext runContext, FtpInterface ftpInterface) throws IOException, IllegalVariableEvaluationException {
        FtpFileSystemConfigBuilder instance = FtpFileSystemConfigBuilder.getInstance();

        return FtpService.fsOptions(instance, runContext, ftpInterface);
    }

    public static FileSystemOptions fsOptions(FtpFileSystemConfigBuilder instance, RunContext runContext, FtpInterface ftpInterface) throws IllegalVariableEvaluationException {
        FileSystemOptions options = new FileSystemOptions();

        instance.setUserDirIsRoot(options, runContext.render(ftpInterface.getRootDir()).as(Boolean.class).orElse(false));

        if (ftpInterface.getProxyType() != null && ftpInterface.getProxyHost() != null) {
            instance.setProxy(
                options,
                new Proxy(
                    runContext.render(ftpInterface.getProxyType()).as(Proxy.Type.class).orElseThrow(),
                    new InetSocketAddress(
                        runContext.render(ftpInterface.getProxyHost()).as(String.class).orElseThrow(),
                        Integer.parseInt(runContext.render(ftpInterface.getProxyPort()).as(String.class).orElseThrow())
                    )
                )
            );

        }

        if (ftpInterface.getPassiveMode() != null) {
            instance.setPassiveMode(options, runContext.render(ftpInterface.getPassiveMode()).as(Boolean.class).orElseThrow());
        }

        if (ftpInterface.getRemoteIpVerification() != null) {
            instance.setRemoteVerification(options, runContext.render(ftpInterface.getRemoteIpVerification()).as(Boolean.class).orElseThrow());
        }

        if (ftpInterface.getOptions() != null) {
            if (ftpInterface.getOptions().getConnectionTimeout() != null) {
                instance.setConnectTimeout(options, runContext.render(ftpInterface.getOptions().getConnectionTimeout()).as(Duration.class).orElseThrow());
            }

            if (ftpInterface.getOptions().getDataTimeout() != null) {
                instance.setDataTimeout(options, runContext.render(ftpInterface.getOptions().getDataTimeout()).as(Duration.class).orElseThrow());
            }

            if (ftpInterface.getOptions().getSocketTimeout() != null) {
                instance.setSoTimeout(options, runContext.render(ftpInterface.getOptions().getSocketTimeout()).as(Duration.class).orElseThrow());
            }

            if (ftpInterface.getOptions().getControlKeepAliveTimeout() != null) {
                instance.setControlKeepAliveTimeout(options, runContext.render(ftpInterface.getOptions().getControlKeepAliveTimeout()).as(Duration.class).orElseThrow());
            }

            if (ftpInterface.getOptions().getControlKeepAliveReplyTimeout() != null) {
                instance.setControlKeepAliveReplyTimeout(options, runContext.render(ftpInterface.getOptions().getControlKeepAliveReplyTimeout()).as(Duration.class).orElseThrow());
            }
        }

        return options;
    }
}
