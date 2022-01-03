package io.kestra.plugin.fs.ftps;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.fs.ftp.FtpInterface;
import io.kestra.plugin.fs.ftp.FtpService;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.ftps.FtpsFileSystemConfigBuilder;

import java.io.IOException;

public abstract class FtpsService {
    public static FileSystemOptions fsOptions(RunContext runContext, FtpInterface ftpInterface, FtpsInterface ftpsInterface) throws IOException, IllegalVariableEvaluationException {
        FtpsFileSystemConfigBuilder instance = FtpsFileSystemConfigBuilder.getInstance();

        FileSystemOptions options = FtpService.fsOptions(instance, runContext, ftpInterface);

        if (ftpsInterface.getMode() != null) {
            instance.setFtpsMode(options, ftpsInterface.getMode());
        }

        if (ftpsInterface.getDataChannelProtectionLevel() != null) {
            instance.setDataChannelProtectionLevel(options, ftpsInterface.getDataChannelProtectionLevel());
        }

        // instance.setKeyManager(options, null);
        // instance.setTrustManager(options, null);

        return options;
    }
}
