package io.kestra.plugin.fs.ftps;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.fs.ftp.FtpInterface;
import io.kestra.plugin.fs.ftp.FtpService;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.ftps.FtpsDataChannelProtectionLevel;
import org.apache.commons.vfs2.provider.ftps.FtpsFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.ftps.FtpsMode;

import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.security.cert.X509Certificate;

public abstract class FtpsService {
    public static FileSystemOptions fsOptions(RunContext runContext, FtpInterface ftpInterface, FtpsInterface ftpsInterface) throws IOException, IllegalVariableEvaluationException {
        FtpsFileSystemConfigBuilder instance = FtpsFileSystemConfigBuilder.getInstance();

        FileSystemOptions options = FtpService.fsOptions(instance, runContext, ftpInterface);

        if (ftpsInterface.getMode() != null) {
            instance.setFtpsMode(options, runContext.render(ftpsInterface.getMode()).as(FtpsMode.class).orElseThrow());
        }

        if (ftpsInterface.getDataChannelProtectionLevel() != null) {
            instance.setDataChannelProtectionLevel(options, runContext.render(ftpsInterface.getDataChannelProtectionLevel()).as(FtpsDataChannelProtectionLevel.class).orElseThrow());
        }

        // instance.setKeyManager(options, null);

        if (runContext.render(ftpsInterface.getInsecureTrustAllCertificates()).as(Boolean.class).orElse(false)) {
            instance.setTrustManager(options, new X509TrustManager() {
                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }

                public void checkClientTrusted(X509Certificate[] certs, String authType) {
                }

                public void checkServerTrusted(X509Certificate[] certs, String authType) {
                }
            });
        }

        return options;
    }
}
