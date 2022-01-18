package io.kestra.plugin.fs.ftps;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.fs.ftp.FtpInterface;
import io.kestra.plugin.fs.ftp.FtpService;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.ftps.FtpsFileSystemConfigBuilder;

import java.io.IOException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

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

        if (ftpsInterface.getInsecureTrustAllCertificates() != null && ftpsInterface.getInsecureTrustAllCertificates()) {
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
