package io.kestra.plugin.fs.ftps;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.fs.ftp.FtpInterface;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.ftps.FtpsDataChannelProtectionLevel;
import org.apache.commons.vfs2.provider.ftps.FtpsMode;

import java.io.IOException;
import java.net.Proxy;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Move a file to a FTPS server.",
    description ="If the destination directory doesn't exist, it will be created"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "host: localhost",
                "port: 990",
                "username: foo",
                "password: pass",
                "from: \"/upload/dir1/file.txt\"",
                "to: \"/upload/dir2/file.txt\"",
            }
        )
    }
)
public class Move extends io.kestra.plugin.fs.vfs.Move implements FtpInterface, FtpsInterface {
    protected String proxyHost;
    protected String proxyPort;
    protected Proxy.Type proxyType;
    @Builder.Default
    protected Boolean rootDir = true;
    @Builder.Default
    protected String port = "990";
    @Builder.Default
    protected Boolean passiveMode = true;
    @Builder.Default
    protected Boolean remoteIpVerification = true;

    @Builder.Default
    protected FtpsMode mode = FtpsMode.EXPLICIT;
    @Builder.Default
    protected FtpsDataChannelProtectionLevel dataChannelProtectionLevel = FtpsDataChannelProtectionLevel.P;
    protected Boolean insecureTrustAllCertificates;

    @Override
    protected FileSystemOptions fsOptions(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        return FtpsService.fsOptions(runContext, this, this);
    }

    @Override
    protected String scheme() {
        return "ftps";
    }
}
