package io.kestra.plugin.fs.ftps;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
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
    title = "Download a file from FTPS",
    description = "Fetches a single remote file over FTPS. Defaults: port 990, EXPLICIT mode, PROT P data channel, passive mode on, remote IP verification on, paths relative to user home. `insecureTrustAllCertificates` skips SSL checks for testing."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: fs_ftps_download
                namespace: company.team

                tasks:
                  - id: download
                    type: io.kestra.plugin.fs.ftps.Download
                    host: localhost
                    port: 990
                    username: foo
                    password: "{{ secret('FTPS_PASSWORD') }}"
                    from: "/in/file.txt"
                """
        )
    }
)
public class Download extends io.kestra.plugin.fs.vfs.Download implements FtpInterface, FtpsInterface {
    protected Property<String> proxyHost;
    protected Property<String> proxyPort;
    protected Property<Proxy.Type> proxyType;
    @Builder.Default
    protected Property<Boolean> rootDir = Property.ofValue(true);
    @Builder.Default
    protected Property<String> port = Property.ofValue("990");
    @Builder.Default
    protected Property<Boolean> passiveMode = Property.ofValue(true);
    @Builder.Default
    protected Property<Boolean> remoteIpVerification = Property.ofValue(true);
    @Builder.Default
    protected Options options = Options.builder().build();

    @Builder.Default
    protected Property<FtpsMode> mode = Property.ofValue(FtpsMode.EXPLICIT);
    @Builder.Default
    protected Property<FtpsDataChannelProtectionLevel> dataChannelProtectionLevel = Property.ofValue(FtpsDataChannelProtectionLevel.P);
    protected Property<Boolean> insecureTrustAllCertificates;

    @Override
    protected FileSystemOptions fsOptions(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        return FtpsService.fsOptions(runContext, this, this);
    }

    @Override
    protected String scheme() {
        return "ftps";
    }
}
