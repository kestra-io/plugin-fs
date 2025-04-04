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
    title = "Download multiple files from FTPS server"
)
@Plugin(
    examples = {
        @Example(
            title = "Download a list of files and move it to an archive folders",
            full = true,
            code = """
                id: fs_ftps_downloads
                namespace: company.team

                tasks:
                  - id: downloads
                    type: io.kestra.plugin.fs.ftps.Downloads
                    host: localhost
                    port: 990
                    username: foo
                    password: "{{ secret('FTPS_PASSWORD') }}"
                    from: "/in/"
                    interval: PT10S
                    action: MOVE
                    moveDirectory: "/archive/"
                """
        )
    }
)
public class Downloads extends io.kestra.plugin.fs.vfs.Downloads implements FtpInterface, FtpsInterface {
    protected Property<String> proxyHost;
    protected Property<String> proxyPort;
    protected Property<Proxy.Type> proxyType;
    @Builder.Default
    protected Property<Boolean> rootDir = Property.of(true);
    @Builder.Default
    protected Property<String> port = Property.of("990");
    @Builder.Default
    protected Property<Boolean> passiveMode = Property.of(true);
    @Builder.Default
    protected Property<Boolean> remoteIpVerification = Property.of(true);
    @Builder.Default
    protected Options options = Options.builder().build();

    @Builder.Default
    protected Property<FtpsMode> mode = Property.of(FtpsMode.EXPLICIT);
    @Builder.Default
    protected Property<FtpsDataChannelProtectionLevel> dataChannelProtectionLevel = Property.of(FtpsDataChannelProtectionLevel.P);
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
