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
    title = "Upload files to an FTPS server's directory."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: fs_ftps_uploads
                namespace: company.team

                inputs:
                  - id: file1
                    type: FILE
                  - id: file2
                    type: FILE

                tasks:
                  - id: uploads
                    type: io.kestra.plugin.fs.ftps.Uploads
                    host: localhost
                    port: 990
                    username: foo
                    password: "{{ secret('FTPS_PASSWORD') }}"
                    from:
                      - "{{ inputs.file1 }}"
                      - "{{ inputs.file2 }}"
                    to: "/upload/dir2"
                """
        )
    }
)
public class Uploads extends io.kestra.plugin.fs.vfs.Uploads implements FtpInterface, FtpsInterface {
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
