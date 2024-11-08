package io.kestra.plugin.fs.ftp;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.FileSystemOptions;

import java.io.IOException;
import java.net.Proxy;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Download multiple files from FTP server"
)
@Plugin(
    examples = {
        @Example(
            title = "Download a list of files and move it to an archive folders",
            full = true,
            code = """
                id: fs_ftp_downloads
                namespace: company.team

                tasks:
                  - id: downloads
                    type: io.kestra.plugin.fs.ftp.Downloads
                    host: localhost
                    port: 21
                    username: foo
                    password: pass
                    from: "/in/"
                    interval: PT10S
                    action: MOVE
                    moveDirectory: "/archive/"
                """
        )
    }
)
public class Downloads extends io.kestra.plugin.fs.vfs.Downloads implements FtpInterface {
    protected Property<String> proxyHost;
    protected Property<String> proxyPort;
    protected Property<Proxy.Type> proxyType;
    @Builder.Default
    protected Property<Boolean> rootDir = Property.of(true);
    @Builder.Default
    protected Property<String> port = Property.of("21");
    @Builder.Default
    protected Property<Boolean> passiveMode = Property.of(true);
    @Builder.Default
    protected Property<Boolean> remoteIpVerification = Property.of(true);

    @Override
    protected FileSystemOptions fsOptions(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        return FtpService.fsOptions(runContext, this);
    }

    @Override
    protected String scheme() {
        return "ftp";
    }
}
