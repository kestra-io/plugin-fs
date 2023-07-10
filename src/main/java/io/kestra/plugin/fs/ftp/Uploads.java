package io.kestra.plugin.fs.ftp;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
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
        title = "Upload files to a FTP server's directory"
)
@Plugin(
        examples = {
                @Example(
                        code = {
                                "host: localhost",
                                "port: 21",
                                "username: foo",
                                "password: pass",
                                "from:",
                                "  - \"{{ outputs.taskid1.uri }}\"",
                                "  - \"{{ outputs.taskid2.uri }}\"",
                                "to: \"/upload/dir2\"",
                        }
                )
        }
)
public class Uploads extends io.kestra.plugin.fs.vfs.Uploads implements FtpInterface {
    private String proxyHost;
    private String proxyPort;
    private Proxy.Type proxyType;
    @Builder.Default
    private Boolean rootDir = true;
    @Builder.Default
    private String port = "21";
    @Builder.Default
    private Boolean passiveMode = true;
    @Builder.Default
    private Boolean remoteIpVerification = true;

    @Override
    protected FileSystemOptions fsOptions(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        return FtpService.fsOptions(runContext, this);
    }

    @Override
    protected String scheme() {
        return "ftp";
    }
}
