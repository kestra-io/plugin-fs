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
    title = "Wait for files on a FTPS server"
)
@Plugin(
    examples = {
        @Example(
            title = "Wait for a list of file on a FTP server and iterate through the files",
            full = true,
            code = {
                "id: gcs-listen",
                "namespace: io.kestra.tests",
                "",
                "tasks:",
                "  - id: each",
                "    type: io.kestra.core.tasks.flows.EachSequential",
                "    tasks:",
                "      - id: return",
                "        type: io.kestra.core.tasks.debugs.Return",
                "        format: \"{{taskrun.value}}\"",
                "    value: \"{{ trigger.files | jq '.path' }}\"",
                "",
                "triggers:",
                "  - id: watch",
                "    type: io.kestra.plugin.fs.ftp.Trigger",
                "    host: localhost",
                "    port: 990",
                "    username: foo",
                "    password: pass",
                "    from: \"/in/\"",
                "    interval: PT10S",
                "    action: MOVE",
                "    moveDirectory: \"/archive/\"",
            }
        )
    }
)
public class Trigger extends io.kestra.plugin.fs.vfs.Trigger implements FtpInterface, FtpsInterface {
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
    protected FtpsMode mode = FtpsMode.EXPLICIT;
    @Builder.Default
    protected FtpsDataChannelProtectionLevel dataChannelProtectionLevel = FtpsDataChannelProtectionLevel.P;

    @Override
    protected FileSystemOptions fsOptions(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        return FtpsService.fsOptions(runContext, this, this);
    }

    @Override
    protected String scheme() {
        return "ftps";
    }
}
