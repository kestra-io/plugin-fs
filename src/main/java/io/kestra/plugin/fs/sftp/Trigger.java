package io.kestra.plugin.fs.sftp;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.FileSystemOptions;

import java.io.IOException;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Wait for files on a SFTP server"
)
@Plugin(
    examples = {
        @Example(
            title = "Wait for a list of file on a SFTP server and iterate through the files",
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
                "    value: \"{{ trigger.files | jq('.path') }}\"",
                "",
                "triggers:",
                "  - id: watch",
                "    type: io.kestra.plugin.fs.sftp.Trigger",
                "    host: localhost",
                "    port: 6622",
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
public class Trigger extends io.kestra.plugin.fs.vfs.Trigger implements SftpInterface {
    protected String keyfile;
    protected String passphrase;
    protected String proxyHost;
    protected String proxyPort;
    protected String proxyUser;
    protected String proxyPassword;
    protected String proxyType;
    @Builder.Default
    protected Boolean rootDir = true;
    @Builder.Default
    protected String port = "22";

    @Override
    protected FileSystemOptions fsOptions(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        return SftpService.fsOptions(runContext, this);
    }

    @Override
    protected String scheme() {
        return "sftp";
    }
}
