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
    title = "Trigger a flow as soon as new files are detected in a given FTP server's directory."
)
@Plugin(
    examples = {
        @Example(
            title = "Wait for one or more files in a given FTP server's directory and process each of these files sequentially.",
            full = true,
            code = {
                "id: ftp_trigger_flow",
                "namespace: dev",
                "",
                "tasks:",
                "  - id: for_each_file",
                "    type: io.kestra.core.tasks.flows.EachSequential",
                "    value: \"{{ trigger.files | jq('.path') }}\"",
                "    tasks:",
                "      - id: return",
                "        type: io.kestra.core.tasks.debugs.Return",
                "        format: \"{{ taskrun.value }}\"",
                "",
                "triggers:",
                "  - id: watch",
                "    type: io.kestra.plugin.fs.ftp.Trigger",
                "    host: localhost",
                "    port: 21",
                "    username: foo",
                "    password: bar",
                "    from: \"/in/\"",
                "    interval: PT10S",
                "    action: MOVE",
                "    moveDirectory: \"/archive/\"",
            }
        ),
        @Example(
            title = "Wait for one or more files in a given FTP server's directory and process each of these files sequentially. Delete files manually after processing to prevent infinite triggering.",
            full = true,
            code = {
                "id: ftp_trigger_flow",
                "namespace: dev",
                "",
                "tasks:",
                "  - id: for_each_file",
                "    type: io.kestra.core.tasks.flows.EachSequential",
                "    value: \"{{ trigger.files | jq('.name') }}\"",
                "    tasks:",
                "      - id: return",
                "        type: io.kestra.core.tasks.debugs.Return",
                "        format: \"{{ taskrun.value }}\"",
                "      - id: delete",
                "        type: io.kestra.plugin.fs.ftp.Delete",
                "        host: localhost",
                "        port: 21",
                "        username: foo",
                "        password: bar",
                "        uri: \"/in/{{ taskrun.value }}\"",
                "",
                "triggers:",
                "  - id: watch",
                "    type: io.kestra.plugin.fs.ftp.Trigger",
                "    host: localhost",
                "    port: 21",
                "    username: foo",
                "    password: bar",
                "    from: \"/in/\"",
                "    interval: PT10S",
                "    action: NONE",
            }
        ),
        @Example(
            title = "Wait for one or more files in a given FTP server's directory and process each of these files sequentially. In this example, we restrict the trigger to only wait for CSV files in the `mydir` directory.",
            full = true,
            code = """
                id: ftp_wait_for_csv_in_mydir
                namespace: dev

                tasks:
                  - id: each
                    type: io.kestra.core.tasks.flows.EachSequential
                    value: "{{ trigger.files | jq('.path') }}"
                    tasks:
                      - id: return
                        type: io.kestra.core.tasks.debugs.Return
                        format: "{{ taskrun.value }}"
                    
                triggers:
                  - id: watch
                    type: io.kestra.plugin.fs.ftp.Trigger
                    host: localhost
                    port: "21"
                    username: foo
                    password: bar
                    from: "mydir/"
                    regExp: ".*.csv"
                    action: MOVE
                    moveDirectory: "archive/"
                    interval: PTS"""
        )        
    }
)
public class Trigger extends io.kestra.plugin.fs.vfs.Trigger implements FtpInterface {
    protected String proxyHost;
    protected String proxyPort;
    protected Proxy.Type proxyType;
    @Builder.Default
    protected Boolean rootDir = true;
    @Builder.Default
    protected String port = "21";
    @Builder.Default
    protected Boolean passiveMode = true;
    @Builder.Default
    protected Boolean remoteIpVerification = true;

    @Override
    protected FileSystemOptions fsOptions(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        return FtpService.fsOptions(runContext, this);
    }

    @Override
    protected String scheme() {
        return "ftp";
    }
}
