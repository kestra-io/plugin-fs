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
    title = "Trigger a flow as soon as new files are detected in a given FTPS server's directory."
)
@Plugin(
    examples = {
        @Example(
            title = "Wait for one or more files in a given FTPS server's directory and process each of these files sequentially.",
            full = true,
            code = """
                id: ftps_trigger_flow
                namespace: company.team
                
                tasks:
                  - id: for_each_file
                    type: io.kestra.plugin.core.flow.EachSequential
                    value: "{{ trigger.files }}"
                    tasks:
                      - id: return
                        type: io.kestra.plugin.core.debug.Return
                        format: "{{ taskrun.value | jq('.path') }}"
                
                triggers:
                  - id: watch
                    type: io.kestra.plugin.fs.ftps.Trigger
                    host: localhost
                    port: 990
                    username: foo
                    password: bar
                    from: "/in/"
                    interval: PT10S
                    action: MOVE
                    moveDirectory: "/archive/"
                """
        ),
        @Example(
            title = "Wait for one or more files in a given FTPS server's directory and process each of these files sequentially. In this example, we restrict the trigger to only wait for CSV files in the `mydir` directory.",
            full = true,
            code = """
                id: ftp_wait_for_csv_in_mydir
                namespace: company.team

                tasks:
                  - id: each
                    type: io.kestra.plugin.core.flow.EachSequential
                    value: "{{ trigger.files }}"
                    tasks:
                      - id: return
                        type: io.kestra.plugin.core.debug.Return
                        format: "{{ taskrun.value | jq('.path') }}"
                    
                triggers:
                  - id: watch
                    type: io.kestra.plugin.fs.ftps.Trigger
                    host: localhost
                    port: "21"
                    username: foo
                    password: bar
                    from: "mydir/"
                    regExp: ".*.csv"
                    action: MOVE
                    moveDirectory: "archive/"
                    interval: PTS
                """
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
