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
    title = "Trigger on new FTP files",
    description = "Polls a remote directory on the interval and starts a Flow when new files appear. Defaults: port 21, passive mode on, remote IP verification on, paths relative to user home. Use `action` MOVE/DELETE to avoid reprocessing."
)
@Plugin(
    examples = {
        @Example(
            title = "Wait for one or more files in a given FTP server's directory and process each of these files sequentially.",
            full = true,
            code = """
                id: ftp_trigger_flow
                namespace: company.team

                tasks:
                  - id: for_each_file
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.files }}"
                    tasks:
                      - id: return
                        type: io.kestra.plugin.core.debug.Return
                        format: "{{ taskrun.value | jq('.path') }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.fs.ftp.Trigger
                    host: localhost
                    port: 21
                    username: foo
                    password: "{{ secret('FTP_PASSWORD') }}"
                    from: "/in/"
                    interval: PT10S
                    action: MOVE
                    moveDirectory: "/archive/"
                """
        ),
        @Example(
            title = "Wait for one or more files in a given FTP server's directory and process each of these files sequentially. Delete files manually after processing to prevent infinite triggering.",
            full = true,
            code = """
                id: ftp_trigger_flow
                namespace: company.team

                tasks:
                  - id: for_each_file
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.files }}"
                    tasks:
                      - id: return
                        type: io.kestra.plugin.core.debug.Return
                        format: "{{ taskrun.value | jq('.name') }}"
                      - id: delete
                        type: io.kestra.plugin.fs.ftp.Delete
                        host: localhost
                        port: 21
                        username: foo
                        password: "{{ secret('FTP_PASSWORD') }}"
                        uri: "/in/{{ taskrun.value | jq('.name') }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.fs.ftp.Trigger
                    host: localhost
                    port: 21
                    username: foo
                    password: "{{ secret('FTP_PASSWORD') }}"
                    from: "/in/"
                    interval: PT10S
                    action: NONE
                """
        ),
        @Example(
            title = "Wait for one or more files in a given FTP server's directory and process each of these files sequentially. In this example, we restrict the trigger to only wait for CSV files in the `mydir` directory.",
            full = true,
            code = """
                id: ftp_wait_for_csv_in_mydir
                namespace: company.team

                tasks:
                  - id: each
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.files }}"
                    tasks:
                      - id: return
                        type: io.kestra.plugin.core.debug.Return
                        format: "{{ taskrun.value | jq('.path') }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.fs.ftp.Trigger
                    host: localhost
                    port: "21"
                    username: foo
                    password: "{{ secret('FTP_PASSWORD') }}"
                    from: "mydir/"
                    regExp: ".*.csv"
                    action: MOVE
                    moveDirectory: "archive/"
                    interval: PT10S
                """
        )
    }
)
public class Trigger extends io.kestra.plugin.fs.vfs.Trigger implements FtpInterface {
    protected Property<String> proxyHost;
    protected Property<String> proxyPort;
    protected Property<Proxy.Type> proxyType;
    @Builder.Default
    protected Property<Boolean> rootDir = Property.ofValue(true);
    @Builder.Default
    protected Property<String> port = Property.ofValue("21");
    @Builder.Default
    protected Property<Boolean> passiveMode = Property.ofValue(true);
    @Builder.Default
    protected Property<Boolean> remoteIpVerification = Property.ofValue(true);
    @Builder.Default
    protected Options options = Options.builder().build();

    @Override
    protected FileSystemOptions fsOptions(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        return FtpService.fsOptions(runContext, this);
    }

    @Override
    protected String scheme() {
        return "ftp";
    }
}
