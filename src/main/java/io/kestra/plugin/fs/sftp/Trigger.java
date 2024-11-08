package io.kestra.plugin.fs.sftp;

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

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger a flow as soon as new files are detected in a given SFTP server's directory."
)
@Plugin(
    examples = {
        @Example(
            title = "Wait for one or more files in a given SFTP server's directory and process each of these files sequentially.",
            full = true,
            code = """
                id: sftp_trigger_flow
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
                    type: io.kestra.plugin.fs.sftp.Trigger
                    host: localhost
                    port: 6622
                    username: foo
                    password: bar
                    from: "/in/"
                    interval: PT10S
                    action: MOVE
                    moveDirectory: "/archive/"
                """
        ),
        @Example(
            title = "Wait for one or more files in a given SFTP server's directory and process each of these files sequentially. Delete files manually after processing to prevent infinite triggering.",
            full = true,
            code = """
                id: sftp_trigger_flow
                namespace: company.team
                
                tasks:
                  - id: for_each_file
                    type: io.kestra.plugin.core.flow.EachSequential
                    value: "{{ trigger.files | jq('.path') }}"
                    tasks:
                      - id: return
                        type: io.kestra.plugin.core.debug.Return
                        format: "{{ taskrun.value }}"
                      - id: delete
                        type: io.kestra.plugin.fs.sftp.Delete
                        host: localhost
                        port: 6622
                        username: foo
                        password: bar
                        uri: "/in/{{ taskrun.value }}"
                
                triggers:
                  - id: watch
                    type: io.kestra.plugin.fs.sftp.Trigger
                    host: localhost
                    port: 6622
                    username: foo
                    password: bar
                    from: "/in/"
                    interval: PT10S
                    action: NONE
                """
        ),
        @Example(
            title = "Wait for one or more files in a given SFTP server's directory and process each of these files sequentially. In this example, we restrict the trigger to only wait for CSV files in the `mydir` directory.",
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
                    type: io.kestra.plugin.fs.sftp.Trigger
                    host: localhost
                    port: "6622"
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
public class Trigger extends io.kestra.plugin.fs.vfs.Trigger implements SftpInterface {
    protected Property<String> keyfile;
    protected Property<String> passphrase;
    protected Property<String> proxyHost;
    protected Property<String> proxyPort;
    protected Property<String> proxyUser;
    protected Property<String> proxyPassword;
    protected Property<String> proxyType;
    @Builder.Default
    protected Property<Boolean> rootDir = Property.of(true);
    @Builder.Default
    protected Property<String> port = Property.of("22");
    protected Property<String> keyExchangeAlgorithm;

    @Override
    protected FileSystemOptions fsOptions(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        return SftpService.fsOptions(runContext, this);
    }

    @Override
    protected String scheme() {
        return "sftp";
    }
}
