package io.kestra.plugin.fs.smb;

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
    title = "Trigger a flow as soon as new files are detected in a given SMB (Samba for eg.) server's directory."
)
@Plugin(
    examples = {
        @Example(
            title = """
                Wait for one or more files in a given SMB server's directory and process each of these files sequentially.
                Then move them to another share which is used as an archive.""",
            full = true,
            code = {
                "id: smb_trigger_flow",
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
                "    type: io.kestra.plugin.fs.smb.Trigger",
                "    host: localhost",
                "    port: 445",
                "    username: foo",
                "    password: bar",
                "    from: \"/my_share/in/\"",
                "    interval: PT10S",
                "    action: MOVE",
                "    moveDirectory: \"/archive_share/\"",
            }
        ),
        @Example(
            title = """
                Wait for one or more files in a given SMB server's directory and process each of these files sequentially.
                Then move them to another share which is used as an archive.""",
            full = true,
            code = {
                "id: smb_trigger_flow",
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
                "      - id: delete",
                "        type: io.kestra.plugin.fs.smb.Delete",
                "        host: localhost",
                "        port: 445",
                "        username: foo",
                "        password: bar",
                "        uri: \"/my_share/in/{{ taskrun.value }}\"",
                "",
                "triggers:",
                "  - id: watch",
                "    type: io.kestra.plugin.fs.smb.Trigger",
                "    host: localhost",
                "    port: 445",
                "    username: foo",
                "    password: bar",
                "    from: \"/my_share/in/\"",
                "    interval: PT10S",
                "    action: NONE"
            }
        ),
        @Example(
            title = """
                Wait for one or more files in a given SMB server's directory (composed of share name followed by dir path) and process each of these files sequentially.
                In this example, we restrict the trigger to only wait for CSV files in the `mydir` directory.""",
            full = true,
            code = """
                id: smb_wait_for_csv_in_my_share_my_dir
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
                    type: io.kestra.plugin.fs.smb.Trigger
                    host: localhost
                    port: "445"
                    username: foo
                    password: bar
                    from: "my_share/mydir/"
                    regExp: ".*.csv"
                    action: MOVE
                    moveDirectory: "my_share/archivedir"
                    interval: PTS"""
        )
    }
)
public class Trigger extends io.kestra.plugin.fs.vfs.Trigger implements SmbInterface {
    @Builder.Default
    protected String port = "445";

    @Override
    protected FileSystemOptions fsOptions(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        return SmbService.fsOptions(runContext, this);
    }

    @Override
    protected String scheme() {
        return "smb";
    }
}
