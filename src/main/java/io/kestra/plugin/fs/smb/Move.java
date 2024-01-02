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
    title = "Move a file to a different share / folder on a SMB (Samba for eg.) server.",
    description = "If the destination directory doesn't exist, it will be created"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "host: localhost",
                "port: 445",
                "username: foo",
                "password: pass",
                "from: \"/my_share/dir1/file.txt\"",
                "to: \"/my_share/dir2/file.txt\"",
            }
        )
    }
)
public class Move extends io.kestra.plugin.fs.vfs.Move implements SmbInterface {
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
