package io.kestra.plugin.fs.smb;

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
    title = "Upload a file to an SMB (e.g., Samba) server directory."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: fs_smb_upload
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE

                tasks:
                  - id: upload
                    type: io.kestra.plugin.fs.smb.Upload
                    host: localhost
                    port: "445"
                    username: foo
                    password: pass
                    from: "{{ inputs.file }}"
                    to: "/my_share/dir2/file.txt"
                """
        )
    }
)
public class Upload extends io.kestra.plugin.fs.vfs.Upload implements SmbInterface {
    @Builder.Default
    protected Property<String> port = Property.of("445");

    @Override
    protected FileSystemOptions fsOptions(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        return SmbService.fsOptions(runContext, this);
    }

    @Override
    protected String scheme() {
        return "smb";
    }
}
