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

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Download file from SMB (Samba for eg.) server"
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: fs_smb_download
                namespace: company.team

                tasks:
                  - id: download
                    type: io.kestra.plugin.fs.smb.Download
                    host: localhost
                    port: "445"
                    username: foo
                    password: "{{ secret('SMB_PASSWORD') }}"
                    from: "/my_share/file.txt"
                """
        )
    }
)
public class Download extends io.kestra.plugin.fs.vfs.Download implements SmbInterface {
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
