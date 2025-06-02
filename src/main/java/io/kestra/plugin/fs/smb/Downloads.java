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
    title = "Download multiple files from an SMB (e.g., Samba) server."
)
@Plugin(
    examples = {
        @Example(
            title = "Download files from `my_share` and move them to an `archive_share`",
            full = true,
            code = """
                id: fs_smb_downloads
                namespace: company.team

                tasks:
                  - id: downloads
                    type: io.kestra.plugin.fs.smb.Downloads
                    host: localhost
                    port: "445"
                    username: foo
                    password: pass
                    from: "/my_share/"
                    interval: PT10S
                    action: MOVE
                    moveDirectory: "/archive_share/"
                """
        )
    }
)
public class Downloads extends io.kestra.plugin.fs.vfs.Downloads implements SmbInterface {
    @Builder.Default
    protected Property<String> port = Property.ofValue("445");

    @Override
    protected FileSystemOptions fsOptions(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        return SmbService.fsOptions(runContext, this);
    }

    @Override
    protected String scheme() {
        return "smb";
    }
}
