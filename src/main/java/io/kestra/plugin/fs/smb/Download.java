package io.kestra.plugin.fs.smb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Download a file over SMB",
    description = "Fetches a single file from an SMB/CIFS share to internal storage. Default port 445."
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
public class Download extends AbstractSmbTask implements RunnableTask<io.kestra.plugin.fs.vfs.Download.Output> {
    @Schema(
        title = "Source URI to download"
    )
    @NotNull
    protected Property<String> from;

    public io.kestra.plugin.fs.vfs.Download.Output run(RunContext runContext) throws Exception {
        var ctx = createContext(runContext);
        return SmbService.download(
            runContext,
            ctx,
            this,
            runContext.render(this.from).as(String.class).orElseThrow()
        );
    }
}
