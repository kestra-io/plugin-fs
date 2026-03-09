package io.kestra.plugin.fs.smb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import org.codelibs.jcifs.smb.CIFSContext;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.net.URI;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Upload a file over SMB",
    description = "Pushes one local file to an SMB/CIFS share. Default port 445."
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
                    password: "{{ secret('SMB_PASSWORD') }}"
                    from: "{{ inputs.file }}"
                    to: "/my_share/dir2/file.txt"
                """
        )
    }
)
public class Upload extends AbstractSmbTask implements RunnableTask<io.kestra.plugin.fs.vfs.Upload.Output> {
    @Schema(
        title = "Source file (kestra:// URI)"
    )
    @NotNull
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    @Schema(
        title = "Destination path",
        description = "If unset, uses the source filename."
    )
    private Property<String> to;

    @Schema(
        title = "Overwrite existing files",
        description = "If false (default), fails when the destination already exists."
    )
    @Builder.Default
    private Property<Boolean> overwrite = Property.ofValue(false);

    public io.kestra.plugin.fs.vfs.Upload.Output run(RunContext runContext) throws Exception {
        CIFSContext ctx = createContext(runContext);
        var renderedFrom = runContext.render(this.from).as(String.class).orElseThrow();
        if (!renderedFrom.startsWith("kestra://")) {
            throw new IllegalArgumentException("'from' must be a Kestra's internal storage URI");
        }
        var renderedTo = runContext.render(this.to).as(String.class).orElse(renderedFrom.substring(renderedFrom.lastIndexOf('/')));
        return SmbService.upload(
            runContext,
            ctx,
            this,
            URI.create(renderedFrom),
            renderedTo,
            runContext.render(this.overwrite).as(Boolean.class).orElseThrow()
        );
    }
}
