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
    title = "Delete a file over SMB",
    description = "Removes the specified file on an SMB/CIFS share. Default port 445."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: fs_smb_delete
                namespace: company.team

                tasks:
                  - id: delete
                    type: io.kestra.plugin.fs.smb.Delete
                    host: localhost
                    port: "445"
                    username: foo
                    password: "{{ secret('SMB_PASSWORD') }}"
                    uri: "/my_share/dir1/file.txt"
                """
        )
    }
)
public class Delete extends AbstractSmbTask implements RunnableTask<io.kestra.plugin.fs.vfs.Delete.Output> {
    @Schema(
        title = "URI of the file to delete"
    )
    @NotNull
    private Property<String> uri;

    @Schema(
        title = "Raise error if missing"
    )
    @Builder.Default
    private final Property<Boolean> errorOnMissing = Property.ofValue(false);

    public io.kestra.plugin.fs.vfs.Delete.Output run(RunContext runContext) throws Exception {
        var ctx = createContext(runContext);
        return SmbService.delete(
            runContext,
            ctx,
            this,
            runContext.render(this.uri).as(String.class).orElseThrow(),
            runContext.render(this.errorOnMissing).as(Boolean.class).orElse(false)
        );
    }
}
