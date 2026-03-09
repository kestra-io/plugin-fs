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

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Move or rename a file over SMB",
    description = "Moves/renames a file on an SMB/CIFS share; destination directories are created when missing. Default port 445."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: fs_smb_move
                namespace: company.team

                tasks:
                  - id: move
                    type: io.kestra.plugin.fs.smb.Move
                    host: localhost
                    port: "445"
                    username: foo
                    password: "{{ secret('SMB_PASSWORD') }}"
                    from: "/my_share/dir1/file.txt"
                    to: "/my_share/dir2/file.txt"
                """
        )
    }
)
public class Move extends AbstractSmbTask implements RunnableTask<io.kestra.plugin.fs.vfs.Move.Output> {
    @Schema(
        title = "Source file or directory URI"
    )
    @NotNull
    private Property<String> from;

    @Schema(
        title = "Destination URI",
        description = "Full target path. If it ends with `/`, the source name is kept. Existing targets are replaced when `overwrite` is true."
    )
    @NotNull
    private Property<String> to;

    @Schema(
        title = "Overwrite existing files",
        description = "If false (default), fails when the destination already exists."
    )
    @Builder.Default
    protected Property<Boolean> overwrite = Property.ofValue(false);

    public io.kestra.plugin.fs.vfs.Move.Output run(RunContext runContext) throws Exception {
        var ctx = createContext(runContext);
        return SmbService.move(
            runContext,
            ctx,
            this,
            runContext.render(this.from).as(String.class).orElseThrow(),
            runContext.render(this.to).as(String.class).orElseThrow(),
            runContext.render(this.overwrite).as(Boolean.class).orElseThrow()
        );
    }
}
