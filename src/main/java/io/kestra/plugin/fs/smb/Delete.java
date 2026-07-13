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
import io.kestra.core.models.annotations.PluginProperty;

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
            title = "Delete a single file",
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
        ),
        @Example(
            title = "Delete files matching a pattern",
            full = true,
            code = """
                id: fs_smb_delete_regexp
                namespace: company.team

                tasks:
                  - id: delete
                    type: io.kestra.plugin.fs.smb.Delete
                    host: localhost
                    port: "445"
                    username: foo
                    password: "{{ secret('SMB_PASSWORD') }}"
                    uri: "/my_share/dir1/"
                    regExp: '.*\\.csv'
                """
        )
    }
)
public class Delete extends AbstractSmbTask implements RunnableTask<io.kestra.plugin.fs.vfs.Delete.Output> {
    @Schema(
        title = "URI of the file to delete"
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> uri;

    @Schema(
        title = "Raise error if missing"
    )
    @Builder.Default
    @PluginProperty(group = "reliability")
    private final Property<Boolean> errorOnMissing = Property.ofValue(false);

    @Schema(
        title = "A regular expression to filter files for deletion",
        description = """
            When set, `uri` must point to a directory. The pattern is matched against the share-relative path \
            of each file (for example, `/share_name/dir/file.csv`). Only regular files are deleted; directories \
            are skipped.
            """
    )
    @PluginProperty(group = "advanced")
    private Property<String> regExp;

    @Schema(
        title = "Include subdirectories",
        description = "If true, traverses subdirectories when deleting files matching `regExp`."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> recursive = Property.ofValue(false);

    public io.kestra.plugin.fs.vfs.Delete.Output run(RunContext runContext) throws Exception {
        var cifsContext = createContext(runContext);
        try {
            return SmbService.delete(
                runContext,
                cifsContext,
                this,
                runContext.render(this.uri).as(String.class).orElseThrow(),
                runContext.render(this.errorOnMissing).as(Boolean.class).orElse(false),
                runContext.render(this.regExp).as(String.class).orElse(null),
                runContext.render(this.recursive).as(Boolean.class).orElse(false)
            );
        } finally {
            cifsContext.close();
        }
    }
}
