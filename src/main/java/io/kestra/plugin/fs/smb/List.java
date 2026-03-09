package io.kestra.plugin.fs.smb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.fs.vfs.models.File;
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
    title = "List files on an SMB share",
    description = "Lists entries under the given share path with optional regexp filter. Default port 445."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: fs_smb_list
                namespace: company.team

                tasks:
                  - id: list
                    type: io.kestra.plugin.fs.smb.List
                    host: localhost
                    port: "445"
                    username: foo
                    password: "{{ secret('SMB_PASSWORD') }}"
                    from: "/my_share/dir1/"
                    regExp: ".*\\/dir1\\/.*.(yaml|yml)"
                """
        )
    }
)
public class List extends AbstractSmbTask implements RunnableTask<io.kestra.plugin.fs.vfs.List.Output> {
    @Schema(
        title = "Directory URI to list"
    )
    @NotNull
    protected Property<String> from;

    @Schema(
        title = "Regexp filter on full path"
    )
    private Property<String> regExp;

    @Schema(
        title = "List files recursively"
    )
    @Builder.Default
    private Property<Boolean> recursive = Property.ofValue(false);

    @Builder.Default
    @Schema(
        title = "Maximum files to retrieve"
    )
    private Property<Integer> maxFiles = Property.ofValue(25);

    public io.kestra.plugin.fs.vfs.List.Output run(RunContext runContext) throws Exception {
        var ctx = createContext(runContext);
        var output = SmbService.list(
            runContext,
            ctx,
            this,
            runContext.render(this.from).as(String.class).orElseThrow(),
            runContext.render(this.regExp).as(String.class).orElse(null),
            runContext.render(this.recursive).as(Boolean.class).orElse(false)
        );

        var files = output.getFiles();

        int rMaxFiles = runContext.render(this.maxFiles).as(Integer.class).orElse(25);
        if (files.size() > rMaxFiles) {
            runContext.logger().warn("Too many files to process ({}), limiting to {}", files.size(), rMaxFiles);
            files = files.subList(0, rMaxFiles);
        }

        return io.kestra.plugin.fs.vfs.List.Output.builder()
            .files(files)
            .build();
    }
}
