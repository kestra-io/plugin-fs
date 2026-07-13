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
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "List files on an SMB share",
    description = "Lists entries under the given share path with optional regexp filter. Sorted with `sort` (default `NONE`) before `maxFiles` truncation. Default port 445."
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
                    regExp: '.*/dir1/.*\\.(yaml|yml)'
                    sort: NAME_ASC
                """
        )
    }
)
public class List extends AbstractSmbTask implements RunnableTask<io.kestra.plugin.fs.vfs.List.Output> {
    @Schema(
        title = "Directory URI to list"
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> from;

    @Schema(
        title = "Regexp filter on full path"
    )
    @PluginProperty(group = "processing")
    private Property<String> regExp;

    @Schema(
        title = "List files recursively"
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> recursive = Property.ofValue(false);

    @Builder.Default
    @Schema(
        title = "Maximum files to retrieve"
    )
    @PluginProperty(group = "processing")
    private Property<Integer> maxFiles = Property.ofValue(25);

    // FQCN needed: naming conflict with smb.List. Reuses vfs.List.Sort rather than duplicating the enum,
    // since this class already depends on vfs.List.Output/vfs.models.File.
    @Builder.Default
    @Schema(
        title = "Sort order applied to the list before `maxFiles` truncation",
        description = """
            `NONE` (default) preserves the order returned by the share listing. `LAST_MODIFIED_ASC`/`LAST_MODIFIED_DESC` sort by last modified date, oldest/newest first. `NAME_ASC`/`NAME_DESC` sort alphabetically by file name."""
    )
    @PluginProperty(group = "processing")
    private Property<io.kestra.plugin.fs.vfs.List.Sort> sort = Property.ofValue(io.kestra.plugin.fs.vfs.List.Sort.NONE);

    public io.kestra.plugin.fs.vfs.List.Output run(RunContext runContext) throws Exception {
        var ctx = createContext(runContext);
        try {
            var output = SmbService.list(
                runContext,
                ctx,
                this,
                runContext.render(this.from).as(String.class).orElseThrow(),
                runContext.render(this.regExp).as(String.class).orElse(null),
                runContext.render(this.recursive).as(Boolean.class).orElse(false)
            );

            var files = output.getFiles();

            var rSort = runContext.render(this.sort).as(io.kestra.plugin.fs.vfs.List.Sort.class).orElse(io.kestra.plugin.fs.vfs.List.Sort.NONE);
            var comparator = io.kestra.plugin.fs.vfs.List.comparator(rSort, File::getUpdatedDate, File::getName);
            if (comparator != null) {
                files = files.stream().sorted(comparator).toList();
            }

            int rMaxFiles = runContext.render(this.maxFiles).as(Integer.class).orElse(25);
            if (files.size() > rMaxFiles) {
                runContext.logger().warn("Too many files to process ({}), limiting to {}", files.size(), rMaxFiles);
                files = files.subList(0, rMaxFiles);
            }

            return io.kestra.plugin.fs.vfs.List.Output.builder()
                .files(files)
                .build();
        } finally {
            ctx.close();
        }
    }
}
