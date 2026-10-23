package io.kestra.plugin.fs.vfs;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;

import java.net.URI;
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class Delete extends AbstractVfsTask implements RunnableTask<Delete.Output> {
    @Schema(
        title = "URI of the file or directory to delete")
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
        title = "Include subdirectories",
        description = "If true, deletes directory contents recursively."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private final Property<Boolean> recursive = Property.ofValue(false);

    @Schema(
        title = "A regular expression to filter files for deletion",
        description = """
            When set, `uri` must point to a directory. The pattern is matched against the path component of \
            the remote URI (for example, `/remote/dir/file.csv`). Only regular files are deleted; directories \
            are skipped.
            """
    )
    @PluginProperty(group = "advanced")
    private Property<String> regExp;

    public Output run(RunContext runContext) throws Exception {
        try (var fsm = new KestraStandardFileSystemManager(runContext)) {
            fsm.setConfiguration(StandardFileSystemManager.class.getResource(KestraStandardFileSystemManager.CONFIG_RESOURCE));
            fsm.init();

            return VfsService.delete(
                runContext,
                fsm,
                this.fsOptions(runContext),
                this.uri(runContext, runContext.render(this.uri).as(String.class).orElseThrow()),
                runContext.render(this.errorOnMissing).as(Boolean.class).orElse(false),
                runContext.render(this.recursive).as(Boolean.class).orElse(false),
                runContext.render(this.regExp).as(String.class).orElse(null)
            );
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The deleted URI"
        )
        private final URI uri;

        @Schema(
            title = "Whether the file or directory was deleted"
        )
        private final boolean deleted;

        @Schema(
            title = "URIs of all deleted files when regExp is used"
        )
        private final java.util.List<URI> uris;
    }
}
