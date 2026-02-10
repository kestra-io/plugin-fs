package io.kestra.plugin.fs.vfs;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;

import java.net.URI;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class Upload extends AbstractVfsTask implements RunnableTask<Upload.Output> {
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

    public Upload.Output run(RunContext runContext) throws Exception {
        try (StandardFileSystemManager fsm = new KestraStandardFileSystemManager(runContext)) {
            fsm.setConfiguration(StandardFileSystemManager.class.getResource(KestraStandardFileSystemManager.CONFIG_RESOURCE));
            fsm.init();

            var renderedFrom = runContext.render(this.from).as(String.class).orElseThrow();
            if (!renderedFrom.startsWith("kestra://")) {
                throw new IllegalArgumentException("'from' must be a Kestra's internal storage URI");
            }
            var renderedTo = runContext.render(this.to).as(String.class).orElse(renderedFrom.substring(renderedFrom.lastIndexOf('/')));
            return VfsService.upload(
                runContext,
                fsm,
                this.fsOptions(runContext),
                URI.create(renderedFrom),
                this.uri(runContext, renderedTo),
                runContext.render(this.overwrite).as(Boolean.class).orElseThrow()
            );
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The fully-qualified URIs that point to source data"
        )
        private URI from;

        @Schema(
            title = "The fully-qualified URIs that point to destination path"
        )
        private URI to;
    }
}
