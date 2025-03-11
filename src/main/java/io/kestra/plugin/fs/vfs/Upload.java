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

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class Upload extends AbstractVfsTask implements RunnableTask<Upload.Output> {
    @Schema(
        title = "The file to copy, must be an internal storage URI"
    )
    @NotNull
    private Property<String> from;

    @Schema(
        title = "The destination path, if not set it will use the name of the file denoted by the `from` property"
    )
    private Property<String> to;

    @Schema(
        title = "Overwrite.",
        description = "If set to false, it will raise an exception if the destination folder or file already exists."
    )
    @Builder.Default
    private Property<Boolean> overwrite = Property.of(false);

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
