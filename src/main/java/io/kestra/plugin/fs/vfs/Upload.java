package io.kestra.plugin.fs.vfs;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;

import java.net.URI;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class Upload extends AbstractVfsTask implements RunnableTask<Upload.Output> {
    @Schema(
        title = "The file to copy, must be an internal storage URI"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String from;

    @Schema(
        title = "The destination path, if not set it will use the name of the file denoted by the `from` property"
    )
    @PluginProperty(dynamic = true)
    private String to;

    public Upload.Output run(RunContext runContext) throws Exception {
        try (StandardFileSystemManager fsm = new StandardFileSystemManager()) {
            fsm.init();

            var renderedFrom = runContext.render(this.from);
            if (!renderedFrom.startsWith("kestra://")) {
                throw new IllegalArgumentException("'from' must be a Kestra's internal storage URI");
            }
            var renderedTo = this.to != null ? runContext.render(this.to) : renderedFrom.substring(renderedFrom.lastIndexOf('/'));
            return VfsService.upload(
                runContext,
                fsm,
                this.fsOptions(runContext),
                URI.create(renderedFrom),
                this.uri(runContext, renderedTo)
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
