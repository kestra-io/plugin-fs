package io.kestra.plugin.fs.vfs;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
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
    @PluginProperty(dynamic = true)
    private String from;

    @Schema(
        title = "The destination path"
    )
    @PluginProperty(dynamic = true)
    private String to;

    public Upload.Output run(RunContext runContext) throws Exception {
        try (StandardFileSystemManager fsm = new StandardFileSystemManager()) {
            fsm.init();

            return VfsService.upload(
                runContext,
                fsm,
                this.fsOptions(runContext),
                URI.create(runContext.render(this.from)),
                this.uri(runContext, this.to)
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
