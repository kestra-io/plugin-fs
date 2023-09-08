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
public abstract class Download extends AbstractVfsTask implements RunnableTask<Download.Output> {
    @Schema(
        title = "The fully-qualified URIs that point to destination path"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    protected String from;

    public Output run(RunContext runContext) throws Exception {
        try (StandardFileSystemManager fsm = new StandardFileSystemManager()) {
            fsm.init();

            return VfsService.download(
                runContext,
                fsm,
                this.fsOptions(runContext),
                this.uri(runContext, this.from)
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
