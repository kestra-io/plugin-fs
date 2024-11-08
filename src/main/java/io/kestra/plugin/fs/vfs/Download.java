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
public abstract class Download extends AbstractVfsTask implements RunnableTask<Download.Output> {
    @Schema(
        title = "The fully-qualified URIs that point to destination path"
    )
    @NotNull
    protected Property<String> from;

    public Output run(RunContext runContext) throws Exception {
        try (StandardFileSystemManager fsm = new KestraStandardFileSystemManager(runContext)) {
            fsm.setConfiguration(StandardFileSystemManager.class.getResource(KestraStandardFileSystemManager.CONFIG_RESOURCE));
            fsm.init();

            return VfsService.download(
                runContext,
                fsm,
                this.fsOptions(runContext),
                this.uri(runContext, runContext.render(this.from).as(String.class).orElseThrow())
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
