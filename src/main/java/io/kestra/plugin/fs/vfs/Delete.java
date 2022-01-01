package io.kestra.plugin.fs.vfs;

import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;

import java.net.URI;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class Delete extends AbstractVfsTask implements RunnableTask<Delete.Output> {
    @Schema(
        title = "The file to delete")
    private String uri;

    @Schema(
        title = "raise an error if the file is not found"
    )
    @Builder.Default
    private final Boolean errorOnMissing = false;

    public Output run(RunContext runContext) throws Exception {
        try (StandardFileSystemManager fsm = new StandardFileSystemManager()) {
            fsm.init();

            return VfsService.delete(
                runContext,
                fsm,
                this.fsOptions(runContext),
                this.uri(runContext, this.uri),
                this.errorOnMissing
            );
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The deleted uri"
        )
        private final URI uri;

        @Schema(
            title = "If the files was really deleted"
        )
        private final boolean deleted;
    }
}
