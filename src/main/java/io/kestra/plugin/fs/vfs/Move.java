package io.kestra.plugin.fs.vfs;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;

import java.net.URI;

import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class Move extends AbstractVfsTask implements RunnableTask<Move.Output> {
    @Schema(
        title = "The file or directory to move from remote server."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String from;

    @Schema(
        title = "The path to move the file or directory to on the remote server.",
        description = "The full destination path (with filename optionally)\n" +
            "If end with a `/`, the destination is considered as a directory and filename will be happen\n" +
            "If the destFile exists, it is deleted first."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String to;

    public Output run(RunContext runContext) throws Exception {
        try (StandardFileSystemManager fsm = new KestraStandardFileSystemManager(runContext)) {
            fsm.setConfiguration(StandardFileSystemManager.class.getResource(KestraStandardFileSystemManager.CONFIG_RESOURCE));
            fsm.init();

            return VfsService.move(
                runContext,
                fsm,
                this.fsOptions(runContext),
                this.uri(runContext, this.from),
                this.uri(runContext, this.to)
            );
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The from uri"
        )
        private final URI from;

        @Schema(
            title = "The destination uri"
        )
        private final URI to;
    }
}
