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
public abstract class Move extends AbstractVfsTask implements RunnableTask<Move.Output> {
    @Schema(
        title = "The file or directory to move from remote server."
    )
    @NotNull
    private Property<String> from;

    @Schema(
        title = "The path to move the file or directory to on the remote server.",
        description = "The full destination path (with filename optionally)\n" +
            "If end with a `/`, the destination is considered as a directory and filename will be happen\n" +
            "If the destFile exists, it is deleted first."
    )
    @NotNull
    private Property<String> to;

    @Schema(
        title = "Overwrite.",
        description = "If set to false, it will raise an exception if the destination folder or file already exists."
    )
    @Builder.Default
    protected Property<Boolean> overwrite = Property.ofValue(false);

    public Output run(RunContext runContext) throws Exception {
        try (StandardFileSystemManager fsm = new KestraStandardFileSystemManager(runContext)) {
            fsm.setConfiguration(StandardFileSystemManager.class.getResource(KestraStandardFileSystemManager.CONFIG_RESOURCE));
            fsm.init();

            return VfsService.move(
                runContext,
                fsm,
                this.fsOptions(runContext),
                this.uri(runContext, runContext.render(this.from).as(String.class).orElseThrow()),
                this.uri(runContext, runContext.render(this.to).as(String.class).orElseThrow()),
                runContext.render(this.overwrite).as(Boolean.class).orElseThrow()
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
