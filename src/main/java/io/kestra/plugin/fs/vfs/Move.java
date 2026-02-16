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
        title = "Source file or directory URI"
    )
    @NotNull
    private Property<String> from;

    @Schema(
        title = "Destination URI",
        description = "Full target path. If it ends with `/`, the source name is kept. Existing targets are replaced when `overwrite` is true."
    )
    @NotNull
    private Property<String> to;

    @Schema(
        title = "Overwrite existing files",
        description = "If false (default), fails when the destination already exists."
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
