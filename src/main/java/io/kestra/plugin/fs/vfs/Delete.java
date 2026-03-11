package io.kestra.plugin.fs.vfs;

import com.jcraft.jsch.JSch;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
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
        title = "URI of the file to delete")
    @NotNull
    private Property<String> uri;

    @Schema(
        title = "Raise error if missing"
    )
    @Builder.Default
    private final Property<Boolean> errorOnMissing = Property.ofValue(false);

    public Output run(RunContext runContext) throws Exception {
        try (var fsm = new KestraStandardFileSystemManager(runContext)) {
            fsm.setConfiguration(StandardFileSystemManager.class.getResource(KestraStandardFileSystemManager.CONFIG_RESOURCE));
            fsm.init();

            return VfsService.delete(
                runContext,
                fsm,
                this.fsOptions(runContext),
                this.uri(runContext, runContext.render(this.uri).as(String.class).orElseThrow()),
                runContext.render(this.errorOnMissing).as(Boolean.class).orElse(false)
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
