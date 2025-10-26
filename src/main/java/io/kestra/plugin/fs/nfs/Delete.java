package io.kestra.plugin.fs.nfs;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete a file from an NFS mount."
)
@Plugin(
    examples = {
        @Example(
            title = "Delete a file from an NFS mount.",
            code = {
                "uri: /mnt/nfs/shared/logs/old_log.txt",
                "errorOnMissing: false"
            }
        )
    }
)
public class Delete extends Task implements RunnableTask<Delete.Output> { 

    @Schema(title = "The path to the file to delete.")
    @PluginProperty(dynamic = true)
    @NotNull
    private String uri;

    @Schema(title = "Raise an error if the file doesn't exist.")
    @PluginProperty
    @Builder.Default
    private boolean errorOnMissing = true;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        Path path = NfsService.toNfsPath(runContext.render(this.uri));

        logger.info("Deleting file at {}", path);

        boolean deleted;
        try {
            deleted = Files.deleteIfExists(path);
            if (!deleted && this.errorOnMissing) {
                throw new NoSuchFileException("File not found and 'errorOnMissing' is true: " + path);
            }
        } catch (NoSuchFileException e) {
            if (this.errorOnMissing) {
                throw e;
            }
            deleted = false;
        }

        return Output.builder()
            .uri(path.toUri())
            .deleted(deleted)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The URI of the deleted file.")
        private final URI uri;

        @Schema(title = "Whether the file was successfully deleted.")
        private final boolean deleted;
    }
}

