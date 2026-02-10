package io.kestra.plugin.fs.nfs;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.inject.Inject;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.IOException;
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
    title = "Delete a file on NFS",
    description = "Removes a file from an NFS mount. By default `errorOnMissing` is true and raises when the target is absent."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Delete a file from an NFS mount.",
            code = """
                id: nfs_delete
                namespace: company.team

                tasks:
                  - id: delete_file
                    type: io.kestra.plugin.fs.nfs.Delete
                    uri: /mnt/nfs/shared/logs/old_log.txt
                    errorOnMissing: false
                """
        )
    }
)
public class Delete extends Task implements RunnableTask<Delete.Output> {
    
    @Inject
    @Builder.Default
    private NfsService nfsService = NfsService.getInstance();

    @Schema(title = "Path of the file to delete")
    @NotNull
    private Property<String> uri;

    @Schema(title = "Raise an error if missing")
    @Builder.Default
    private boolean errorOnMissing = true;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        String rUri = runContext.render(this.uri).as(String.class).orElseThrow(() -> new IllegalArgumentException("`uri` cannot be null or empty"));
        Path path = nfsService.toNfsPath(rUri);

        logger.info("Deleting file at {}", path);

        boolean deleted;
        try {
            deleted = Files.deleteIfExists(path);
             if (!deleted) {
                 if (this.errorOnMissing) {
                      logger.error("File not found: {}", path);
                      throw new NoSuchFileException("File not found and 'errorOnMissing' is true: " + path);
                 } else {
                      logger.warn("File not found, but 'errorOnMissing' is false: {}", path);
                 }
             } else {
                  logger.debug("Successfully deleted file: {}", path);
             }
        } catch (NoSuchFileException e) {
            if (this.errorOnMissing) {
                logger.error("Attempted to delete non-existent file: {}", path, e);
                throw e;
            }
            logger.warn("Attempted to delete non-existent file, but 'errorOnMissing' is false: {}", path);
            deleted = false;
    } catch (IOException e) {
             logger.error("Error deleting file {}: {}", path, e.getMessage(), e);
             throw e;
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
