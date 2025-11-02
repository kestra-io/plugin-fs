package io.kestra.plugin.fs.nfs;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Path;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Check if a path is a valid and accessible NFS mount."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Check if a path is a valid and accessible NFS mount.",
            code = """
                id: check_nfs_mount
                namespace: dev
                tasks:
                  - id: check_mount
                    type: io.kestra.plugin.fs.nfs.CheckMount
                    path: /mnt/nfs/data
                """ 
        )
    }
)
public class CheckMount extends Task implements RunnableTask<CheckMount.Output> {

    @Schema(
        title = "The NFS path to check."
    )
    @NotNull
    private Property<String> path;

    @Override
    public Output run(RunContext runContext) throws Exception {
        NfsService nfsService = NfsService.getInstance();
        
        Logger logger = runContext.logger();
        String rPath = runContext.render(this.path).as(String.class).orElseThrow(() -> new IllegalArgumentException("`path` cannot be null or empty"));
        Path nfsPath = nfsService.toNfsPath(rPath);

        try {
            String storeType = nfsService.getFileStoreType(nfsPath);
            boolean isNfs = storeType.toLowerCase().contains("nfs");

            logger.info("Path {} is on a file store of type: {}. Is NFS: {}", nfsPath, storeType, isNfs);

            
            return Output.builder()
                .path(rPath) 
                .isNfsMount(isNfs)
                .fileStoreType(storeType)
                .build();
        } catch (IOException e) {
            logger.error("Failed to check NFS mount at {}: {}", nfsPath, e.getMessage(), e);
            throw new IOException("Could not verify file store for path: " + nfsPath + ". Make sure the path exists and the mount is accessible.", e);
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The path that was checked.")
        private final String path;

        @Schema(title = "Whether the path is identified as an NFS mount.")
        private final boolean isNfsMount;

        @Schema(title = "The type of the file store (e.g., 'nfs', 'nfs4', 'ext4').")
        private final String fileStoreType;
    }
}
