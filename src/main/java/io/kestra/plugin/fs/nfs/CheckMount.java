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
            title = "Check a specific NFS mount point.",
            code = "path: /mnt/nfs/share"
        )
    }
)
public class CheckMount extends Task implements RunnableTask<CheckMount.Output> {

    @Schema(
        title = "The NFS path to check."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String path;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        Path nfsPath = NfsService.toNfsPath(runContext.render(this.path));

        try {
            String storeType = NfsService.getFileStoreType(nfsPath);
            boolean isNfs = storeType.toLowerCase().contains("nfs");

            logger.info("Path {} is on a file store of type: {}. Is NFS: {}", nfsPath, storeType, isNfs);

            return Output.builder()
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
        @Schema(title = "Whether the path is identified as an NFS mount.")
        private final boolean isNfsMount;

        @Schema(title = "The type of the file store (e.g., 'nfs', 'nfs4', 'ext4').")
        private final String fileStoreType;
    }
}
