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
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Copy a file on an NFS mount."
)
@Plugin(
    examples = {
        @Example(
            title = "Copy a file from one location to another on an NFS mount.",
            code = {
                "from: /mnt/nfs/shared/in/file.txt",
                "to: /mnt/nfs/shared/out/file_copy.txt"
            }
        )
    }
)
public class Copy extends Task implements RunnableTask<Copy.Output> { // REMOVED CopyInterface

    @Schema(title = "The path to the file to copy.")
    @PluginProperty(dynamic = true)
    @NotNull
    private String from;

    @Schema(title = "The destination path.")
    @PluginProperty(dynamic = true)
    @NotNull
    private String to;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        Path fromPath = NfsService.toNfsPath(runContext.render(this.from));
        Path toPath = NfsService.toNfsPath(runContext.render(this.to));

        logger.info("Copying from {} to {}", fromPath, toPath);

        Path newPath = Files.copy(fromPath, toPath, StandardCopyOption.REPLACE_EXISTING);

        return Output.builder()
            .from(fromPath.toUri())
            .to(newPath.toUri())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The URI of the original file.")
        private final URI from;

        @Schema(title = "The URI of the new copied file.")
        private final URI to;
    }
}

