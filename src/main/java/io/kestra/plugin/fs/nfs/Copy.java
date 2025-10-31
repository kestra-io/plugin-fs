package io.kestra.plugin.fs.nfs;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
 
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import jakarta.inject.Inject;
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
            full = true,
            title = "Copy a file from one location to another on an NFS mount.",
            code = """
                id: nfs_copy
                namespace: company.team

                tasks:
                  - id: copy_file
                    type: io.kestra.plugin.fs.nfs.Copy
                    from: /mnt/nfs/shared/in/file.txt
                    to: /mnt/nfs/shared/out/file_copy.txt
            """
        )
    }
)
public class Copy extends Task implements RunnableTask<Copy.Output> {

    @Inject
    private transient NfsService nfsService;

    public void setNfsService(NfsService nfsService) {
        this.nfsService = nfsService;
    }

    @Schema(title = "The path to the file to copy.")
    @NotNull
    private Property<String> from;

    @Schema(title = "The destination path.")
    @NotNull
    private Property<String> to;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        
        String rFrom = runContext.render(this.from).as(String.class).orElseThrow(() -> new IllegalArgumentException("`from` cannot be null or empty"));
        String rTo = runContext.render(this.to).as(String.class).orElseThrow(() -> new IllegalArgumentException("`to` cannot be null or empty"));

        Path fromPath = nfsService.toNfsPath(rFrom);
        Path toPath = nfsService.toNfsPath(rTo);

        logger.info("Copying from {} to {}", fromPath, toPath);

        
        Path toParent = toPath.getParent();
        if (toParent != null && !Files.exists(toParent)) {
            Files.createDirectories(toParent);
            logger.debug("Created parent directory: {}", toParent);
        }

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
