package io.kestra.plugin.fs.local;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.NoSuchElementException;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public class Delete extends AbstractLocalTask implements RunnableTask<Delete.Output> {

    @Schema(title = "The local file URI to delete (e.g., file:///tmp/test.txt)")
    @NotNull
    private Property<String> uri;

    @Schema(title = "Raise an error if the file is not found")
    @Builder.Default
    private Property<Boolean> errorOnMissing = Property.of(false);

    @Schema(
        title = "Whether to include subdirectories",
        description = "If true, will recursively delete files in all subdirectories"
    )
    @Builder.Default
    private Property<Boolean> recursive = Property.of(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        URI uri = URI.create(runContext.render(this.uri).as(String.class).orElseThrow());
        Path path = Paths.get(uri);

        if (!Files.exists(path)) {
            if (runContext.render(this.errorOnMissing).as(Boolean.class).orElse(false)) {
                throw new NoSuchElementException("Unable to find file '" + path + "'");
            }

            runContext.logger().debug("File doesn't exist '{}'", path);

            return Output.builder()
                .uri(uri)
                .deleted(false)
                .build();
        }

        Output.OutputBuilder outputBuilder = Output.builder();

        if (Boolean.TRUE.equals(runContext.render(this.recursive).as(Boolean.class).orElse(false))
            && Files.isDirectory(path)) {
            outputBuilder.deleted(deleteDirectoryRecursively(path));

            runContext.logger().debug("Deleted directory '{}'", path);
        } else {
           outputBuilder.deleted(Files.deleteIfExists(path));
           runContext.logger().debug("Deleted file '{}'", path);
        }

        return outputBuilder
            .uri(uri)
            .build();
    }

    private boolean deleteDirectoryRecursively(Path dir) {
        try {
            Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The deleted URI")
        private final URI uri;

        @Schema(title = "Whether the file was deleted")
        private final boolean deleted;
    }
}