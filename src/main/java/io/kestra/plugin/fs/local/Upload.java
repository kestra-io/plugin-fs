package io.kestra.plugin.fs.local;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.nio.file.*;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Copy a file to a local filesystem location."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: fs_local_upload
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE

                tasks:
                  - id: upload
                    type: io.kestra.plugin.fs.local.Upload
                    from: "{{ inputs.file }}"
                    to: "/data/uploads/file.txt"
                    overwrite: true
                    workerGroup: "etl-worker"
                """
        )
    }
)
public class Upload extends AbstractLocalTask implements RunnableTask<Upload.Output> {
    private static final String USER_DIR = System.getProperty("user.home");

    @Schema(
        title = "Source file URI",
        description = "Can be any valid Kestra file URI (kestra://, file://, etc.)"
    )
    @NotNull
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    @Schema(
        title = "The destination path, if not set it will use the name of the file denoted by the `from` property"
    )
    private Property<String> to;

    @Schema(
        title = "Whether to overwrite existing files",
        description = "If false, will throw an error if the target file already exists"
    )
    @Builder.Default
    private Property<Boolean> overwrite = Property.of(true);

    @Override
    public Output run(RunContext runContext) throws Exception {

        var renderedFrom = runContext.render(this.from).as(String.class).orElseThrow();
        if (!renderedFrom.startsWith("kestra://")) {
            throw new IllegalArgumentException("'from' must be a Kestra's internal storage URI");
        }

        var renderedTo = runContext.render(this.to).as(String.class)
            .orElse(renderedFrom.substring(renderedFrom.lastIndexOf('/') + 1));

        Path destinationPath = resolveLocalPath(renderedTo);

        Files.createDirectories(destinationPath.getParent()!=null ? destinationPath.getParent() : destinationPath);

        CopyOption[] options = runContext.render(overwrite).as(Boolean.class).orElse(true)
            ? new CopyOption[] { StandardCopyOption.REPLACE_EXISTING }
            : new CopyOption[] {};

        try (var sourceFile = runContext.storage().getFile(URI.create(renderedFrom))) {
            runContext.logger().info("Copying {} to {}", URI.create(renderedFrom), destinationPath);
            Files.copy(sourceFile, destinationPath, options);
        }

        return Output.builder()
            .uri(destinationPath.toUri())
            .size(Files.size(destinationPath))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "URI of the uploaded file"
        )
        private URI uri;

        @Schema(
            title = "Size of the uploaded file in bytes"
        )
        private Long size;
    }
}