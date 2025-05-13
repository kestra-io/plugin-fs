package io.kestra.plugin.fs.local;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.FileUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.net.URI;
import java.nio.file.*;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Download a file from the local filesystem to the Kestra storage."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: fs_local_download
                namespace: company.team

                tasks:
                  - id: download
                    type: io.kestra.plugin.fs.local.Download
                    from: "/data/files/source.csv"
                    workerGroup: "etl-worker"
                """
        )
    }
)
public class Download extends AbstractLocalTask implements RunnableTask<Download.Output> {

    @Schema(
        title = "Source file path on the local filesystem",
        description = "Absolute path of the file to download"
    )
    @NotNull
    private Property<String> from;

    @Override
    public Output run(RunContext runContext) throws Exception {

        String renderedFrom = runContext.render(this.from).as(String.class).orElseThrow();

        Path sourcePath = resolveLocalPath(renderedFrom, runContext);

        if (!Files.exists(sourcePath)) {
            throw new IllegalArgumentException("Source file '" + sourcePath + "' does not exist");
        }

        File tempFile = runContext.workingDir().createTempFile(FileUtils.getExtension(renderedFrom)).toFile();
        Files.copy(sourcePath, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

        URI storageUri = runContext.storage().putFile(tempFile);

        return Output.builder()
            .uri(storageUri)
            .size(Files.size(sourcePath))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "URI of the file in Kestra storage"
        )
        private URI uri;

        @Schema(
            title = "Size of the downloaded file in bytes"
        )
        private Long size;
    }
}