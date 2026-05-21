package io.kestra.plugin.fs.local;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.FileUtils;
import io.kestra.plugin.fs.vfs.ChecksumService;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.net.URI;
import java.nio.file.*;
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Download local file to internal storage",
    description = """
        Copies a local file (within configured `allowed-paths`) into Kestra internal storage. Fails if the source is missing; preserves size and file extension when possible.
        Local access requires `allowed-paths` in plugin defaults.

        Example (Kestra config):
        ```yaml
        plugins:
          configurations:
            - type: io.kestra.plugin.fs.local.Download
              values:
                allowed-paths:
                  - /data/files
        ```
        """
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
        ),
        @Example(
            title = "Download a local file and fail if its SHA-256 checksum does not match",
            full = true,
            code = """
                id: fs_local_download_checksum
                namespace: company.team

                tasks:
                  - id: download
                    type: io.kestra.plugin.fs.local.Download
                    from: "/data/files/source.csv"
                    validateChecksum: true
                    checksumExpected: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                """
        )
    }
)
public class Download extends AbstractLocalTask implements RunnableTask<Download.Output> {

    @Schema(
        title = "Source file path",
        description = "Absolute local path of the file to download"
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> from;

    @Schema(
        title = "Validate the downloaded file against an expected checksum",
        description = "When enabled, the task fails if the computed checksum does not match `checksumExpected`. The downloaded file is not stored in internal storage if validation fails."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> validateChecksum = Property.ofValue(false);

    @Schema(
        title = "Expected checksum value to compare against the downloaded file",
        description = "Required when `validateChecksum` is `true`. Comparison is case-insensitive."
    )
    @PluginProperty(group = "advanced")
    private Property<String> checksumExpected;

    @Schema(
        title = "Checksum algorithm to use",
        description = "Defaults to `SHA_256`. The computed checksum is always exposed on the output as `checksum`."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<ChecksumService.Algorithm> checksumAlgorithm = Property.ofValue(ChecksumService.Algorithm.SHA_256);

    @Override
    public Output run(RunContext runContext) throws Exception {

        String renderedFrom = runContext.render(this.from).as(String.class).orElseThrow();

        Path sourcePath = resolveLocalPath(renderedFrom, runContext);

        if (!Files.exists(sourcePath)) {
            throw new IllegalArgumentException("Source file '" + sourcePath + "' does not exist");
        }

        String extension = FileUtils.getExtension(renderedFrom);
        if (extension == null) {
            extension = ".tmp";
        }

        File tempFile = runContext.workingDir().createTempFile(extension).toFile();
        Files.copy(sourcePath, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

        boolean rValidateChecksum = runContext.render(this.validateChecksum).as(Boolean.class).orElse(false);
        String rChecksumExpected = runContext.render(this.checksumExpected).as(String.class).orElse(null);
        ChecksumService.Algorithm rChecksumAlgorithm = runContext.render(this.checksumAlgorithm).as(ChecksumService.Algorithm.class).orElse(ChecksumService.Algorithm.SHA_256);

        String checksum = rValidateChecksum
            ? ChecksumService.verify(tempFile.toPath(), rChecksumAlgorithm, rChecksumExpected)
            : ChecksumService.compute(tempFile.toPath(), rChecksumAlgorithm);

        URI storageUri = runContext.storage().putFile(tempFile);

        return Output.builder()
            .uri(storageUri)
            .size(Files.size(sourcePath))
            .checksum(checksum)
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

        @Schema(
            title = "Checksum of the downloaded file",
            description = "Hex-encoded digest computed with `checksumAlgorithm`. Populated whenever the file is downloaded successfully."
        )
        private String checksum;
    }
}
