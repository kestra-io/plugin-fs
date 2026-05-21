package io.kestra.plugin.fs.vfs;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;

import java.net.URI;
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class Download extends AbstractVfsTask implements RunnableTask<Download.Output> {
    @Schema(
        title = "Source URI to download"
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> from;

    @Schema(
        title = "Validate the downloaded file against an expected checksum",
        description = "When enabled, the task fails if the computed checksum does not match `checksumExpected`. The downloaded file is not stored in internal storage if validation fails."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<Boolean> validateChecksum = Property.ofValue(false);

    @Schema(
        title = "Expected checksum value to compare against the downloaded file",
        description = "Required when `validateChecksum` is `true`. Comparison is case-insensitive."
    )
    @PluginProperty(group = "advanced")
    protected Property<String> checksumExpected;

    @Schema(
        title = "Checksum algorithm to use",
        description = "Defaults to `SHA_256`. The computed checksum is always exposed on the output as `checksum`."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<ChecksumService.Algorithm> checksumAlgorithm = Property.ofValue(ChecksumService.Algorithm.SHA_256);

    public Output run(RunContext runContext) throws Exception {
        try (StandardFileSystemManager fsm = new KestraStandardFileSystemManager(runContext)) {
            fsm.setConfiguration(StandardFileSystemManager.class.getResource(KestraStandardFileSystemManager.CONFIG_RESOURCE));
            fsm.init();

            boolean rValidateChecksum = runContext.render(this.validateChecksum).as(Boolean.class).orElse(false);
            String rChecksumExpected = runContext.render(this.checksumExpected).as(String.class).orElse(null);
            ChecksumService.Algorithm rChecksumAlgorithm = runContext.render(this.checksumAlgorithm).as(ChecksumService.Algorithm.class).orElse(ChecksumService.Algorithm.SHA_256);

            return VfsService.download(new VfsDownloadRequest(
                runContext,
                fsm,
                this.fsOptions(runContext),
                this.uri(runContext, runContext.render(this.from).as(String.class).orElseThrow()),
                rValidateChecksum,
                rChecksumExpected,
                rChecksumAlgorithm
            ));
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The fully-qualified URIs that point to source data"
        )
        private URI from;

        @Schema(
            title = "The fully-qualified URIs that point to destination path"
        )
        private URI to;

        @Schema(
            title = "Checksum of the downloaded file",
            description = "Hex-encoded digest computed with `checksumAlgorithm`. Populated whenever the file is downloaded successfully."
        )
        private String checksum;
    }
}
