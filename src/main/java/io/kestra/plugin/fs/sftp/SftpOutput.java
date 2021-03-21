package io.kestra.plugin.fs.sftp;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

import java.net.URI;

@Builder
@Getter
public class SftpOutput implements io.kestra.core.models.tasks.Output {
    @Schema(
            title = "The fully-qualified URIs that point to destination path"
    )
    private URI from;

    @Schema(
            title = "The fully-qualified URIs that point to source data"
    )
    private URI to;
}
