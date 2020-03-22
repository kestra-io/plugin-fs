package org.kestra.task.fs.sftp;

import lombok.Builder;
import lombok.Getter;
import org.kestra.core.models.annotations.OutputProperty;

import java.net.URI;

@Builder
@Getter
public class SftpOutput implements org.kestra.core.models.tasks.Output {
    @OutputProperty(
        description = "The fully-qualified URIs that point to destination path"
    )
    private URI from;

    @OutputProperty(
        description = "The fully-qualified URIs that point to source data"
    )
    private URI to;
}
