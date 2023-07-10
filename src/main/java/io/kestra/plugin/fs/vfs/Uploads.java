package io.kestra.plugin.fs.vfs;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class Uploads extends AbstractVfsTask implements RunnableTask<Uploads.Output> {
    @PluginProperty(dynamic = true)
    @Schema(
            title = "The files to upload, must be internal storage URIs"
    )
    private String[] from;

    @Schema(
            title = "The destination directory"
    )
    @PluginProperty(dynamic = true)
    private String to;

    public Output run(RunContext runContext) throws Exception {
        try (StandardFileSystemManager fsm = new StandardFileSystemManager()) {
            fsm.init();

            List<Upload.Output> outputs = Arrays.stream(this.from).map(throwFunction((fromURI) -> VfsService.upload(
                    runContext,
                    fsm,
                    this.fsOptions(runContext),
                    URI.create(runContext.render(fromURI)),
                    this.uri(runContext, this.to + fromURI.substring(fromURI.lastIndexOf('/') + (this.to.endsWith("/") ? 1 : 0)))
            ))).toList();

            return Output.builder()
                    .files(outputs.stream()
                            .map(Upload.Output::getTo)
                            .toList()
                    )
                    .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
                title = "The fully-qualified URIs that point to the uploaded files on remote"
        )
        private List<URI> files;
    }
}
