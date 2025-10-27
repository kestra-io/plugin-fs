package io.kestra.plugin.fs.vfs;

import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class Uploads extends AbstractVfsTask implements RunnableTask<Uploads.Output>, Data.From {
    @Schema(
            title = "The files to upload.",
            description = "Must be Kestra internal storage URIs. Can be a single URI string, a list of URI strings, or an internal storage URI pointing to a file containing URIs."
    )
    @NotNull
    private Object from;

    @Schema(
            title = "The destination directory"
    )
    @NotNull
    private Property<String> to;

    public Output run(RunContext runContext) throws Exception {
        try (StandardFileSystemManager fsm = new KestraStandardFileSystemManager(runContext)) {
            fsm.setConfiguration(StandardFileSystemManager.class.getResource(KestraStandardFileSystemManager.CONFIG_RESOURCE));
            fsm.init();

            String renderedTo = runContext.render(this.to).as(String.class).orElseThrow();

            List<Upload.Output> outputs = Data.from(from).read(runContext)
                .map(throwFunction(row -> {
                    String fromURI = row.toString();
                    
                    if (!fromURI.startsWith("kestra://")) {
                        throw new IllegalArgumentException("'from' must be a list of Kestra's internal storage URI");
                    }

                    return VfsService.upload(
                        runContext,
                        fsm,
                        this.fsOptions(runContext),
                        URI.create(fromURI),
                        this.uri(runContext, renderedTo + fromURI.substring(fromURI.lastIndexOf('/') + (renderedTo.endsWith("/") ? 1 : 0)))
                    );
                }))
                .collectList()
                .block();

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
