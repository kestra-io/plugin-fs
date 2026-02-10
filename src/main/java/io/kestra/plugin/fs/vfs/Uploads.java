package io.kestra.plugin.fs.vfs;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class Uploads extends AbstractVfsTask implements RunnableTask<Uploads.Output>, Data.From {
    @Schema(
        title = "Files to upload (kestra:// URIs)",
        anyOf = {
            String.class,
            List.class,
            Map.class
        },
        description = "Kestra internal storage URIs; accepts a single URI string, a list of URIs, or a URI pointing to a file that contains URIs."
    )
    @PluginProperty(dynamic = true, internalStorageURI = true)
    @NotNull
    private Object from;

    @Schema(
            title = "Destination directory"
    )
    @NotNull
    private Property<String> to;

    @Builder.Default
    @Schema(
        title = "Maximum files to upload"
    )
    private Property<Integer> maxFiles = Property.ofValue(25);

    public Output run(RunContext runContext) throws Exception {
        try (StandardFileSystemManager fsm = new KestraStandardFileSystemManager(runContext)) {
            fsm.setConfiguration(StandardFileSystemManager.class.getResource(KestraStandardFileSystemManager.CONFIG_RESOURCE));
            fsm.init();

            String[] renderedFrom = parseFromProperty(runContext);

            int rMaxFiles = runContext.render(this.maxFiles).as(Integer.class).orElse(25);
            if (renderedFrom.length > rMaxFiles) {
                runContext.logger().warn("Too many files to process ({}), limiting to {}", renderedFrom.length, rMaxFiles);
                renderedFrom = Arrays.copyOf(renderedFrom, rMaxFiles);
            }

            List<Upload.Output> outputs = Arrays.stream(renderedFrom).map(throwFunction(fromURI -> {
                var rTo = runContext.render(this.to).as(String.class).orElseThrow();
                return VfsService.upload(
                    runContext,
                    fsm,
                    this.fsOptions(runContext),
                    URI.create(fromURI),
                    this.uri(runContext, rTo + fromURI.substring(fromURI.lastIndexOf('/') + (rTo.endsWith("/") ? 1 : 0)))
                );
            })).toList();

            return Output.builder()
                .files(outputs.stream()
                    .map(Upload.Output::getTo)
                    .toList()
                )
                .build();
        }
    }

    private String[] parseFromProperty(RunContext runContext) throws Exception {
        if (this.from instanceof String from) {
            var rFrom = runContext.render(from).trim();

            if (rFrom.startsWith("[") && rFrom.endsWith("]")) {
                return JacksonMapper.ofJson().readValue(rFrom, String[].class);
            }
        }

        return Objects.requireNonNull(Data.from(this.from)
                .readAs(runContext, String.class, Object::toString)
                .map(throwFunction(runContext::render))
                .collectList()
                .block())
            .toArray(String[]::new);
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
