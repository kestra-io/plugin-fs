package io.kestra.plugin.fs.vfs;

import io.kestra.core.models.annotations.PluginProperty;
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
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

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
        description = "Kestra internal storage URIs; accepts a single URI string, a list of URIs, a map of destination filenames to URIs (to preserve original filenames), or a URI pointing to a file that contains URIs."
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

            // Each entry maps a destination filename (or null) to a source URI
            java.util.List<Map.Entry<String, String>> fileMappings = parseFromProperty(runContext);

            int rMaxFiles = runContext.render(this.maxFiles).as(Integer.class).orElse(25);
            if (fileMappings.size() > rMaxFiles) {
                runContext.logger().warn("Too many files to process ({}), limiting to {}", fileMappings.size(), rMaxFiles);
                fileMappings = fileMappings.subList(0, rMaxFiles);
            }

            java.util.List<Upload.Output> outputs = fileMappings.stream().map(throwFunction(entry -> {
                String destFileName = entry.getKey();
                String fromURI = entry.getValue();
                var rTo = runContext.render(this.to).as(String.class).orElseThrow();

                String destPath;
                if (destFileName != null) {
                    // Map-based: use the provided filename
                    destPath = rTo + (rTo.endsWith("/") ? "" : "/") + destFileName;
                } else {
                    // List/String-based: use the URI's last segment (existing behavior)
                    destPath = rTo + fromURI.substring(fromURI.lastIndexOf('/') + (rTo.endsWith("/") ? 1 : 0));
                }

                return VfsService.upload(
                    runContext,
                    fsm,
                    this.fsOptions(runContext),
                    URI.create(fromURI),
                    this.uri(runContext, destPath)
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

    @SuppressWarnings("unchecked")
    private java.util.List<Map.Entry<String, String>> parseFromProperty(RunContext runContext) throws Exception {
        if (this.from instanceof Map<?, ?> fromMap) {
            return ((Map<String, String>) fromMap).entrySet().stream()
                .map(throwFunction(e -> new SimpleEntry<>(
                    runContext.render(e.getKey()),
                    runContext.render(e.getValue())
                )))
                .map(e -> (Map.Entry<String, String>) e)
                .toList();
        }

        Data data = Data.from(this.from);

        try {
            Function<Map<String, Object>, Map<String, String>> toStringMap = map -> {
                Map<String, String> result = new HashMap<>();
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    result.put(entry.getKey(), entry.getValue().toString());
                }
                return result;
            };
            Map<String, String> map = data.readAs(runContext, (Class<Map<String, String>>) Map.of().getClass(), toStringMap).blockFirst();
            if (map != null) {
                return map.entrySet().stream()
                    .<Map.Entry<String, String>>map(e -> new SimpleEntry<>(e.getKey(), e.getValue()))
                    .toList();
            }
        } catch (Exception e) {
            runContext.logger().debug("'from' is not a Map, trying as list/URI...", e);
        }

        return Objects.requireNonNull(data
            .readAs(runContext, String.class, Object::toString)
            .map(throwFunction(runContext::render))
            .<Map.Entry<String, String>>map(uri -> new SimpleEntry<>(null, uri))
            .collectList()
            .block());
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The fully-qualified URIs that point to the uploaded files on remote"
        )
        private java.util.List<URI> files;
    }
}
