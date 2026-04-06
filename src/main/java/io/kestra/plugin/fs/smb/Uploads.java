package io.kestra.plugin.fs.smb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
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

import java.net.URI;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Upload multiple files over SMB",
    description = "Uploads each provided file to the target directory on an SMB/CIFS share. Default port 445."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: fs_smb_uploads
                namespace: company.team

                inputs:
                  - id: file1
                    type: FILE
                  - id: file2
                    type: FILE

                tasks:
                  - id: uploads
                    type: io.kestra.plugin.fs.smb.Uploads
                    host: localhost
                    port: "445"
                    username: foo
                    password: "{{ secret('SMB_PASSWORD') }}"
                    from:
                      - "{{ inputs.file1 }}"
                      - "{{ inputs.file2 }}"
                    to: "/my_share/dir2"
                """
        )
    }
)
public class Uploads extends AbstractSmbTask implements RunnableTask<Uploads.Output>, Data.From {
    @Schema(
        title = "Files to upload (kestra:// URIs)",
        anyOf = {
            String.class,
            java.util.List.class,
            Map.class
        },
        description = "Kestra internal storage URIs; accepts a single URI string, a list of URIs, a map of destination filenames to URIs (to preserve original filenames), or a URI pointing to a file that contains URIs."
    )
    @PluginProperty(dynamic = true, internalStorageURI = true, group = "main")
    @NotNull
    private Object from;

    @Schema(
        title = "Destination directory"
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> to;

    @Builder.Default
    @Schema(
        title = "Maximum files to upload"
    )
    @PluginProperty(group = "processing")
    private Property<Integer> maxFiles = Property.ofValue(25);

    public Output run(RunContext runContext) throws Exception {
        var ctx = createContext(runContext);
        try {
            var fileMappings = parseFromProperty(runContext);

            var rMaxFiles = runContext.render(this.maxFiles).as(Integer.class).orElse(25);
            if (fileMappings.size() > rMaxFiles) {
                runContext.logger().warn("Too many files to process ({}), limiting to {}", fileMappings.size(), rMaxFiles);
                fileMappings = fileMappings.subList(0, rMaxFiles);
            }

            var outputs = fileMappings.stream().map(throwFunction(entry -> {
                var destFileName = entry.getKey();
                var fromURI = entry.getValue();
                var rTo = runContext.render(this.to).as(String.class).orElseThrow();

                String destPath;
                if (destFileName != null) {
                    destPath = rTo + (rTo.endsWith("/") ? "" : "/") + destFileName;
                } else {
                    destPath = rTo + fromURI.substring(fromURI.lastIndexOf('/') + (rTo.endsWith("/") ? 1 : 0));
                }

                return SmbService.upload(
                    runContext,
                    ctx,
                    this,
                    URI.create(fromURI),
                    destPath
                );
            })).toList();

            return Output.builder()
                .files(outputs.stream()
                    .map(io.kestra.plugin.fs.vfs.Upload.Output::getTo)
                    .toList()
                )
                .build();
        } finally {
            ctx.close();
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

        if (this.from instanceof String fromStr) {
            var rFrom = runContext.render(fromStr).trim();

            if (rFrom.startsWith("[") && rFrom.endsWith("]")) {
                String[] uris = JacksonMapper.ofJson().readValue(rFrom, String[].class);
                return Arrays.stream(uris)
                    .<Map.Entry<String, String>>map(uri -> new SimpleEntry<>(null, uri))
                    .toList();
            }
        }

        return Objects.requireNonNull(Data.from(this.from)
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
