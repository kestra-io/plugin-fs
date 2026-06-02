package io.kestra.plugin.fs.local;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.exceptions.KestraRuntimeException;
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

import java.io.InputStream;
import java.net.URI;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Upload multiple files to the local filesystem",
    description = """
        Writes each provided Kestra storage URI to the destination directory under configured `allowed-paths`.
        Accepts a list, a map (destination filename to URI), a JSON-encoded list/map, or a storage URI of a file containing URIs.

        Example (Kestra config):
        ```yaml
        plugins:
          configurations:
            - type: io.kestra.plugin.fs.local.Uploads
              values:
                allowed-paths:
                  - /data/uploads
        ```
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Upload a list of files to a local directory",
            full = true,
            code = """
                id: fs_local_uploads
                namespace: company.team

                inputs:
                  - id: file1
                    type: FILE
                  - id: file2
                    type: FILE

                tasks:
                  - id: uploads
                    type: io.kestra.plugin.fs.local.Uploads
                    from:
                      - "{{ inputs.file1 }}"
                      - "{{ inputs.file2 }}"
                    to: "/data/uploads"
                """
        ),
        @Example(
            title = "Upload only files matching a regex (e.g. .sql files)",
            full = true,
            code = """
                id: fs_local_uploads_regexp
                namespace: company.team

                tasks:
                  - id: uploads
                    type: io.kestra.plugin.fs.local.Uploads
                    from:
                      - "{{ outputs.step1.uri }}"
                      - "{{ outputs.step2.uri }}"
                    regExp: ".*\\\\.sql$"
                    to: "/data/uploads"
                """
        ),
        @Example(
            title = "Upload with custom destination filenames using a map",
            full = true,
            code = """
                id: fs_local_uploads_with_names
                namespace: company.team

                inputs:
                  - id: file1
                    type: FILE
                  - id: file2
                    type: FILE

                tasks:
                  - id: uploads
                    type: io.kestra.plugin.fs.local.Uploads
                    from:
                      report.csv: "{{ inputs.file1 }}"
                      data.json: "{{ inputs.file2 }}"
                    to: "/data/uploads"
                """
        )
    }
)
public class Uploads extends AbstractLocalTask implements RunnableTask<Uploads.Output>, Data.From {

    @Schema(
        title = "Files to upload (kestra:// URIs)",
        anyOf = {
            String.class,
            List.class,
            Map.class
        },
        description = "Kestra internal storage URIs; accepts a single URI string, a list of URIs, a map of destination filenames to URIs (to preserve original filenames), or a URI pointing to a file that contains URIs."
    )
    @PluginProperty(dynamic = true, internalStorageURI = true, group = "main")
    @NotNull
    private Object from;

    @Schema(
        title = "Destination directory",
        description = "Must resolve within one of the configured `allowed-paths`."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> to;

    @Schema(
        title = "Regexp filter on full path",
        description = "Only source URIs fully matching this regex are uploaded. Example: `.*\\.sql$`."
    )
    @PluginProperty(group = "processing")
    private Property<String> regExp;

    @Builder.Default
    @Schema(
        title = "Maximum files to upload"
    )
    @PluginProperty(group = "execution")
    private Property<Integer> maxFiles = Property.ofValue(25);

    @Builder.Default
    @Schema(
        title = "Overwrite existing files",
        description = "If false, fails when the destination file already exists."
    )
    @PluginProperty(group = "advanced")
    private Property<Boolean> overwrite = Property.ofValue(true);

    @Override
    public Output run(RunContext runContext) throws Exception {
        List<Map.Entry<String, String>> fileMappings = parseFromProperty(runContext);

        String rRegExp = runContext.render(this.regExp).as(String.class).orElse(null);
        if (rRegExp != null) {
            Pattern pattern = Pattern.compile(rRegExp);
            fileMappings = fileMappings.stream()
                .filter(entry -> pattern.matcher(entry.getValue()).matches())
                .toList();
        }

        int rMaxFiles = runContext.render(this.maxFiles).as(Integer.class).orElse(25);
        if (fileMappings.size() > rMaxFiles) {
            runContext.logger().warn("Too many files to process ({}), limiting to {}", fileMappings.size(), rMaxFiles);
            fileMappings = fileMappings.subList(0, rMaxFiles);
        }

        boolean rOverwrite = runContext.render(this.overwrite).as(Boolean.class).orElse(true);
        String rTo = runContext.render(this.to).as(String.class).orElseThrow();

        Path destinationDir = resolveLocalPath(rTo, runContext);
        Files.createDirectories(destinationDir);

        CopyOption[] options = rOverwrite
            ? new CopyOption[]{StandardCopyOption.REPLACE_EXISTING}
            : new CopyOption[]{};

        List<URI> outputs = new ArrayList<>();
        for (Map.Entry<String, String> entry : fileMappings) {
            String fromURI = entry.getValue();
            String fileName = entry.getKey() != null
                ? entry.getKey()
                : fromURI.substring(fromURI.lastIndexOf('/') + 1);

            Path target = destinationDir.resolve(fileName).normalize();
            // re-validate the resolved target to prevent escapes via "../" in filename
            validatePath(target, runContext);

            if (Files.exists(target) && !rOverwrite) {
                throw new KestraRuntimeException(String.format(
                    "Target file already exists: %s. Set 'overwrite: true' to replace.", target
                ));
            }

            try (InputStream in = runContext.storage().getFile(URI.create(fromURI))) {
                runContext.logger().debug("Copying {} to {}", fromURI, target);
                Files.copy(in, target, options);
            }

            outputs.add(target.toUri());
        }

        return Output.builder().files(outputs).build();
    }

    @SuppressWarnings("unchecked")
    private List<Map.Entry<String, String>> parseFromProperty(RunContext runContext) throws Exception {
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

            if (rFrom.startsWith("{") && rFrom.endsWith("}")) {
                Map<String, String> jsonMap = JacksonMapper.ofJson().readValue(rFrom, new TypeReference<Map<String, String>>() {});
                return jsonMap.entrySet().stream()
                    .<Map.Entry<String, String>>map(e -> new SimpleEntry<>(e.getKey(), e.getValue()))
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
            title = "Local file URIs of the uploaded files"
        )
        private List<URI> files;
    }
}
