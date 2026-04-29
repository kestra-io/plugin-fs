package io.kestra.plugin.fs.local;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractLocalTask extends Task {
    static final String ALLOWED_PATHS = "allowed-paths";

    protected List<String> allowedPaths(RunContext runContext) {
        Optional<List<String>> allowedPathConfig = runContext.pluginConfiguration(ALLOWED_PATHS);

        if (allowedPathConfig.isEmpty() || allowedPathConfig.get().isEmpty()) {
            throw new SecurityException(
                "The 'allowed-paths' configuration is required to enable access to the local filesystem. " +
                "You must define at least one allowed path in the plugin configuration, `kestra.plugins.configurations`. " +
                "Refer to the example in the plugin documentation."
            );
        }

        return allowedPathConfig.get();
    }

    protected Path resolveLocalPath(String renderedPath, RunContext runContext) throws IllegalVariableEvaluationException {
        Path path = Paths.get(renderedPath).normalize();

        validatePath(path, runContext);

        return path;
    }

    /**
     * Validates a path against the list of allowed paths.
     *
     * @param path The path to validate
     * @param runContext The run context
     */
    protected void validatePath(Path path, RunContext runContext) {

        List<String> renderedAllowedPaths = allowedPaths(runContext);

        // gets real path also resolves symbolic links
        Path realPath;
        try {
            if (path.toFile().exists()) {
                realPath = path.toRealPath();
            } else {
                // for non-existing paths need to check if the parent directory is allowed
                Path absolute = path.toAbsolutePath().normalize();
                Path ancestor = absolute;
                Path suffix = Path.of("");
                while (ancestor != null && !ancestor.toFile().exists()) {
                    suffix = ancestor.getFileName() == null ? suffix : ancestor.getFileName().resolve(suffix);
                    ancestor = ancestor.getParent();
                }
                if (ancestor != null) {
                    realPath = ancestor.toRealPath().resolve(suffix);
                } else {
                    realPath = absolute;
                }
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid path: " + path + ". Error: " + e.getMessage(), e);
        }

        List<Path> normalizedAllowedPaths = renderedAllowedPaths.stream()
            .map(allowed -> {
                try {
                    return Paths.get(allowed).toRealPath();
                } catch (IOException e) {
                    return Paths.get(allowed).toAbsolutePath().normalize();
                }
            })
            .toList();

        boolean isAllowed = normalizedAllowedPaths.stream()
            .anyMatch(realPath::startsWith);

        if (!isAllowed) {
            String formattedAllowedPaths = normalizedAllowedPaths.stream()
                .map(Path::toString)
                .collect(Collectors.joining("', '", "'", "'"));

            throw new SecurityException(
                "Access to path '" + realPath + "' is denied. " +
                "The specified path must be within one of the configured 'allowed-paths': " + formattedAllowedPaths + ". " +
                "Refer to:: https://kestra.io/docs/configuration#set-default-values"
            );
        }
    }
}
