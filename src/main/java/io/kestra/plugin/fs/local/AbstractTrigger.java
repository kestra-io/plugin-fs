package io.kestra.plugin.fs.local;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractTrigger extends io.kestra.core.models.triggers.AbstractTrigger {
    @Schema(
        title = "Allowed paths for local filesystem access",
        description = "List of allowed paths for local filesystem access." +
            " Tasks will only be able to operate on files within these paths."
    )
    @NotNull
    protected Property<java.util.List<String>> allowedPaths;

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
     * @throws SecurityException If the path is outside allowed paths
     */
    protected void validatePath(Path path, RunContext runContext) throws IllegalVariableEvaluationException {

        java.util.List<String> renderedAllowedPaths = runContext.render(allowedPaths).asList(String.class);

        if(renderedAllowedPaths.isEmpty()) {
            runContext.logger().warn("No 'allowedPaths' configured task execution stopped " +
                "to enforce secure file access.");

            throw new SecurityException(
                "Missing configuration: 'allowedPaths' must be set to allow local filesystem access. " +
                    "Configure 'allowedPaths' in your Kestra task or globally as a 'plugin default' in your Kestra " +
                    "configuration file."
            );
        }

        // gets real path also resolves symbolic links
        Path realPath;
        try {
            if (!path.toFile().exists()) {
                // for non-existing paths need to check if the parent directory is allowed
                Path parent = path.getParent();
                if (parent != null && parent.toFile().exists()) {
                    realPath = parent.toRealPath().resolve(path.getFileName());
                } else {
                    realPath = path.toAbsolutePath();
                }
            } else {
                realPath = path.toRealPath();
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid path: " + path + ". Error: " + e.getMessage(), e);
        }

        List<Path> normalizedAllowedPaths = renderedAllowedPaths.stream()
            .map(allowed -> Paths.get(allowed).toAbsolutePath().normalize())
            .toList();

        boolean isAllowed = normalizedAllowedPaths.stream()
            .anyMatch(realPath::startsWith);

        if (!isAllowed) {
            String formattedAllowedPaths = normalizedAllowedPaths.stream()
                .map(Path::toString)
                .collect(Collectors.joining("', '", "'", "'"));

            runContext.logger().warn(
                "Access to path '{}' is denied. It is not within the allowed paths: {}. " +
                    "Update the 'allowedPaths' configuration in your task if access is intended.",
                realPath, formattedAllowedPaths
            );

            throw new SecurityException(
                "Access denied to path '" + realPath + "'. " +
                    "The path must be within one of the configured allowed paths: " + formattedAllowedPaths + ". "
            );
        }
    }
}
