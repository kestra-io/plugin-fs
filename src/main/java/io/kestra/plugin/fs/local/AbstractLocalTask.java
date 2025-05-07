package io.kestra.plugin.fs.local;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.nio.file.Path;
import java.nio.file.Paths;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractLocalTask extends Task {
    protected static final String USER_DIR = System.getProperty("user.home");

    @Schema(
        title = "Base path for local filesystem operations",
        description = "Relative paths in `to` will be resolved against this from. If not set, defaults to `/data/kestra/`."
    )
    @Builder.Default
    protected Property<String> basePath = Property.of(USER_DIR);

    protected Path resolveLocalPath(String renderedPath, String basePath) {
        Path basePathResolved = Paths.get(basePath).normalize();
        Path resolvedPath = Paths.get(renderedPath).normalize();

        Path targetPath;

        if (resolvedPath.isAbsolute()) {
            if (resolvedPath.startsWith(basePathResolved)) {
                targetPath = resolvedPath;
            } else {
                targetPath = basePathResolved.resolve(resolvedPath).normalize();
            }
        } else {
            targetPath = basePathResolved.resolve(resolvedPath).normalize();
        }

        if (!targetPath.startsWith(basePathResolved)) {
            throw new IllegalArgumentException("Resolved path " + targetPath + " is outside of basePath " + basePathResolved);
        }

        return targetPath;
    }
}
