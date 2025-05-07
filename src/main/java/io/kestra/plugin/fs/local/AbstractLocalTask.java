package io.kestra.plugin.fs.local;

import io.kestra.core.models.tasks.Task;
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
    protected Path resolveLocalPath(String renderedPath) {
        return Paths.get(renderedPath).normalize();
    }
}
