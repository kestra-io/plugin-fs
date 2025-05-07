package io.kestra.plugin.fs.local;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;
import java.nio.file.*;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Move a file within the local filesystem",
            code = """
                id: move_file
                namespace: company.team

                tasks:
                  - id: move
                    type: io.kestra.plugin.fs.local.Move
                    from: "input/data.csv"
                    to: "archive/data.csv"
                    basePath: "/Users/malay/desktop/kestra-output"
                    overwrite: true
                """
        )
    }
)
public class Move extends AbstractLocalTask implements RunnableTask<VoidOutput> {

    @NotNull
    @PluginProperty
    private Property<String> from;

    @NotNull
    @PluginProperty
    private Property<String> to;

    @PluginProperty
    @Builder.Default
    private Property<Boolean> overwrite = Property.of(false);

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        String renderedFrom = runContext.render(this.from).as(String.class).orElseThrow();
        String renderedTo = runContext.render(this.to).as(String.class).orElseThrow();

        Path sourcePath = resolveLocalPath(renderedFrom);
        Path targetPath = resolveLocalPath(renderedTo);

        if (!Files.exists(sourcePath)) {
            throw new IllegalArgumentException("Source file does not exist: " + sourcePath);
        }

        if (Files.exists(targetPath) && !runContext.render(overwrite).as(Boolean.class).orElse(false)) {
            throw new IllegalArgumentException("Target file already exists: " + targetPath +
                ". To avoid this error, configure `overwrite: true` to replace the existing file.");
        }

        Files.createDirectories(targetPath.getParent());

        Files.move(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);

        return null;
    }
}
