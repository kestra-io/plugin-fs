package io.kestra.plugin.fs.local;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
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
            title = "Copy a file within the local filesystem",
            code = """
                id: copy_file
                namespace: company.team

                tasks:
                  - id: copy
                    type: io.kestra.plugin.fs.local.Copy
                    from: "input/data.csv"
                    to: "backup/data.csv"
                    basePath: "/Users/malay/desktop/kestra-output"
                    overwrite: true
                """
        )
    }
)
public class Copy extends AbstractLocalTask implements RunnableTask<Copy.Output> {

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
    public Output run(RunContext runContext) throws Exception {
        String renderedFrom = runContext.render(this.from).as(String.class).orElseThrow();
        String renderedTo = runContext.render(this.to).as(String.class).orElseThrow();
        String basePath = runContext.render(this.basePath).as(String.class).orElse(USER_DIR);

        Path sourcePath = resolveLocalPath(renderedFrom, basePath);
        Path targetPath = resolveLocalPath(renderedTo, basePath);

        if (!Files.exists(sourcePath)) {
            throw new IllegalArgumentException("Source file does not exist: " + sourcePath);
        }

        if (Files.exists(targetPath)) {
            if (runContext.render(overwrite).as(Boolean.class).orElse(false)) {
                Files.delete(targetPath);
            } else {
                throw new IllegalArgumentException("Target file already exists: " + targetPath);
            }
        } else {
            Files.createDirectories(targetPath.getParent());
        }

        Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);

        return Output.builder()
            .from(sourcePath.toString())
            .to(targetPath.toString())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        private String from;
        private String to;
    }
}
