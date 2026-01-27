package io.kestra.plugin.fs.vfs;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.fs.vfs.models.File;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class List extends AbstractVfsTask implements RunnableTask<List.Output> {
    @Schema(
        title = "The fully-qualified URIs that point to a path"
    )
    @NotNull
    protected Property<String> from;

    @Schema(
        title = "A regexp to filter on full path"
    )
    private Property<String> regExp;

    @Schema(
        title = "List files recursively"
    )
    @Builder.Default
    private Property<Boolean> recursive = Property.ofValue(false);

    @Builder.Default
    @Schema(
        title = "The maximum number of files to retrieve at once"
    )
    private Property<Integer> maxFiles = Property.ofValue(25);

    public Output run(RunContext runContext) throws Exception {
        try (StandardFileSystemManager fsm = new KestraStandardFileSystemManager(runContext)) {
            fsm.setConfiguration(StandardFileSystemManager.class.getResource(KestraStandardFileSystemManager.CONFIG_RESOURCE));
            fsm.init();

            Output output = VfsService.list(
                runContext,
                fsm,
                this.fsOptions(runContext),
                this.uri(runContext, runContext.render(this.from).as(String.class).orElseThrow()),
                runContext.render(this.regExp).as(String.class).orElse(null),
                runContext.render(this.recursive).as(Boolean.class).orElse(false)
            );

            java.util.List<File> files = output.getFiles();

            int rMaxFiles = runContext.render(this.maxFiles).as(Integer.class).orElse(25);
            if (files.size() > rMaxFiles) {
                runContext.logger().warn("Too many files to process ({}), limiting to {}", files.size(), rMaxFiles);
                files = files.subList(0, rMaxFiles);
            }

            return Output.builder()
                .files(files)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The list of files"
        )
        private final java.util.List<File> files;
    }
}
