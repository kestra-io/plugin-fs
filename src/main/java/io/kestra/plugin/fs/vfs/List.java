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
import io.kestra.core.models.annotations.PluginProperty;

import java.time.Instant;
import java.util.Comparator;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class List extends AbstractVfsTask implements RunnableTask<List.Output> {
    @Schema(
        title = "Directory URI to list"
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> from;

    @Schema(
        title = "Regexp filter on full path"
    )
    @PluginProperty(group = "advanced")
    private Property<String> regExp;

    @Schema(
        title = "List files recursively"
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> recursive = Property.ofValue(false);

    @Builder.Default
    @Schema(
        title = "Maximum files to retrieve"
    )
    @PluginProperty(group = "execution")
    private Property<Integer> maxFiles = Property.ofValue(25);

    @Builder.Default
    @Schema(
        title = "Sort order applied to the list before `maxFiles` truncation",
        description = """
            `NONE` (default) preserves the order in which the server returns files. `LAST_MODIFIED_ASC`/`LAST_MODIFIED_DESC` sort by last modified date, oldest/newest first. `NAME_ASC`/`NAME_DESC` sort alphabetically by file name."""
    )
    @PluginProperty(group = "processing")
    private Property<Sort> sort = Property.ofValue(Sort.NONE);

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

            Sort rSort = runContext.render(this.sort).as(Sort.class).orElse(Sort.NONE);
            Comparator<File> comparator = comparator(rSort);
            if (comparator != null) {
                files = files.stream().sorted(comparator).toList();
            }

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

    static Comparator<File> comparator(Sort sort) {
        return switch (sort) {
            case NONE -> null;
            case LAST_MODIFIED_ASC -> Comparator.comparing(File::getUpdatedDate, Comparator.nullsLast(Comparator.naturalOrder()));
            case LAST_MODIFIED_DESC -> Comparator.comparing(File::getUpdatedDate, Comparator.nullsLast(Comparator.<Instant>naturalOrder().reversed()));
            case NAME_ASC -> Comparator.comparing(File::getName, Comparator.nullsLast(Comparator.naturalOrder()));
            case NAME_DESC -> Comparator.comparing(File::getName, Comparator.nullsLast(Comparator.<String>naturalOrder().reversed()));
        };
    }

    public enum Sort {
        NONE,
        LAST_MODIFIED_ASC,
        LAST_MODIFIED_DESC,
        NAME_ASC,
        NAME_DESC
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
