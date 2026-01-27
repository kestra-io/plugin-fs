package io.kestra.plugin.fs.vfs;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.slf4j.Logger;

import java.net.URI;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class Downloads extends AbstractVfsTask implements RunnableTask<Downloads.Output> {
    @Schema(
        title = "The directory to list"
    )
    @NotNull
    private Property<String> from;

    @Schema(
        title = "The action to take on downloaded files"
    )
    private Property<Downloads.Action> action;

    @Schema(
        title = "The destination directory in case of `MOVE`"
    )
    private Property<String> moveDirectory;

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
        Logger logger = runContext.logger();

        try (StandardFileSystemManager fsm = new KestraStandardFileSystemManager(runContext)) {
            fsm.setConfiguration(StandardFileSystemManager.class.getResource(KestraStandardFileSystemManager.CONFIG_RESOURCE));
            fsm.init();

            // path
            URI from = this.uri(runContext, runContext.render(this.from).as(String.class).orElseThrow());

            // connection options
            FileSystemOptions fileSystemOptions = this.fsOptions(runContext);

            List.Output run = VfsService.list(
                runContext,
                fsm,
                fileSystemOptions,
                this.uri(runContext, runContext.render(this.from).as(String.class).orElseThrow()),
                runContext.render(this.regExp).as(String.class).orElse(null),
                runContext.render(this.recursive).as(Boolean.class).orElse(false)
            );

            java.util.List<io.kestra.plugin.fs.vfs.models.File> files = run
                .getFiles()
                .stream()
                .filter(file -> file.getFileType() == FileType.FILE)
                .toList();

            int rMaxFiles = runContext.render(this.maxFiles).as(Integer.class).orElse(25);
            if (files.size() > rMaxFiles) {
                logger.warn("Too many files to process ({}), limiting to {}", files.size(), rMaxFiles);
                files = files.subList(0, rMaxFiles);
            }

            java.util.List<io.kestra.plugin.fs.vfs.models.File> list = files
                .stream()
                .map(throwFunction(file -> {
                    Download.Output download = VfsService.download(
                        runContext,
                        fsm,
                        fileSystemOptions,
                        VfsService.uri(
                            runContext,
                            this.scheme(),
                            runContext.render(this.host).as(String.class).orElse(null),
                            runContext.render(this.getPort()).as(String.class).orElse(null),
                            runContext.render(this.username).as(String.class).orElse(null),
                            runContext.render(this.password).as(String.class).orElse(null),
                            file.getServerPath().getPath()
                        )
                    );

                    logger.debug("File '{}' download to '{}'", from.getPath(), download.getTo());

                    return file.withPath(download.getTo());
                }))
                .toList();

            Map<String, URI> outputFiles = list.stream()
                .filter(file -> file.getFileType() != FileType.FOLDER)
                .map(file -> new AbstractMap.SimpleEntry<>(file.getName(), file.getPath()))
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

            if (this.action != null) {
                VfsService.performAction(
                    runContext,
                    fsm,
                    fileSystemOptions,
                    files,
                    runContext.render(this.action).as(Downloads.Action.class).orElse(null),
                    this.uri(runContext, runContext.render(this.moveDirectory).as(String.class).orElse(null))
                );
            }

            return Downloads.Output
                .builder()
                .files(list)
                .outputFiles(outputFiles)
                .build();
        }
    }

    public enum Action {
        MOVE,
        DELETE,
        NONE
    }


    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Metadata of downloaded files."
        )
        private final java.util.List<io.kestra.plugin.fs.vfs.models.File> files;

        @Schema(
            title = "The downloaded files as a map of from/to URIs."
        )
        private final Map<String, URI> outputFiles;
    }
}
