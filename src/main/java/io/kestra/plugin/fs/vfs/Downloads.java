package io.kestra.plugin.fs.vfs;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.slf4j.Logger;

import java.net.URI;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

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
    @PluginProperty(dynamic = true)
    @NotNull
    private String from;

    @Schema(
        title = "The action to do on find files"
    )
    @PluginProperty(dynamic = true)
    private Downloads.Action action;

    @Schema(
        title = "The destination directory in case off `MOVE` "
    )
    @PluginProperty(dynamic = true)
    private String moveDirectory;

    @Schema(
        title = "A regexp to filter on full path"
    )
    @PluginProperty(dynamic = true)
    private String regExp;

    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        try (StandardFileSystemManager fsm = new StandardFileSystemManager()) {
            fsm.init();

            // path
            URI from = this.uri(runContext, this.from);

            // connection options
            FileSystemOptions fileSystemOptions = this.fsOptions(runContext);

            List.Output run = VfsService.list(
                runContext,
                fsm,
                fileSystemOptions,
                this.uri(runContext, this.from),
                this.regExp
            );

            java.util.List<io.kestra.plugin.fs.vfs.models.File> files = run
                .getFiles()
                .stream()
                .filter(file -> file.getFileType() == FileType.FILE)
                .collect(Collectors.toList());

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
                            this.host,
                            this.getPort(),
                            this.username,
                            this.password,
                            file.getPath().toString()
                        )
                    );

                    logger.debug("File '{}' download to '{}'", from.getPath(), download.getTo());

                    return file.withPath(download.getTo());
                }))
                .collect(Collectors.toList());

            if (this.action != null) {
                VfsService.archive(
                    runContext,
                    fsm,
                    fileSystemOptions,
                    files,
                    this.action,
                    this.uri(runContext, this.moveDirectory)
                );
            }

            return Downloads.Output
                .builder()
                .files(list)
                .build();
        }
    }

    public enum Action {
        MOVE,
        DELETE
    }


    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The bucket of the downloaded file"
        )
        @PluginProperty(additionalProperties = io.kestra.plugin.fs.vfs.models.File.class)
        private final java.util.List<io.kestra.plugin.fs.vfs.models.File> files;
    }
}
