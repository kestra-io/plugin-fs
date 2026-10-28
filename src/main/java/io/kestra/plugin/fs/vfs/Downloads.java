package io.kestra.plugin.fs.vfs;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.fs.vfs.models.File;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.slf4j.Logger;

import java.net.URI;
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
        title = "Directory URI to list"
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> from;

    @Schema(
        title = "Action on downloaded files"
    )
    private Property<Downloads.Action> action;

    @Schema(
        title = "Destination directory when action is MOVE",
        description = """
            The directory where files are moved after action.
            When `recursive` is `true`, the relative path from the `from` directory is preserved under `moveDirectory`.
            """
    )
    @PluginProperty(group = "advanced")
    private Property<String> moveDirectory;

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

    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        try (StandardFileSystemManager fsm = new KestraStandardFileSystemManager(runContext)) {
            fsm.setConfiguration(StandardFileSystemManager.class.getResource(KestraStandardFileSystemManager.CONFIG_RESOURCE));
            fsm.init();

            // path
            var rAction = runContext.render(this.action).as(Downloads.Action.class).orElse(null);
            var rMoveDirectory = runContext.render(this.moveDirectory).as(String.class).orElse(null);
            if (rAction == Action.MOVE && (rMoveDirectory == null || rMoveDirectory.isBlank())) {
                throw new IllegalArgumentException("moveDirectory must be specified when action is MOVE");
            }

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
                    Download.Output download = VfsService.download(VfsDownloadRequest.of(
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
                    ));

                    logger.debug("File '{}' download to '{}'", from.getPath(), download.getTo());

                    return file.withPath(download.getTo());
                }))
                .toList();

            var outputFiles = list.stream()
                .filter(file -> file.getFileType() != FileType.FOLDER)
                .collect(Collectors.toMap(
                    file -> relativePath(from, file),
                    file -> file,
                    (f1, f2) -> {
                        var p1 = f1.getServerPath() != null && f1.getServerPath().getPath() != null
                            ? f1.getServerPath().getPath()
                            : f1.getName();
                        var p2 = f2.getServerPath() != null && f2.getServerPath().getPath() != null
                            ? f2.getServerPath().getPath()
                            : f2.getName();
                        throw new IllegalStateException(String.format(
                            "Cannot generate unique outputFiles key: duplicate key for files '%s' and '%s'.",
                            p1, p2
                        ));
                    }
                ))
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getPath()));

            if (this.action != null) {
                VfsService.performAction(
                    runContext,
                    fsm,
                    fileSystemOptions,
                    files,
                    rAction,
                    this.uri(runContext, rMoveDirectory),
                    from
                );
            }

            return Downloads.Output
                .builder()
                .files(list)
                .outputFiles(outputFiles)
                .build();
        }
    }

    static String relativePath(URI from, File file) {
        var filePath = file.getServerPath() != null && file.getServerPath().getPath() != null
            ? file.getServerPath().getPath()
            : null;

        var fromPath = from != null ? from.getPath() : null;
        if (fromPath == null || filePath == null) {
            return file.getName();
        }

        var normalizedFrom = normalizePath(fromPath);
        var normalizedFile = normalizePath(filePath);

        if (normalizedFrom == null || normalizedFile == null) {
            return file.getName();
        }

        if (!normalizedFrom.endsWith("/")) {
            normalizedFrom += "/";
        }

        if (normalizedFile.startsWith(normalizedFrom)) {
            return normalizedFile.substring(normalizedFrom.length());
        }

        return file.getName();
    }

    private static String normalizePath(String path) {
        if (path == null || path.isBlank()) {
            return null;
        }

        var p = path.startsWith("/") ? path : "/" + path;
        p = p.replaceAll("/+", "/");
        var normalized = FilenameUtils.normalizeNoEndSeparator(p, true);
        if (normalized == null) {
            return p;
        }
        return normalized.isEmpty() ? "/" : normalized;
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
            title = "Metadata of downloaded files"
        )
        private final java.util.List<io.kestra.plugin.fs.vfs.models.File> files;

        @Schema(
            title = "The downloaded files as a map of from/to URIs",
            description = """
                The downloaded files as a map of from/to URIs.
                Keys are paths relative to `from` (the file name for files directly under `from`, or subfolder paths for recursive downloads)."""
        )
        private final Map<String, URI> outputFiles;
    }
}
