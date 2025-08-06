package io.kestra.plugin.fs.local;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.*;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.FileUtils;
import io.kestra.plugin.fs.local.models.File;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Download multiple files from a local filesystem directory to Kestra's internal storage."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: fs_local_downloads
                namespace: company.team

                tasks:
                  - id: downloads
                    type: io.kestra.plugin.fs.local.Downloads
                    from: "/data/files/"
                    regExp: ".*.csv"
                    recursive: true
                    action: NONE
                    allowedPaths:
                      - /data/files
                """
        ),
        @Example(
            full = true,
            code = """
                id: fs_local_downloads_move
                namespace: company.team

                tasks:
                  - id: downloads_move
                    type: io.kestra.plugin.fs.local.Downloads
                    from: /data/files/
                    regExp: ".*.csv"
                    recursive: true
                    action: MOVE
                    moveDirectory: "/data/archive"
                    allowedPaths:
                      - /data/files
                      - /data/archive
                """
        )
    }
)
public class Downloads extends AbstractLocalTask implements RunnableTask<Downloads.Output> {

    @Schema(
        title = "The directory to list"
    )
    @NotNull
    private Property<String> from;

    @Schema(
        title = "The action to take on downloaded files"
    )
    @Builder.Default
    private Property<Downloads.Action> action = Property.ofValue(Downloads.Action.NONE);

    @Schema(
        title = "The destination directory in case of `MOVE`"
    )
    private Property<String> moveDirectory;

    @Schema(
        title = "A regexp to filter on full path"
    )
    private Property<String> regExp;

    @Schema(
        title = "List file recursively"
    )
    @Builder.Default
    private Property<Boolean> recursive = Property.ofValue(false);

    static void performAction(
        java.util.List<File> files,
        Action action,
        Property<String> moveDirectory,
        RunContext runContext) throws Exception {
        if (action == Action.DELETE) {
            for (File file : files) {
                if (!file.isDirectory()) {
                    Delete delete = Delete.builder()
                        .id(Delete.class.getSimpleName())
                        .type(Delete.class.getName())
                        .from(Property.ofValue(file.getLocalPath().toString()))
                        .build();
                    delete.run(runContext);
                }
            }
        } else if (action == Action.MOVE) {
            if (moveDirectory == null) {
                throw new IllegalArgumentException("moveDirectory must be specified when action is MOVE");
            }

            String rMoveDirectory = runContext.render(moveDirectory).as(String.class).orElseThrow();

            if (!rMoveDirectory.endsWith("/")) {
                rMoveDirectory = rMoveDirectory + "/";
            }

            for (File file : files) {
                if (!file.isDirectory()) {
                    String fileName = file.getLocalPath().getFileName().toString();
                    String destinationPath = rMoveDirectory + fileName;

                    Move move = Move.builder()
                        .id(Move.class.getSimpleName())
                        .type(Move.class.getName())
                        .from(Property.ofValue(file.getLocalPath().toString()))
                        .to(Property.ofValue(destinationPath))
                        .overwrite(Property.ofValue(true))
                        .build();
                    move.run(runContext);
                }
            }
        }
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        String renderedFrom = runContext.render(this.from).as(String.class).orElseThrow();

        io.kestra.plugin.fs.local.List listTask = io.kestra.plugin.fs.local.List.builder()
            .id(io.kestra.plugin.fs.local.List.class.getSimpleName())
            .type(io.kestra.plugin.fs.local.List.class.getName())
            .from(Property.ofValue(renderedFrom))
            .regExp(this.regExp)
            .recursive(this.recursive)
            .build();

        io.kestra.plugin.fs.local.List.Output listOutput = listTask.run(runContext);

        List<File> downloadedFiles = listOutput
            .getFiles()
            .stream()
            .map(throwFunction(fileItem -> {
                if (fileItem.isDirectory()) {
                    return fileItem;
                }

                Download downloadTask = Download.builder()
                    .id(Download.class.getSimpleName())
                    .type(Download.class.getName())
                    .from(Property.ofValue(fileItem.getLocalPath().toString()))
                    .build();

                Download.Output downloadOutput = downloadTask.run(runContext);

                return fileItem.withUri(downloadOutput.getUri());
            }))
            .toList();

        java.util.List<File> filesToProcess = downloadedFiles.stream()
            .filter(file -> !file.isDirectory())
            .toList();

        Action selectedAction = this.action != null ?
            runContext.render(this.action).as(Action.class).orElse(Action.NONE) :
            Action.NONE;

        if (selectedAction != Action.NONE) {
            performAction(filesToProcess, selectedAction, this.moveDirectory, runContext);
        }

        Map<String, URI> outputFiles = downloadedFiles.stream()
            .filter(file -> !file.isDirectory() && file.getUri() != null)
            .map(file -> new AbstractMap.SimpleEntry<>(file.getLocalPath().toString(), file.getUri()))
            .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

        return Output.builder()
            .files(downloadedFiles)
            .outputFiles(outputFiles)
            .build();
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
        private final List<File> files;

        @Schema(
            title = "The downloaded files formatted as a map of from/to URIs"
        )
        private final Map<String, URI> outputFiles;
    }
}
