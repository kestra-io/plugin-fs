package io.kestra.plugin.fs.local;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Examples;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.*;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.FileUtils;
import io.kestra.plugin.fs.local.models.File;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

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
@NoArgsConstructor
@ToString
@EqualsAndHashCode
@Getter
@Examples({
    @Example(
        title = "Download multiple files from a local directory",
        code = """
            id: download-multiple
            type: io.kestra.plugin.fs.local.Downloads
            from:
              - "file:///tmp/file1.txt"
              - "file:///tmp/file2.csv"
            """
    )
})
public class Downloads extends AbstractLocalTask implements RunnableTask<Downloads.Output> {

    @NotNull
    @Schema(
        title = "Paths to files to download",
        description = "List of full file paths (e.g., file:///tmp/my-file.txt)"
    )
    private Property<String> from;

    static void performAction(
        List<File> fileList,
        Action action,
        Property<String> moveDirectory,
        RunContext runContext
    ) throws Exception {
        if (action == Action.DELETE) {
            for (File file : fileList) {
                Delete delete = Delete.builder()
                    .id("archive")
                    .type(Delete.class.getName())
                    .build();
                delete.run(runContext);
            }
        } else if (action == Action.MOVE) {
            for (File file : fileList) {
                Move copy = Move.builder()
                    .id("archive")
                    .type(Copy.class.getName())
                    .from(Property.of(file.getLocalPath().toString()))
                    .to(Property.of(StringUtils.stripEnd(runContext.render(moveDirectory).as(String.class).orElseThrow() + "/", "/")
                        + "/" + FilenameUtils.getName(file.getName())
                    ))
                    .build();
                copy.run(runContext);
            }
        }
    }

    @Override
    public Output run(RunContext runContext) throws Exception {
        String renderedFrom = runContext.render(this.from).as(String.class).orElseThrow();

        io.kestra.plugin.fs.local.List task = io.kestra.plugin.fs.local.List.builder()
            .id(this.id)
            .type(List.class.getName())
            .from(this.from)
            .build();

        io.kestra.plugin.fs.local.List.Output run = task.run(runContext);

        List<File> list = run
            .getFiles()
            .stream()
            .map(throwFunction(fileItem -> {
                Path sourcePath = resolveLocalPath(renderedFrom);
                java.io.File tempFile = runContext.workingDir().createTempFile(FileUtils.getExtension(renderedFrom)).toFile();
                Files.copy(sourcePath, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                URI storageUri = runContext.storage().putFile(tempFile);

                return fileItem.withUri(storageUri);
                })
            ).toList();

        Map<String, URI> outputFiles = list.stream()
            .filter(file -> !file.isDirectory())
            .map(file -> new AbstractMap.SimpleEntry<>(file.getName(), file.getUri()))
            .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

        return Output.builder()
            .files(list)
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
            title = "URIs of the downloaded files in internal storage"
        )
        private final List<File> files;

        @Schema(
            title = "The downloaded files as a map of from/to URIs."
        )
        private final Map<String, URI> outputFiles;
    }
}
