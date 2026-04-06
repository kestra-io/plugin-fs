package io.kestra.plugin.fs.smb;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.fs.vfs.models.File;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.FileType;
import java.net.URI;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;
import io.kestra.core.models.annotations.PluginProperty;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Download multiple files over SMB",
    description = "Lists matching files then downloads them to internal storage. Respects `maxFiles` (default 25) and optional post-download action (MOVE/DELETE). Default port 445."
)
@Plugin(
    examples = {
        @Example(
            title = "Download files from `my_share` and move them to an `archive_share`",
            full = true,
            code = """
                id: fs_smb_downloads
                namespace: company.team

                tasks:
                  - id: downloads
                    type: io.kestra.plugin.fs.smb.Downloads
                    host: localhost
                    port: "445"
                    username: foo
                    password: "{{ secret('SMB_PASSWORD') }}"
                    from: "/my_share/"
                    action: MOVE
                    moveDirectory: "/archive_share/"
                """
        )
    }
)
public class Downloads extends AbstractSmbTask implements RunnableTask<Downloads.Output> {
    @Schema(
        title = "Directory URI to list"
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> from;

    @Schema(
        title = "Action on downloaded files"
    )
    private Property<io.kestra.plugin.fs.vfs.Downloads.Action> action;

    @Schema(
        title = "Destination directory when action is MOVE"
    )
    @PluginProperty(group = "destination")
    private Property<String> moveDirectory;

    @Schema(
        title = "Regexp filter on full path"
    )
    @PluginProperty(group = "processing")
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
    @PluginProperty(group = "processing")
    private Property<Integer> maxFiles = Property.ofValue(25);

    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();

        var ctx = createContext(runContext);
        try {
            var fromPath = runContext.render(this.from).as(String.class).orElseThrow();

            var run = SmbService.list(
                runContext,
                ctx,
                this,
                fromPath,
                runContext.render(this.regExp).as(String.class).orElse(null),
                runContext.render(this.recursive).as(Boolean.class).orElse(false)
            );

            var files = run.getFiles().stream()
                .filter(file -> file.getFileType() == FileType.FILE)
                .toList();

            var rMaxFiles = runContext.render(this.maxFiles).as(Integer.class).orElse(25);
            if (files.size() > rMaxFiles) {
                logger.warn("Too many files to process ({}), limiting to {}", files.size(), rMaxFiles);
                files = files.subList(0, rMaxFiles);
            }

            var list = files.stream()
                .map(throwFunction(file -> {
                    var download = SmbService.download(
                        runContext,
                        ctx,
                        this,
                        file.getServerPath().getPath()
                    );

                    logger.debug("File '{}' download to '{}'", fromPath, download.getTo());

                    return file.withPath(download.getTo());
                }))
                .toList();

            var outputFiles = list.stream()
                .filter(file -> file.getFileType() != FileType.FOLDER)
                .map(file -> new AbstractMap.SimpleEntry<>(file.getName(), file.getPath()))
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

            if (this.action != null) {
                var rAction = runContext.render(this.action).as(io.kestra.plugin.fs.vfs.Downloads.Action.class).orElse(null);
                SmbService.performAction(
                    runContext,
                    ctx,
                    this,
                    files,
                    rAction,
                    runContext.render(this.moveDirectory).as(String.class).orElse(null)
                );
            }

            return Output.builder()
                .files(list)
                .outputFiles(outputFiles)
                .build();
        } finally {
            ctx.close();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Metadata of downloaded files."
        )
        private final java.util.List<File> files;

        @Schema(
            title = "The downloaded files as a map of from/to URIs."
        )
        private final Map<String, URI> outputFiles;
    }
}
