package io.kestra.plugin.fs.nfs;
import com.fasterxml.jackson.annotation.JsonIgnore;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.inject.Inject;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Stream;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete a file on NFS",
    description = """
        Removes a file from an NFS mount. By default `errorOnMissing` is true and raises when the target is absent.

        When `regExp` is set, `uri` must point to a directory. All regular files whose absolute path matches the
        expression are deleted. Set `recursive` to true to also traverse subdirectories.
        """
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Delete a single file from an NFS mount.",
            code = """
                id: nfs_delete
                namespace: company.team

                tasks:
                  - id: delete_file
                    type: io.kestra.plugin.fs.nfs.Delete
                    uri: /mnt/nfs/shared/logs/old_log.txt
                    errorOnMissing: false
                """
        ),
        @Example(
            full = true,
            title = "Delete files matching a pattern in a directory.",
            code = """
                id: nfs_delete_logs
                namespace: company.team

                tasks:
                  - id: delete_logs
                    type: io.kestra.plugin.fs.nfs.Delete
                    uri: /mnt/nfs/shared/logs
                    regExp: '.*\\.log'
                    recursive: true
                    errorOnMissing: false
                """
        )
    }
)
public class Delete extends Task implements RunnableTask<Delete.Output> {

    @Inject
    @Builder.Default
    @PluginProperty(group = "advanced")
    @JsonIgnore
    private NfsService nfsService = NfsService.getInstance();

    @Schema(title = "Path of the file or directory to target")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> uri;

    @Schema(title = "Raise an error if the target is missing")
    @Builder.Default
    @PluginProperty(group = "reliability")
    private Property<Boolean> errorOnMissing = Property.ofValue(true);

    @Schema(
        title = "A regular expression to filter files for deletion",
        description = """
            When set, `uri` must point to a directory. The pattern is matched against the absolute OS path \
            of each file (for example, `/mnt/nfs/shared/logs/old.log`). Only regular files are deleted; \
            directories are skipped.
            """
    )
    @PluginProperty(group = "advanced")
    private Property<String> regExp;

    @Schema(
        title = "Include subdirectories",
        description = "If true, traverses subdirectories when deleting files matching `regExp`."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> recursive = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var logger = runContext.logger();

        var rUri = runContext.render(this.uri).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("`uri` cannot be null or empty"));
        var path = nfsService.toNfsPath(rUri);

        var rRegExp = runContext.render(this.regExp).as(String.class);

        if (rRegExp.isPresent()) {
            return deleteMatching(logger, path, rRegExp.get(), runContext);
        }

        return runSingleFile(logger, path, runContext);
    }

    private Output runSingleFile(Logger logger, Path path, RunContext runContext) throws Exception {
        var rErrorOnMissing = runContext.render(this.errorOnMissing).as(Boolean.class).orElse(true);

        boolean deleted;
        try {
            deleted = Files.deleteIfExists(path);
            if (!deleted) {
                if (rErrorOnMissing) {
                    throw new NoSuchFileException("File not found and 'errorOnMissing' is true: " + path);
                } else {
                    logger.debug("File not found '{}'", path);
                }
            } else {
                logger.debug("Deleted file '{}'", path);
            }
        } catch (NoSuchFileException e) {
            if (rErrorOnMissing) {
                throw e;
            }
            logger.debug("File not found '{}'", path);
            deleted = false;
        }

        return Output.builder()
            .uri(path.toUri())
            .deleted(deleted)
            .uris(List.of())
            .build();
    }

    private Output deleteMatching(Logger logger, Path path, String rRegExp, RunContext runContext) throws Exception {
        var rErrorOnMissing = runContext.render(this.errorOnMissing).as(Boolean.class).orElse(true);

        if (!Files.exists(path)) {
            if (rErrorOnMissing) {
                throw new NoSuchFileException("Directory not found and 'errorOnMissing' is true: " + path);
            }
            logger.debug("Directory not found '{}'", path);
            return Output.builder()
                .uri(path.toUri())
                .deleted(false)
                .uris(List.of())
                .build();
        }

        boolean rRecursive = runContext.render(this.recursive).as(Boolean.class).orElse(false);

        final Pattern pattern;
        try {
            pattern = Pattern.compile(rRegExp);
        } catch (PatternSyntaxException e) {
            throw new IllegalArgumentException("Invalid regExp '" + rRegExp + "': " + e.getMessage(), e);
        }

        List<URI> deleted = new ArrayList<>();
        try (Stream<Path> stream = rRecursive ? Files.walk(path) : Files.walk(path, 1)) {
            List<Path> matched = stream
                .filter(p -> !p.equals(path))
                .filter(p -> Files.isRegularFile(p) && pattern.matcher(p.toAbsolutePath().toString()).matches())
                .toList();

            logger.warn("Deleting {} file(s) matching regExp '{}' under '{}'", matched.size(), rRegExp, path);

            for (Path p : matched) {
                Files.delete(p);
                deleted.add(p.toUri());
                logger.debug("Deleted '{}'", p);
            }
        }

        return Output.builder()
            .uri(path.toUri())
            .deleted(!deleted.isEmpty())
            .uris(deleted)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The URI of the deleted file, or the target directory when regExp is used")
        private final URI uri;

        @Schema(title = "Whether at least one file was deleted")
        private final boolean deleted;

        @Schema(title = "URIs of all deleted files when regExp is used. Empty for single-file deletions")
        private final List<URI> uris;
    }
}
