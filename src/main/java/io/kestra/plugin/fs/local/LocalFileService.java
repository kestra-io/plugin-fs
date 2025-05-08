package io.kestra.plugin.fs.local;

import io.kestra.core.runners.RunContext;
import io.kestra.plugin.fs.vfs.Downloads;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public class LocalFileService {
    protected static final String USER_DIR = System.getProperty("user.home");

    public static io.kestra.plugin.fs.local.List.Output list(
        RunContext runContext,
        Path directory,
        String regExp,
        boolean recursive
    ) throws IOException {
        Logger logger = runContext.logger();
        List<io.kestra.plugin.fs.local.models.File> results = new ArrayList<>();

        if (!Files.exists(directory)) {
            throw new NoSuchFileException(directory.toString());
        }

        if (!Files.isDirectory(directory)) {
            throw new NotDirectoryException(directory.toString());
        }

        Pattern pattern = null;
        if (regExp != null && !regExp.isEmpty()) {
            pattern = Pattern.compile(regExp);
        }

        final Pattern finalPattern = pattern;

        try (Stream<Path> pathStream = recursive ?
            Files.walk(directory) :
            Files.list(directory)) {

            pathStream
                .forEach(path -> {
                    String absolutePath = path.toAbsolutePath().toString();

                    if (finalPattern == null || finalPattern.matcher(absolutePath).matches()) {
                        try {
                            results.add(
                                io.kestra.plugin.fs.local.models.File.builder()
                                    .localPath(path)
                                    .name(path.getFileName().toString())
                                    .parent(path.getParent().toString())
                                    .size(Files.isRegularFile(path) ? Files.size(path) : 0)
                                    .modifiedDate(Files.getLastModifiedTime(path).toInstant())
                                    .build()
                            );
                        } catch (IOException e) {
                            logger.warn("Unable to get file information for path: {}", path, e);
                        }
                    }
                });
        }

         return io.kestra.plugin.fs.local.List.Output.builder()
            .files(results)
            .count(results.size())
            .build();
    }

    public static Path download(RunContext runContext, Path sourcePath) throws IOException {
        String fileName = sourcePath.getFileName().toString();
        Path tempFile = Files.createTempFile("kestra-local-", "-" + fileName);

        Files.copy(sourcePath, tempFile, StandardCopyOption.REPLACE_EXISTING);

        return tempFile;
    }

    public static void performAction(
        RunContext runContext,
        List<io.kestra.plugin.fs.local.models.File> files,
        Downloads.Action action,
        Path moveDirectory
    ) throws IOException {
        Logger logger = runContext.logger();

        if (action == Downloads.Action.MOVE && (moveDirectory == null)) {
            throw new IllegalArgumentException("Move directory must be provided for MOVE action");
        }

        if (action == Downloads.Action.MOVE && !Files.exists(moveDirectory)) {
            Files.createDirectories(moveDirectory);
        }

        for (io.kestra.plugin.fs.local.models.File file : files) {
            Path sourcePath = file.getLocalPath();

            switch (action) {
                case DELETE:
                    Files.deleteIfExists(sourcePath);
                    logger.debug("Deleted file '{}'", sourcePath);
                    break;

                case MOVE:
                    Path targetPath = moveDirectory.resolve(sourcePath.getFileName());
                    Files.move(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
                    logger.debug("Moved file '{}' to '{}'", sourcePath, targetPath);
                    break;

                default:
                    break;
            }
        }
    }
}