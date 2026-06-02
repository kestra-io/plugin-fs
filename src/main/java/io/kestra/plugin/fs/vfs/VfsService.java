package io.kestra.plugin.fs.vfs;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.exceptions.KestraRuntimeException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.FileUtils;
import io.kestra.plugin.fs.vfs.models.File;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.apache.commons.vfs2.util.URIUtils;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.kestra.core.utils.Rethrow.throwFunction;

public abstract class VfsService {
    public static String basicAuth(String username, String password) {
        if (username != null && password != null) {
            return username + ":" + password;
        }

        return username;
    }

    public static URI uri(
        RunContext runContext,
        String scheme,
        String host,
        String port,
        String username,
        String password,
        String filepath
    ) throws IllegalVariableEvaluationException, URISyntaxException {
        return uri(
            scheme,
            runContext.render(host),
            port == null ? 22 : Integer.parseInt(runContext.render(port)),
            runContext.render(username),
            runContext.render(password),
            runContext.render(filepath)
        );
    }

    public static URI uri(
        String scheme,
        String host,
        Integer port,
        String username,
        String password,
        String filepath
    ) throws URISyntaxException {
        return new URI(
            scheme,
            basicAuth(username, password),
            host,
            port == null ? 22 : port,
            "/" + StringUtils.stripStart(filepath, "/"),
            null,
            null
        );
    }

    public static URI uriWithoutAuth(URI uri) throws URISyntaxException {
        return new URI(
            uri.getScheme(),
            uri.getHost(),
            uri.getPath(),
            uri.getQuery(),
            uri.getFragment()
        );
    }

    public static List.Output list(
        RunContext runContext,
        StandardFileSystemManager fsm,
        FileSystemOptions fileSystemOptions,
        URI from,
        String regExp,
        boolean recursive
    ) throws Exception {
        try (FileObject local = fsm.resolveFile(from.toString(), fileSystemOptions)) {
            FileObject[] children = local.findFiles(new FileSelector() {
                @Override
                public boolean traverseDescendents(FileSelectInfo file) {
                    // if not recursive only traverse "from"
                    return recursive || Objects.equals(file.getFile().getName().getPath(), local.getName().getPath());
                }

                @Override
                public boolean includeFile(FileSelectInfo file) throws Exception {
                    // Do not include directories in the result and apply user's filter
                    return file.getFile().isFile()
                        && (regExp == null || file.getFile().getName().getPath().matches(regExp));
                }
            });

            if (children == null) {
                return List.Output.builder()
                    .files(java.util.List.of())
                    .build();
            }

            java.util.List<File> list = Stream.of(children)
                .map(throwFunction(r -> File.of((AbstractFileObject<?>) r)))
                .toList();

            runContext.logger().debug("Found '{}' files from '{}'", list.size(), VfsService.uriWithoutAuth(from));

            return List.Output.builder()
                .files(list)
                .build();
        }
    }

    public static Download.Output download(VfsDownloadRequest request) throws Exception {
        RunContext runContext = request.runContext();
        URI from = request.from();
        java.io.File tempFile = runContext.workingDir().createTempFile(FileUtils.getExtension(from)).toFile();

        try (
            FileObject local = request.fsm().resolveFile(tempFile.toURI());
            FileObject remote = request.fsm().resolveFile(from.toString(), request.fileSystemOptions())
        ) {
            local.copyFrom(remote, Selectors.SELECT_SELF);
        }

        String checksum = request.validateChecksum()
            ? ChecksumService.verify(tempFile.toPath(), request.checksumAlgorithm(), request.checksumExpected())
            : ChecksumService.compute(tempFile.toPath(), request.checksumAlgorithm());

        URI storageUri = runContext.storage().putFile(tempFile);

        runContext.logger().debug("File '{}' download to '{}'", VfsService.uriWithoutAuth(from), storageUri);

        return Download.Output.builder()
            .from(VfsService.uriWithoutAuth(from))
            .to(storageUri)
            .checksum(checksum)
            .build();
    }

    public static Upload.Output upload(
        RunContext runContext,
        StandardFileSystemManager fsm,
        FileSystemOptions fileSystemOptions,
        URI from,
        URI to
    ) throws Exception {
        return upload(runContext, fsm, fileSystemOptions, from, to, true);
    }

    public static Upload.Output upload(
        RunContext runContext,
        StandardFileSystemManager fsm,
        FileSystemOptions fileSystemOptions,
        URI from,
        URI to,
        boolean overwrite
    ) throws Exception {
        // copy from to a temp files
        java.io.File tempFile = runContext.workingDir().createTempFile().toFile();

        // copy from to a temp file
        try (OutputStream outputStream = new FileOutputStream(tempFile)) {
            IOUtils.copy(runContext.storage().getFile(from), outputStream);
        }

        // upload
        try (FileObject local = fsm.resolveFile(tempFile.toURI());
             FileObject remote = fsm.resolveFile(to.toString(), fileSystemOptions)
        ) {
            //Avoid overriding a folder with a file when the remote folder exists
            if (!overwrite && remote.isFolder() && remote.exists() && !to.getPath().endsWith("/")) {
                throw new KestraRuntimeException(String.format(
                    """
                    Overwrite field is set to `false`. Folder %s will be overwritten with current file.
                    If you want the folder to be overwritten with the file, set `overwrite: true`.
                    """,
                    remote.getName().getPath()
                ));
            }
            //Fail when the destination file already exists and overwrite is disabled
            if (!overwrite && remote.exists() && !remote.isFolder()) {
                throw new KestraRuntimeException(String.format(
                    "File '%s' already exists in the remote server and cannot be overwritten. Set `overwrite: true` to replace it.",
                    remote.getName().getPath()
                ));
            }
            remote.copyFrom(local, Selectors.SELECT_SELF);
        }

        runContext.logger().debug("File '{}' uploaded to '{}'", VfsService.uriWithoutAuth(from), VfsService.uriWithoutAuth(to));

        return Upload.Output.builder()
            .from(from)
            .to(VfsService.uriWithoutAuth(to))
            .build();
    }

    public static Delete.Output delete(
        RunContext runContext,
        StandardFileSystemManager fsm,
        FileSystemOptions fileSystemOptions,
        URI from,
        Boolean errorOnMissing,
        Boolean recursive
    ) throws Exception {
        return delete(runContext, fsm, fileSystemOptions, from, errorOnMissing, recursive, null);
    }

    public static Delete.Output delete(
        RunContext runContext,
        StandardFileSystemManager fsm,
        FileSystemOptions fileSystemOptions,
        URI from,
        Boolean errorOnMissing,
        Boolean recursive,
        String regExp
    ) throws Exception {
        if (regExp != null) {
            try (FileObject dir = fsm.resolveFile(from.toString(), fileSystemOptions)) {
                if (!dir.exists()) {
                    if (Boolean.TRUE.equals(errorOnMissing)) {
                        throw new NoSuchElementException("Unable to find directory '" + VfsService.uriWithoutAuth(from) + "'");
                    }
                    return Delete.Output.builder()
                        .uri(VfsService.uriWithoutAuth(from))
                        .deleted(false)
                        .uris(java.util.List.of())
                        .build();
                }

                final Pattern pattern;
                try {
                    pattern = Pattern.compile(regExp);
                } catch (PatternSyntaxException e) {
                    throw new IllegalArgumentException("Invalid regExp '" + regExp + "': " + e.getMessage(), e);
                }

                FileObject[] matches = dir.findFiles(new FileSelector() {
                    @Override
                    public boolean traverseDescendents(FileSelectInfo file) {
                        return Boolean.TRUE.equals(recursive) || Objects.equals(file.getFile().getName().getPath(), dir.getName().getPath());
                    }

                    @Override
                    public boolean includeFile(FileSelectInfo file) throws Exception {
                        return file.getFile().isFile() && pattern.matcher(file.getFile().getName().getPath()).matches();
                    }
                });

                if (matches == null || matches.length == 0) {
                    runContext.logger().debug("No files matched regExp '{}' under '{}'", regExp, VfsService.uriWithoutAuth(from));
                    return Delete.Output.builder()
                        .uri(VfsService.uriWithoutAuth(from))
                        .deleted(false)
                        .uris(java.util.List.of())
                        .build();
                }

                runContext.logger().warn("Deleting {} file(s) matching regExp '{}' under '{}'", matches.length, regExp, VfsService.uriWithoutAuth(from));

                var deletedUris = new ArrayList<URI>();
                for (var match : matches) {
                    if (match.getType() == FileType.FOLDER) {
                        continue;
                    }
                    var matchUri = VfsService.uriWithoutAuth(new URI(match.getName().getURI()));
                    match.delete();
                    deletedUris.add(matchUri);
                    runContext.logger().debug("Deleted '{}'", matchUri);
                }

                return Delete.Output.builder()
                    .uri(VfsService.uriWithoutAuth(from))
                    .deleted(!deletedUris.isEmpty())
                    .uris(java.util.List.copyOf(deletedUris))
                    .build();
            }
        }

        try (FileObject local = fsm.resolveFile(from.toString(), fileSystemOptions)) {
            boolean exists = local.exists();

            if (!exists && Boolean.TRUE.equals(errorOnMissing)) {
                throw new NoSuchElementException("Unable to find file '" + VfsService.uriWithoutAuth(from) + "'");
            }

            boolean deleted = Boolean.TRUE.equals(recursive) ? local.deleteAll() > 0 : local.delete();

            if (exists) {
                runContext.logger().debug("Deleted path '{}'", VfsService.uriWithoutAuth(from));
            } else {
                runContext.logger().debug("Path doesn't exist '{}'", VfsService.uriWithoutAuth(from));
            }

            return Delete.Output.builder()
                .uri(VfsService.uriWithoutAuth(from))
                .deleted(deleted)
                .uris(java.util.List.of())
                .build();
        }
    }

    public static Move.Output move(
        RunContext runContext,
        StandardFileSystemManager fsm,
        FileSystemOptions fileSystemOptions,
        URI from,
        URI to,
        boolean overwrite
    ) throws Exception {
        // user pass a destination without filename, we add it
        if (!isDirectory(from) && isDirectory(to)) {
            to = to.resolve(URIUtils.encodePath(StringUtils.stripEnd(to.getPath(), "/") + "/" + FilenameUtils.getName(from.getPath())));
        }

        try (
            FileObject local = fsm.resolveFile(from.toString(), fileSystemOptions);
            FileObject remote = fsm.resolveFile(to.toString(), fileSystemOptions)
        ) {
            if (!local.exists()) {
                throw new NoSuchElementException("Unable to find file '" + VfsService.uriWithoutAuth(from) + "'");
            }

            if (remote.exists()) {
                String remoteFileName = FilenameUtils.getName(remote.getName().getPath());
                if (remoteFileName != null && remoteFileName.equals(FilenameUtils.getName(local.getName().getPath()))) {
                    if (overwrite) {
                        runContext.logger().warn("File '%s' already exists in the remote server and will be overwritten.");
                    } else {
                        throw new KestraRuntimeException(String.format("File '%s' already exists in the remote server and cannot be overwritten. If you want to ignore this, set `overwrite` to `true`.", remoteFileName));
                    }
                }
            } else {
                URI pathToCreate = to.resolve("/" + URIUtils.encodePath(FilenameUtils.getPath(to.getPath())));

                try (FileObject directory = fsm.resolveFile(to.toString(), fileSystemOptions)) {
                    if (!directory.exists()) {
                        directory.createFolder();
                        runContext.logger().debug("Create directory '{}", VfsService.uriWithoutAuth(pathToCreate));
                    }
                }
            }

            local.moveTo(remote);

            if (local.exists()) {
                runContext.logger().debug("Move file '{}'", VfsService.uriWithoutAuth(from));
            } else {
                runContext.logger().debug("File doesn't exists '{}'", VfsService.uriWithoutAuth(from));
            }

            return Move.Output.builder()
                .from(VfsService.uriWithoutAuth(from))
                .to(VfsService.uriWithoutAuth(to))
                .build();
        }
    }

    public static void performAction(
        RunContext runContext,
        StandardFileSystemManager fsm,
        FileSystemOptions fileSystemOptions,
        java.util.List<io.kestra.plugin.fs.vfs.models.File> blobList,
        Downloads.Action action,
        URI moveDirectory
    ) throws Exception {
        if (action == Downloads.Action.DELETE) {
            for (io.kestra.plugin.fs.vfs.models.File file : blobList) {
                VfsService.delete(
                    runContext,
                    fsm,
                    fileSystemOptions,
                    file.getServerPath(),
                    false,
                    false
                );
            }
        } else if (action == Downloads.Action.MOVE) {
            for (io.kestra.plugin.fs.vfs.models.File file : blobList) {
                //Destination should be considered as a directory
                if (!moveDirectory.getPath().endsWith("/")) {
                    moveDirectory = moveDirectory.resolve(URIUtils.encodePath(StringUtils.stripEnd(moveDirectory.getPath(), "/") + "/"));
                }
                VfsService.move(
                    runContext,
                    fsm,
                    fileSystemOptions,
                    file.getServerPath(),
                    moveDirectory,
                    true
                );
            }
        }
    }

    private static boolean isDirectory(URI uri) {
        return ("/" + FilenameUtils.getPath(uri.getPath())).equals(uri.getPath());
    }

    public static String extension(URI uri) {
        String path = uri.getPath();
        if (path == null) {
            return null;
        }

        if (path.indexOf('/') != -1) {
            path = path.substring(path.lastIndexOf('/')); // keep the last segment
        }
        if (path.indexOf('.') != -1) {
            return path.substring(path.indexOf('.'));
        }
        return null;
    }
}
