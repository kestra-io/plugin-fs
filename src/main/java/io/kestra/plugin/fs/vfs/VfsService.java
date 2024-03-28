package io.kestra.plugin.fs.vfs;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.fs.vfs.models.File;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.AbstractFileObject;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.NoSuchElementException;
import java.util.Objects;
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
    ) throws IllegalVariableEvaluationException, URISyntaxException {
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
                .filter(r -> regExp == null || r.getPath().toString().matches(regExp))
                .collect(Collectors.toList());

            runContext.logger().debug("Found '{}' files from '{}'", list.size(), VfsService.uriWithoutAuth(from));

            return List.Output.builder()
                .files(list)
                .build();
        }
    }

    public static Download.Output download(
        RunContext runContext,
        StandardFileSystemManager fsm,
        FileSystemOptions fileSystemOptions,
        URI from
    ) throws Exception {
        java.io.File tempFile = runContext.tempFile(extension(from)).toFile();

        try (
            FileObject local = fsm.resolveFile(tempFile.toURI());
            FileObject remote = fsm.resolveFile(from.toString(), fileSystemOptions)
        ) {
            local.copyFrom(remote, Selectors.SELECT_SELF);
        }

        URI storageUri = runContext.storage().putFile(tempFile);

        runContext.logger().debug("File '{}' download to '{}'", VfsService.uriWithoutAuth(from), storageUri);

        return Download.Output.builder()
            .from(VfsService.uriWithoutAuth(from))
            .to(storageUri)
            .build();
    }

    public static Upload.Output upload(
        RunContext runContext,
        StandardFileSystemManager fsm,
        FileSystemOptions fileSystemOptions,
        URI from,
        URI to
    ) throws Exception {
        // copy from to a temp files
        java.io.File tempFile = runContext.tempFile().toFile();

        // copy from to a temp file
        try (OutputStream outputStream = new FileOutputStream(tempFile)) {
            IOUtils.copy(runContext.storage().getFile(from), outputStream);
        }

        // upload
        try (FileObject local = fsm.resolveFile(tempFile.toURI());
             FileObject remote = fsm.resolveFile(to.toString(), fileSystemOptions);
        ) {
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
        Boolean errorOnMissing
    ) throws Exception {
        try (FileObject local = fsm.resolveFile(from.toString(), fileSystemOptions)) {
            if (!local.exists() && errorOnMissing) {
                throw new NoSuchElementException("Unable to find file '" + VfsService.uriWithoutAuth(from) + "'");
            }

            if (local.exists()) {
                runContext.logger().debug("Deleted file '{}'", VfsService.uriWithoutAuth(from));
            } else {
                runContext.logger().debug("File doesn't exists '{}'", VfsService.uriWithoutAuth(from));
            }

            return Delete.Output.builder()
                .uri(VfsService.uriWithoutAuth(from))
                .deleted(local.delete())
                .build();
        }
    }

    public static Move.Output move(
        RunContext runContext,
        StandardFileSystemManager fsm,
        FileSystemOptions fileSystemOptions,
        URI from,
        URI to
    ) throws Exception {
        // user pass a destination without filename, we add it
        if (!isDirectory(from) && isDirectory(to)) {
            to = to.resolve(StringUtils.stripEnd(to.getPath(), "/") + "/" + FilenameUtils.getName(from.getPath()));
        }

        try (
            FileObject local = fsm.resolveFile(from.toString(), fileSystemOptions);
            FileObject remote = fsm.resolveFile(to.toString(), fileSystemOptions);
        ) {
            if (!local.exists()) {
                throw new NoSuchElementException("Unable to find file '" + VfsService.uriWithoutAuth(from) + "'");
            }

            if (!remote.exists()) {
                URI pathToCreate = to.resolve("/" + FilenameUtils.getPath(to.getPath()));

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
                    false
                );
            }
        } else if (action == Downloads.Action.MOVE) {
            for (io.kestra.plugin.fs.vfs.models.File file : blobList) {
                VfsService.move(
                    runContext,
                    fsm,
                    fileSystemOptions,
                    file.getServerPath(),
                    moveDirectory
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
