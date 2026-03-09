package io.kestra.plugin.fs.smb;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.exceptions.KestraRuntimeException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.FileUtils;
import io.kestra.plugin.fs.vfs.models.File;
import org.codelibs.jcifs.smb.CIFSContext;
import org.codelibs.jcifs.smb.context.BaseContext;
import org.codelibs.jcifs.smb.config.PropertyConfiguration;
import org.codelibs.jcifs.smb.impl.NtlmPasswordAuthenticator;
import org.codelibs.jcifs.smb.impl.SmbFile;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileType;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.*;

public abstract class SmbService {

    public static CIFSContext createContext(RunContext runContext, SmbInterface smbInterface) throws Exception {
        var props = new Properties();
        props.setProperty("jcifs.smb.client.enableSMB2", "true");
        props.setProperty("jcifs.smb.client.disableSMB1", "false");

        var baseContext = new BaseContext(new PropertyConfiguration(props));

        var rUsername = runContext.render(smbInterface.getUsername()).as(String.class).orElse(null);
        var rPassword = runContext.render(smbInterface.getPassword()).as(String.class).orElse(null);

        if (rUsername != null) {
            var auth = new NtlmPasswordAuthenticator("", rUsername, rPassword != null ? rPassword : "");
            return baseContext.withCredentials(auth);
        }

        return baseContext;
    }

    public static String smbUrl(RunContext runContext, SmbInterface smbInterface, String filepath) throws IllegalVariableEvaluationException {
        var rHost = runContext.render(smbInterface.getHost()).as(String.class).orElseThrow();
        var rPort = runContext.render(smbInterface.getPort()).as(String.class).orElse("445");

        // SMB URLs: smb://host:port/share/path
        var cleanPath = StringUtils.stripStart(filepath, "/");

        var sb = new StringBuilder("smb://");
        sb.append(rHost);
        if (rPort != null && !rPort.equals("445")) {
            sb.append(":").append(rPort);
        }
        sb.append("/").append(cleanPath);

        return sb.toString();
    }

    public static URI serverPathUri(String host, String port, String filepath) throws URISyntaxException {
        var cleanPath = "/" + StringUtils.stripStart(filepath, "/");
        var portNum = port != null ? Integer.parseInt(port) : 445;
        return new URI("smb", null, host, portNum, cleanPath, null, null);
    }

    // --- Operations ---

    public static io.kestra.plugin.fs.vfs.List.Output list(
        RunContext runContext,
        CIFSContext ctx,
        SmbInterface smbInterface,
        String from,
        String regExp,
        boolean recursive
    ) throws Exception {
        var url = smbUrl(runContext, smbInterface, from);
        if (!url.endsWith("/")) {
            url += "/";
        }

        var rHost = runContext.render(smbInterface.getHost()).as(String.class).orElseThrow();
        var rPort = runContext.render(smbInterface.getPort()).as(String.class).orElse("445");

        try (var dir = new SmbFile(url, ctx)) {
            if (!dir.exists()) {
                return io.kestra.plugin.fs.vfs.List.Output.builder()
                    .files(java.util.List.of())
                    .build();
            }

            var files = new ArrayList<File>();

            if (dir.isFile()) {
                // Path points to a file, not a directory: return it as a single-element list
                var file = smbFileToFile(dir, rHost, rPort);
                if (regExp == null || extractPath(dir).matches(regExp)) {
                    files.add(file);
                }
            } else {
                collectFiles(ctx, dir, regExp, recursive, files, rHost, rPort);
            }

            runContext.logger().debug("Found '{}' files from '{}'", files.size(), from);

            return io.kestra.plugin.fs.vfs.List.Output.builder()
                .files(files)
                .build();
        }
    }

    private static void collectFiles(
        CIFSContext ctx,
        SmbFile dir,
        String regExp,
        boolean recursive,
        java.util.List<File> result,
        String host,
        String port
    ) throws Exception {
        var children = dir.listFiles();
        if (children == null) return;

        for (var child : children) {
            try {
                if (child.isDirectory()) {
                    if (recursive) {
                        collectFiles(ctx, child, regExp, recursive, result, host, port);
                    }
                } else if (child.isFile()) {
                    var path = extractPath(child);
                    if (regExp == null || path.matches(regExp)) {
                        result.add(smbFileToFile(child, host, port));
                    }
                }
            } finally {
                child.close();
            }
        }
    }

    public static io.kestra.plugin.fs.vfs.Download.Output download(
        RunContext runContext,
        CIFSContext ctx,
        SmbInterface smbInterface,
        String filepath
    ) throws Exception {
        var url = smbUrl(runContext, smbInterface, filepath);
        var ext = FileUtils.getExtension(filepath);
        var tempFile = runContext.workingDir().createTempFile(ext).toFile();

        try (var remote = new SmbFile(url, ctx);
             var in = remote.getInputStream();
             var out = new FileOutputStream(tempFile)) {
            IOUtils.copy(in, out);
        }

        var storageUri = runContext.storage().putFile(tempFile);

        var rHost = runContext.render(smbInterface.getHost()).as(String.class).orElseThrow();
        var rPort = runContext.render(smbInterface.getPort()).as(String.class).orElse("445");
        var fromUri = serverPathUri(rHost, rPort, filepath);

        runContext.logger().debug("File '{}' download to '{}'", filepath, storageUri);

        return io.kestra.plugin.fs.vfs.Download.Output.builder()
            .from(fromUri)
            .to(storageUri)
            .build();
    }

    public static io.kestra.plugin.fs.vfs.Upload.Output upload(
        RunContext runContext,
        CIFSContext ctx,
        SmbInterface smbInterface,
        URI from,
        String toPath
    ) throws Exception {
        return upload(runContext, ctx, smbInterface, from, toPath, false);
    }

    public static io.kestra.plugin.fs.vfs.Upload.Output upload(
        RunContext runContext,
        CIFSContext ctx,
        SmbInterface smbInterface,
        URI from,
        String toPath,
        boolean overwrite
    ) throws Exception {
        var url = smbUrl(runContext, smbInterface, toPath);

        // Ensure parent directories exist
        var parentUrl = url.substring(0, url.lastIndexOf('/') + 1);
        try (var parentDir = new SmbFile(parentUrl, ctx)) {
            if (!parentDir.exists()) {
                parentDir.mkdirs();
            }
        }

        try (var remote = new SmbFile(url, ctx)) {
            // Check if destination is a folder that would be overwritten
            if (!overwrite && remote.exists() && remote.isDirectory() && !toPath.endsWith("/")) {
                throw new KestraRuntimeException(String.format(
                    """
                    Overwrite field is set to `false`. Folder %s will be overwritten with current file.
                    If you want the folder to be overwritten with the file, set `overwrite: true`.
                    """,
                    extractPath(remote)
                ));
            }

            // If overwrite and destination is a directory, delete it recursively first
            if (overwrite && remote.exists() && remote.isDirectory()) {
                // Ensure we use a directory URL (ending with /) for proper listing
                var dirUrl = url.endsWith("/") ? url : url + "/";
                try (var dirFile = new SmbFile(dirUrl, ctx)) {
                    deleteRecursively(dirFile);
                }
            }

            try (var in = runContext.storage().getFile(from);
                 var out = remote.getOutputStream()) {
                IOUtils.copy(in, out);
            }
        }

        var rHost = runContext.render(smbInterface.getHost()).as(String.class).orElseThrow();
        var rPort = runContext.render(smbInterface.getPort()).as(String.class).orElse("445");
        var toUri = serverPathUri(rHost, rPort, toPath);

        runContext.logger().debug("File '{}' uploaded to '{}'", from, toUri);

        return io.kestra.plugin.fs.vfs.Upload.Output.builder()
            .from(from)
            .to(toUri)
            .build();
    }

    public static io.kestra.plugin.fs.vfs.Delete.Output delete(
        RunContext runContext,
        CIFSContext ctx,
        SmbInterface smbInterface,
        String filepath,
        Boolean errorOnMissing
    ) throws Exception {
        var url = smbUrl(runContext, smbInterface, filepath);
        var rHost = runContext.render(smbInterface.getHost()).as(String.class).orElseThrow();
        var rPort = runContext.render(smbInterface.getPort()).as(String.class).orElse("445");
        var fileUri = serverPathUri(rHost, rPort, filepath);

        try (var remote = new SmbFile(url, ctx)) {
            if (!remote.exists()) {
                if (Boolean.TRUE.equals(errorOnMissing)) {
                    throw new NoSuchElementException("Unable to find file '" + fileUri + "'");
                }
                runContext.logger().debug("File doesn't exist '{}'", filepath);
                return io.kestra.plugin.fs.vfs.Delete.Output.builder()
                    .uri(fileUri)
                    .deleted(false)
                    .build();
            }

            remote.delete();
            runContext.logger().debug("Deleted file '{}'", filepath);
            return io.kestra.plugin.fs.vfs.Delete.Output.builder()
                .uri(fileUri)
                .deleted(true)
                .build();
        }
    }

    public static io.kestra.plugin.fs.vfs.Move.Output move(
        RunContext runContext,
        CIFSContext ctx,
        SmbInterface smbInterface,
        String fromPath,
        String toPath,
        boolean overwrite
    ) throws Exception {
        // If source is a file and destination looks like a directory, append the filename
        var fromIsDir = fromPath.endsWith("/");
        var toIsDir = toPath.endsWith("/");

        if (!fromIsDir && toIsDir) {
            var fileName = FilenameUtils.getName(fromPath);
            toPath = StringUtils.stripEnd(toPath, "/") + "/" + fileName;
        }

        var fromUrl = smbUrl(runContext, smbInterface, fromPath);
        var toUrl = smbUrl(runContext, smbInterface, toPath);

        var rHost = runContext.render(smbInterface.getHost()).as(String.class).orElseThrow();
        var rPort = runContext.render(smbInterface.getPort()).as(String.class).orElse("445");

        // If source and destination are the same, nothing to do
        if (fromUrl.equals(toUrl)) {
            var fromUri = serverPathUri(rHost, rPort, fromPath);
            var toUri = serverPathUri(rHost, rPort, toPath);
            return io.kestra.plugin.fs.vfs.Move.Output.builder()
                .from(fromUri)
                .to(toUri)
                .build();
        }

        try (var source = new SmbFile(fromUrl, ctx)) {
            if (!source.exists()) {
                var fromUri = serverPathUri(rHost, rPort, fromPath);
                throw new NoSuchElementException("Unable to find file '" + fromUri + "'");
            }

            try (var dest = new SmbFile(toUrl, ctx)) {
                if (dest.exists()) {
                    var destFileName = FilenameUtils.getName(extractPath(dest));
                    var srcFileName = FilenameUtils.getName(extractPath(source));
                    if (destFileName != null && destFileName.equals(srcFileName)) {
                        if (overwrite) {
                            runContext.logger().warn("File '{}' already exists in the remote server and will be overwritten.", destFileName);
                            deleteRecursively(dest);
                        } else {
                            throw new KestraRuntimeException(String.format(
                                "File '%s' already exists in the remote server and cannot be overwritten. If you want to ignore this, set `overwrite` to `true`.",
                                destFileName
                            ));
                        }
                    }
                } else {
                    // Create parent directories (strip trailing slash to get actual parent)
                    var trimmedUrl = toUrl.endsWith("/") ? toUrl.substring(0, toUrl.length() - 1) : toUrl;
                    var parentUrl = trimmedUrl.substring(0, trimmedUrl.lastIndexOf('/') + 1);
                    try (var parentDir = new SmbFile(parentUrl, ctx)) {
                        if (!parentDir.exists()) {
                            parentDir.mkdirs();
                            runContext.logger().debug("Create directory '{}'", parentUrl);
                        }
                    }
                }

                // Detect cross-share move: extract share names from URLs
                var fromShare = extractShareName(fromUrl);
                var toShare = extractShareName(toUrl);
                if (fromShare != null && toShare != null && !fromShare.equals(toShare)) {
                    // Cross-share move: renameTo won't work, use copy + delete
                    copyRecursively(ctx, source, dest);
                    deleteRecursively(source);
                } else {
                    source.renameTo(dest);
                }
            }
        }

        var fromUri = serverPathUri(rHost, rPort, fromPath);
        var toUri = serverPathUri(rHost, rPort, toPath);

        return io.kestra.plugin.fs.vfs.Move.Output.builder()
            .from(fromUri)
            .to(toUri)
            .build();
    }

    public static void performAction(
        RunContext runContext,
        CIFSContext ctx,
        SmbInterface smbInterface,
        java.util.List<File> blobList,
        io.kestra.plugin.fs.vfs.Downloads.Action action,
        String moveDirectory
    ) throws Exception {
        if (action == io.kestra.plugin.fs.vfs.Downloads.Action.DELETE) {
            for (File file : blobList) {
                delete(runContext, ctx, smbInterface, file.getServerPath().getPath(), false);
            }
        } else if (action == io.kestra.plugin.fs.vfs.Downloads.Action.MOVE) {
            for (File file : blobList) {
                var moveTo = moveDirectory;
                if (!moveTo.endsWith("/")) {
                    moveTo += "/";
                }
                move(runContext, ctx, smbInterface, file.getServerPath().getPath(), moveTo, true);
            }
        }
    }

    // --- Helpers ---

    /**
     * Extract the share name from an SMB URL (e.g., "smb://host/share/path" → "share").
     */
    private static String extractShareName(String smbUrl) {
        // smb://host/share/... or smb://host:port/share/...
        var schemeEnd = smbUrl.indexOf("://");
        if (schemeEnd < 0) return null;
        var hostEnd = smbUrl.indexOf('/', schemeEnd + 3);
        if (hostEnd < 0) return null;
        var shareEnd = smbUrl.indexOf('/', hostEnd + 1);
        if (shareEnd < 0) return smbUrl.substring(hostEnd + 1);
        return smbUrl.substring(hostEnd + 1, shareEnd);
    }

    /**
     * Recursively delete an SmbFile (file or directory).
     */
    private static void deleteRecursively(SmbFile file) throws Exception {
        if (file.isDirectory()) {
            var children = file.listFiles();
            if (children != null) {
                for (var child : children) {
                    try {
                        deleteRecursively(child);
                    } finally {
                        child.close();
                    }
                }
            }
        }
        file.delete();
    }

    /**
     * Recursively copy an SmbFile (file or directory) to a destination.
     */
    private static void copyRecursively(CIFSContext ctx, SmbFile source, SmbFile dest) throws Exception {
        if (source.isDirectory()) {
            if (!dest.exists()) {
                dest.mkdirs();
            }
            var children = source.listFiles();
            if (children != null) {
                for (var child : children) {
                    var childName = child.getName();
                    try (var childDest = new SmbFile(dest.getCanonicalPath() + childName, ctx)) {
                        copyRecursively(ctx, child, childDest);
                    } finally {
                        child.close();
                    }
                }
            }
        } else {
            // Ensure parent directory exists
            var destUrl = dest.getCanonicalPath();
            var parentUrl = destUrl.substring(0, destUrl.lastIndexOf('/') + 1);
            try (var parentDir = new SmbFile(parentUrl, ctx)) {
                if (!parentDir.exists()) {
                    parentDir.mkdirs();
                }
            }
            try (var in = source.getInputStream();
                 var out = dest.getOutputStream()) {
                IOUtils.copy(in, out);
            }
        }
    }

    /**
     * Extract the logical path from an SmbFile URL (share + path portion).
     */
    static String extractPath(SmbFile smbFile) {
        // SmbFile.getCanonicalPath() returns smb://host/share/path/
        // We want /share/path
        var urlStr = smbFile.getCanonicalPath();
        var schemeEnd = urlStr.indexOf("://");
        if (schemeEnd >= 0) {
            var hostEnd = urlStr.indexOf('/', schemeEnd + 3);
            if (hostEnd >= 0) {
                var path = urlStr.substring(hostEnd);
                // Remove trailing slash for files
                if (path.endsWith("/") && path.length() > 1) {
                    try {
                        if (!smbFile.isDirectory()) {
                            path = path.substring(0, path.length() - 1);
                        }
                    } catch (Exception e) {
                        // If we can't check, strip it
                        path = path.substring(0, path.length() - 1);
                    }
                }
                return path;
            }
        }
        return urlStr;
    }

    static File smbFileToFile(SmbFile smbFile, String host, String port) throws Exception {
        var path = extractPath(smbFile);

        var serverPath = serverPathUri(host, port, path);

        var builder = File.builder()
            .path(new URI(null, path, null))
            .serverPath(serverPath)
            .name(FilenameUtils.getName(path))
            .fileType(smbFile.isDirectory() ? FileType.FOLDER : FileType.FILE)
            .symbolicLink(false);

        try {
            builder.size(smbFile.length());
        } catch (Exception ignored) {
        }

        try {
            var lastModified = smbFile.lastModified();
            if (lastModified > 0) {
                builder.updatedDate(Instant.ofEpochMilli(lastModified));
            }
        } catch (Exception ignored) {
        }

        return builder.build();
    }
}
