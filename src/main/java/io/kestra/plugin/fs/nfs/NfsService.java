package io.kestra.plugin.fs.nfs;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


public final class NfsService {
    private NfsService() {
        
    }

    /**
     * Converts a string path to a java.nio.file.Path.
     * @param path The path as a string.
     * @return The Path object.
     * @throws IOException if the path is invalid.
     */
    public static Path toNfsPath(String path) throws IOException {
        if (path == null || path.isEmpty()) {
            throw new IOException("Path cannot be null or empty");
        }
        return Paths.get(path);
    }

    /**
     * Checks if the given path is on an NFS filesystem.
     * @param path The path to check.
     * @return The type of the file store (e.g., "nfs", "nfs4").
     * @throws IOException if the file store cannot be accessed.
     */
    public static String getFileStoreType(Path path) throws IOException {
        FileStore store = Files.getFileStore(path);
        return store.type();
    }
}
