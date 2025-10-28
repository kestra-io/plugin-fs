package io.kestra.plugin.fs.nfs;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


public final class NfsService {
    private NfsService() {

    }

    
    public static Path toNfsPath(String path) throws IOException {
        if (path == null || path.isEmpty()) {
            throw new IOException("Path cannot be null or empty");
        }
        
        Path resolvedPath = Paths.get(path);

        return resolvedPath;
    }

    
    public static String getFileStoreType(Path path) throws IOException {
        
        Path checkPath = path;
        while (!Files.exists(checkPath) && checkPath.getParent() != null) {
             checkPath = checkPath.getParent();
        }
        if (!Files.exists(checkPath)) {
             throw new IOException("Cannot determine FileStore type, path and parents do not exist: " + path);
        }
        FileStore store = Files.getFileStore(checkPath);
        return store.type();
    }
}

