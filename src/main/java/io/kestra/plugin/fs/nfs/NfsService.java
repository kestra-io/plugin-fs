package io.kestra.plugin.fs.nfs;

import jakarta.inject.Singleton;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Singleton
public class NfsService {

    private static NfsService instance;

    private NfsService() {
    }

    public static NfsService getInstance() {
        if (instance == null) {
            instance = new NfsService();
        }
        return instance;
    }

    public Path toNfsPath(String path) throws IOException {
        if (path == null || path.isEmpty()) {
            throw new IOException("Path cannot be null or empty");
        }
        return Paths.get(path);
    }

    public String getFileStoreType(Path path) throws IOException {
        FileStore store = Files.getFileStore(path);
        return store.type();
    }
}
