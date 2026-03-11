package io.kestra.plugin.fs.vfs;

import io.kestra.core.runners.RunContext;
import org.apache.commons.vfs2.impl.DefaultFileReplicator;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;

import java.io.File;
import java.nio.file.Path;

class KestraStandardFileSystemManager extends StandardFileSystemManager {
    static final String CONFIG_RESOURCE = "providers.xml"; // same as StandardFileSystemManager.CONFIG_RESOURCE

    private final RunContext runContext;

    KestraStandardFileSystemManager(RunContext runContext) {
        super();

        this.runContext = runContext;
    }

    @Override
    protected DefaultFileReplicator createDefaultFileReplicator() {
        // By default, the file replicator uses /tmp as the base temp directory; we create it manually to use the task working directory.
        File vfsCache = this.runContext.workingDir().resolve(Path.of("vfs_cache")).toFile();
        if (!vfsCache.mkdirs()) {
            throw new RuntimeException("Unable to create directory " + vfsCache.getPath());
        }

        return new DefaultFileReplicator(vfsCache);
    }
}
