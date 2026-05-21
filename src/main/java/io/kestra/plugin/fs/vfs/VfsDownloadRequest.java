package io.kestra.plugin.fs.vfs;

import io.kestra.core.runners.RunContext;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;

import java.net.URI;

public record VfsDownloadRequest(
    RunContext runContext,
    StandardFileSystemManager fsm,
    FileSystemOptions fileSystemOptions,
    URI from,
    boolean validateChecksum,
    String checksumExpected,
    ChecksumService.Algorithm checksumAlgorithm
) {
    public VfsDownloadRequest {
        if (checksumAlgorithm == null) {
            checksumAlgorithm = ChecksumService.Algorithm.SHA_256;
        }
    }

    public static VfsDownloadRequest of(
        RunContext runContext,
        StandardFileSystemManager fsm,
        FileSystemOptions fileSystemOptions,
        URI from
    ) {
        return new VfsDownloadRequest(
            runContext,
            fsm,
            fileSystemOptions,
            from,
            false,
            null,
            ChecksumService.Algorithm.SHA_256
        );
    }
}
