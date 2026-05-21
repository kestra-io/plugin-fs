package io.kestra.plugin.fs.smb;

import io.kestra.core.runners.RunContext;
import io.kestra.plugin.fs.vfs.ChecksumService;
import org.codelibs.jcifs.smb.CIFSContext;

public record SmbDownloadRequest(
    RunContext runContext,
    CIFSContext cifsContext,
    SmbInterface smbInterface,
    String filepath,
    boolean validateChecksum,
    String checksumExpected,
    ChecksumService.Algorithm checksumAlgorithm
) {
    public SmbDownloadRequest {
        if (checksumAlgorithm == null) {
            checksumAlgorithm = ChecksumService.Algorithm.SHA_256;
        }
    }

    public static SmbDownloadRequest of(
        RunContext runContext,
        CIFSContext cifsContext,
        SmbInterface smbInterface,
        String filepath
    ) {
        return new SmbDownloadRequest(
            runContext,
            cifsContext,
            smbInterface,
            filepath,
            false,
            null,
            ChecksumService.Algorithm.SHA_256
        );
    }
}
