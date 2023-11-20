package io.kestra.plugin.fs.smb;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;

import java.io.IOException;

public abstract class SmbService {
    public static FileSystemOptions fsOptions(RunContext runContext, SmbInterface smbInterface) throws IOException, IllegalVariableEvaluationException {
        FileSystemOptions opts = new FileSystemOptions();
        DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(
            opts,
            new StaticUserAuthenticator("", smbInterface.getUsername(), smbInterface.getPassword())
        );

        return opts;
    }
}
