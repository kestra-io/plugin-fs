package org.kestra.task.fs.sftp;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.sftp.IdentityInfo;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.kestra.core.exceptions.IllegalVariableEvaluationException;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.runners.RunContext;
import org.kestra.task.fs.AbstractVfsTask;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractSftpTask extends AbstractVfsTask {
    @InputProperty(
        description = "Username on the remote server",
        dynamic = true
    )
    protected String username;

    @InputProperty(
        description = "Password on the remote server",
        dynamic = true
    )
    protected String password;

    @InputProperty(
        description = "Private keyfile to login on the source server with ssh",
        body = "Must be the ssh key content, not a path",
        dynamic = true
    )
    protected String keyfile;

    @InputProperty(
        description = "Passphrase of the ssh key",
        dynamic = true
    )
    protected String passphrase;

    protected String basicAuth(RunContext runContext) throws IllegalVariableEvaluationException {
        return password != null ? runContext.render(username) + ":" + runContext.render(password) + "@" : runContext.render(username) + "@";
    }

    protected String sftpUri(RunContext runContext, String filepath) throws IllegalVariableEvaluationException {
        return "sftp://" +
            basicAuth(runContext) +
            runContext.render(host) +
            ":" + runContext.render(port) +
            "/" + runContext.render(filepath);
    }

    protected FileSystemOptions fsOptions(RunContext runContext) throws IOException, IllegalVariableEvaluationException {
        SftpFileSystemConfigBuilder instance = SftpFileSystemConfigBuilder.getInstance();

        FileSystemOptions options = new FileSystemOptions();

        instance.setStrictHostKeyChecking(options, "no");
        instance.setUserDirIsRoot(options, true);
        instance.setSessionTimeoutMillis(options, 10000);

        String keyContent = runContext.render(this.keyfile);
        if (keyContent != null) {
            File sftpKey = File.createTempFile("sftp-key-", "");
            try (FileWriter myWriter = new FileWriter(sftpKey)) {
                myWriter.write(keyContent);
            }

            IdentityInfo identityInfo;
            if (this.getPassphrase() != null) {
                identityInfo = new IdentityInfo(sftpKey, runContext.render(this.passphrase).getBytes());
            } else {
                identityInfo = new IdentityInfo(sftpKey);
            }

            instance.setIdentityProvider(options, identityInfo);
        }

        return options;
    }
}
