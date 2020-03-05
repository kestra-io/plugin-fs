package org.kestra.task.fs;

import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.*;
import org.kestra.core.exceptions.IllegalVariableEvaluationException;
import org.kestra.core.models.annotations.InputProperty;
import org.kestra.core.models.annotations.OutputProperty;
import org.kestra.core.models.tasks.RunnableTask;
import org.kestra.core.models.tasks.Task;
import org.kestra.core.runners.RunContext;
import org.kestra.task.fs.auths.Auth;
import org.slf4j.Logger;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.IOException;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class VfsTask extends Task implements RunnableTask<VfsTask.Output>, OptionsInterface{
    @InputProperty(
        description = "file path",
        body = "Remote file path location on remote server that will be uploaded / downloaded to local file system",
        dynamic = true // If the variables will be rendered with template {{ }}
    )
    protected String remotePath;
    @InputProperty(
        description = "Local file path",
        body = "File path location on local file system where file will be read / write",
        dynamic = true // If the variables will be rendered with template {{ }}
    )
    protected String localPath;

    @NotNull
    @Valid
    @InputProperty(
        description = "Authentification"
    )
    protected Auth auth;

    @InputProperty(
        description = "keyfile",
        body = "Private keyfile to login on the source server with ssh",
        dynamic = true // If the variables will be rendered with template {{ }}
    )
    protected String keyfile;

    @InputProperty(
        description = "passPhrase",
        body = "PassPhrase to login on the source server with ssh",
        dynamic = true // If the variables will be rendered with template {{ }}
    )
    protected String passPhrase;

    @InputProperty(
        description = "username",
        body = "Remote username to login on the source server",
        dynamic = true // If the variables will be rendered with template {{ }}
    )
    protected String username;
    @InputProperty(
        description = "password",
        body = "Remote password to login on the source server",
        dynamic = true // If the variables will be rendered with template {{ }}
    )
    protected String password;
    @InputProperty(
        description = "host",
        body = "Remote host where to connect for file download",
        dynamic = true // If the variables will be rendered with template {{ }}
    )
    protected String host;
    @InputProperty(
        description = "port",
        body = "remote port in combination with host where to connect",
        dynamic = true // If the variables will be rendered with template {{ }}
    )
    protected String port;
    protected FileSystemOptions options;

    @Override
    public VfsTask.Output run(RunContext runContext) throws IOException, IllegalVariableEvaluationException {
        FileSystemManager fsm = VFS.getManager();
        remotePath = runContext.render(remotePath);
        localPath = runContext.render(localPath);
        auth = new Auth(runContext.render(username), runContext.render(password), runContext.render(keyfile), runContext.render(passPhrase));

        host = runContext.render(host);
        port = runContext.render(port);
        options = new FileSystemOptions();
        String localUri = "file://" + localPath;
        String remoteUri = remoteUri();
        addFsOptions();
        FileObject local = fsm.resolveFile(localUri);
        FileObject remote = fsm.resolveFile(remoteUri, options);
        beforeFileSync();
        doCopy(remote, local);
        afterFileSync();
        local.close();
        remote.close();

        Logger logger = runContext.logger(getClass());
        String render = runContext.render("file " + remotePath + " uploaded to sftp @ " + localPath);
        logger.debug(render);
        return buildOutput(remotePath, localPath);

    }

    protected abstract void afterFileSync();

    protected abstract void beforeFileSync();

    protected abstract String remoteUri();

    protected abstract VfsTask.Output buildOutput(String remotePath, String localPath);

    protected abstract void doCopy(FileObject remote, FileObject local) throws FileSystemException;

    public abstract void addFsOptions() throws IOException;


    /**
     * Input or Output can be nested as you need
     */
    @Builder
    @Getter
    public static class Output implements org.kestra.core.models.tasks.Output {
        @OutputProperty(
            description = "Short description for this output",
            body = "Full description of this output"
        )
        private OutputChild child;
    }

    @Builder
    @Getter
    public static class OutputChild implements org.kestra.core.models.tasks.Output {
        @OutputProperty(
            description = "Short description for this output",
            body = "Full description of this output"
        )
        private String value;
    }
}
