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
import java.io.File;
import java.io.IOException;
import java.net.URI;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class VfsTask extends Task implements RunnableTask<Output> {
    @InputProperty(
        description = "from file path",
        body = "From file system where file will be read / write",
        dynamic = true // If the variables will be rendered with template {{ }}
    )
    protected String to;
    @InputProperty(
        description = "to file path",
        body = "To file system where file will be read / write",
        dynamic = true // If the variables will be rendered with template {{ }}
    )
    protected String from;

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

}
