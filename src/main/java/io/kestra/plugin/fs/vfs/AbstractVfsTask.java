package io.kestra.plugin.fs.vfs;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.vfs2.FileSystemOptions;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

@SuperBuilder(toBuilder = true)
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractVfsTask extends Task implements AbstractVfsInterface {
    protected Property<String> host;
    protected Property<String> username;
    protected Property<String> password;

    @Builder.Default
    @Schema(
        title = "Enable the RSA/SHA1 algorithm (disabled by default)"
    )
    @NotNull
    private Property<Boolean> enableSshRsa1 = Property.ofValue(false);

    protected abstract FileSystemOptions fsOptions(RunContext runContext) throws IllegalVariableEvaluationException, IOException;

    protected abstract String scheme();

    protected URI uri(RunContext runContext, String filepath) throws IllegalVariableEvaluationException, URISyntaxException, JSchException {

        var renderedHost = runContext.render(this.host).as(String.class).orElseThrow();
        var renderedPort = runContext.render(this.getPort()).as(String.class).orElseThrow();
        var jsch = new JSch();
        var session = jsch.getSession(
            runContext.render(username).as(String.class).orElse(null),
            renderedHost, Integer.parseInt(renderedPort)
        );

        // enable disabled by default weak RSA/SHA1 algorithm
        if (runContext.render(enableSshRsa1).as(Boolean.class).orElseThrow()) {
            runContext.logger().info("RSA/SHA1 is enabled, be advised that SHA1 is no longer considered secure by the general cryptographic community.");
            session.setConfig("server_host_key", session.getConfig("server_host_key") + ",ssh-rsa");
            session.setConfig("PubkeyAcceptedAlgorithms", session.getConfig("PubkeyAcceptedAlgorithms") + ",ssh-rsa");
        }

        return VfsService.uri(runContext,
            this.scheme(),
            runContext.render(this.host).as(String.class).orElse(null),
            runContext.render(this.getPort()).as(String.class).orElse(null),
            runContext.render(this.username).as(String.class).orElse(null),
            runContext.render(this.password).as(String.class).orElse(null),
            filepath
        );
    }
}
