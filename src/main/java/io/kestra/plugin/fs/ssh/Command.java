package io.kestra.plugin.fs.ssh;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.fs.vfs.AbstractVfsInterface;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.ByteArrayOutputStream;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Send a command to a remote server using SSH."
)
@Plugin(
    examples = {
        @Example(
            code = {
                "host: localhost",
                "port: 22",
                "username: foo",
                "password: pass",
                "command: ls",
            }
        )
    }
)
public class Command extends Task implements AbstractVfsInterface, RunnableTask<VoidOutput> {
    private static final long SLEEP_DELAY = 25L;

    private String host;
    private String username;
    private String password;
    @Builder.Default
    private String port = "22";

    @Schema(title = "The command to run on the remote server")
    @PluginProperty(dynamic = true)
    private String command;

    @Schema(title = "Whether to check if the host public key could be found among known host, one of 'yes', 'no', 'ask'")
    @PluginProperty
    @Builder.Default
    private String strictHostKeyChecking = "no";

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        Session session = null;
        ChannelExec channel = null;

        try(var outStream = new ByteArrayOutputStream(); var errStream = new ByteArrayOutputStream()) {
            var renderedHost = runContext.render(host);
            var renderedPort = runContext.render(port);

            session = new JSch().getSession(runContext.render(username), renderedHost, Integer.parseInt(renderedPort));
            session.setPassword(runContext.render(password));
            session.setConfig("StrictHostKeyChecking", strictHostKeyChecking);
            session.connect();

            channel = (ChannelExec) session.openChannel("exec");
            channel.setCommand(runContext.render(command));
            channel.setOutputStream(outStream);
            channel.setErrStream(errStream);
            channel.connect();

            while (channel.isConnected()) {
                Thread.sleep(SLEEP_DELAY);
            }

            outStream.toString().lines().forEach(line -> runContext.logger().info(line));
            if(channel.getExitStatus() != 0) {
                errStream.toString().lines().forEach(line -> runContext.logger().error(line));
                throw new RuntimeException("SSH command fails with exit status " + channel.getExitStatus());
            }

            return null;
        } finally {
            if (session != null) {
                session.disconnect();
            }
            if (channel != null) {
                channel.disconnect();
            }
        }
    }
}
