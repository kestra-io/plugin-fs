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
import io.kestra.core.tasks.scripts.AbstractBash;
import io.kestra.core.tasks.scripts.AbstractLogThread;
import io.kestra.plugin.fs.vfs.AbstractVfsInterface;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.BufferedOutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.List;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

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
    private static final long SLEEP_DELAY_MS = 25L;

    private String host;
    private String username;
    private String password;
    @Builder.Default
    private String port = "22";

    @Schema(title = "The list of commands to run on the remote server")
    @PluginProperty(dynamic = true)
    @NotNull
    @NotEmpty
    private String[] commands;

    @Schema(title = "Whether to check if the host public key could be found among known host, one of 'yes', 'no', 'ask'")
    @PluginProperty
    @Builder.Default
    private String strictHostKeyChecking = "no";

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        Session session = null;
        ChannelExec channel = null;

        try(
            var outStream = new PipedOutputStream();
            var inStream = new PipedInputStream(outStream);
            var errStream = new PipedOutputStream();
            var inErrStream = new PipedInputStream(errStream)
        ) {
            var renderedHost = runContext.render(host);
            var renderedPort = runContext.render(port);
            List<String> renderedCommands = new ArrayList<>(commands.length);
            for(String command: commands) {
                renderedCommands.add(runContext.render(command));
            }

            session = new JSch().getSession(runContext.render(username), renderedHost, Integer.parseInt(renderedPort));
            session.setPassword(runContext.render(password));
            session.setConfig("StrictHostKeyChecking", strictHostKeyChecking);
            session.connect();

            channel = (ChannelExec) session.openChannel("exec");
            channel.setCommand(String.join("\n", renderedCommands));
            channel.setOutputStream(new BufferedOutputStream(outStream), true);
            channel.setErrStream(new BufferedOutputStream(errStream), true);
            var stdOut = threadLogSupplier(runContext).call(inStream, false);
            var stdErr = threadLogSupplier(runContext).call(inErrStream, true);

            channel.connect();
            while (channel.isConnected()) {
                Thread.sleep(SLEEP_DELAY_MS);
            }

            stdOut.join();
            stdErr.join();
            
            if(channel.getExitStatus() != 0) {
                throw new Exception("SSH command fails with exit status " + channel.getExitStatus());
            }

            return null;
        } finally {
            if (channel != null) {
                channel.disconnect();
            }
            if (session != null) {
                session.disconnect();
            }
        }
    }

    private AbstractBash.LogSupplier threadLogSupplier(RunContext runContext) {
        return (inputStream, isStdErr) -> {
            AbstractLogThread thread = new AbstractBash.LogThread(runContext.logger(), inputStream, isStdErr, runContext);
            thread.setName("ssh-log-" + (isStdErr ? "-err" : "-out"));
            thread.start();

            return thread;
        };
    }
}
