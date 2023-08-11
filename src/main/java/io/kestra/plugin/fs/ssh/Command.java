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

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
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
        LogThread stdOut = null;
        LogThread stdErr = null;

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
            stdOut = threadLogSupplier(runContext).apply(inStream, false);
            stdErr = threadLogSupplier(runContext).apply(inErrStream, true);

            channel.connect();
            while (channel.isConnected()) {
                Thread.sleep(SLEEP_DELAY_MS);
            }

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
            if (stdOut != null) {
                stdOut.join();
            }
            if (stdErr != null) {
                stdErr.join();
            }
        }
    }

    private BiFunction<InputStream, Boolean, LogThread> threadLogSupplier(RunContext runContext) {
        return (inputStream, isStdErr) -> {
            LogThread thread = new LogThread(inputStream, isStdErr, runContext);
            thread.setName("ssh-log-" + (isStdErr ? "-err" : "-out"));
            thread.start();

            return thread;
        };
    }

    private static class LogThread extends Thread {
        private final InputStream inputStream;

        private final boolean isStdErr;

        private final RunContext runContext;

        protected LogThread(InputStream inputStream, boolean isStdErr, RunContext runContext) {
            this.inputStream = inputStream;
            this.isStdErr = isStdErr;
            this.runContext = runContext;
        }

        @Override
        public void run() {
            try {
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                try (BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
                    String line;
                    while ((line = bufferedReader.readLine()) != null) {
                        if (isStdErr) {
                            runContext.logger().warn(line);
                        } else {
                            runContext.logger().info(line);
                        }
                    }
                }
            } catch (Exception e) {
                // silently fail if we cannot log a line
            }
        }
    }
}
