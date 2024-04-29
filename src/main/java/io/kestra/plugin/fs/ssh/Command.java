package io.kestra.plugin.fs.ssh;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.tasks.PluginUtilsService;
import io.kestra.core.utils.MapUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

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
            title = "Run SSH command using password authentication",
            code = {
                "host: localhost",
                "port: \"22\"",
                "authMethod: PASSWORD",
                "username: foo",
                "password: pass",
                "commands: ['ls']",
            }
        ),
        @Example(
            title = "Run SSH command using public key authentication (must be an OpenSSH private key)",
            code = {
                "host: localhost",
                "port: \"22\"",
                "authMethod: PUBLIC_KEY",
                "username: root",
                "privateKey: "{{ secret('SSH_RSA_PRIVATE_KEY') }}"",
                "commands: ['touch kestra_was_here']"
            }
        )
    }
)
public class Command extends Task implements SshInterface, RunnableTask<Command.ScriptOutput> {
    private static final long SLEEP_DELAY_MS = 25L;

    private String host;

    // Password Auth method
    private String username;
    private String password;

    // PubKey Auth method
    private String privateKey;
    private String privateKeyPassphrase;

    @Builder.Default
    private AuthMethod authMethod = AuthMethod.PASSWORD;

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

    @Schema(
        title = "Environment variables to pass to the SSH process."
    )
    @PluginProperty(
        additionalProperties = String.class,
        dynamic = true
    )
    protected Map<String, String> env;

    @Builder.Default
    @Schema(
        title = "Use `WARNING` state if any stdErr is sent"
    )
    @PluginProperty
    @NotNull
    protected Boolean warningOnStdErr = true;

    @Override
    public Command.ScriptOutput run(RunContext runContext) throws Exception {
        JSch jsch;
        Session session = null;
        ChannelExec channel = null;
        LogThread stdOut = null;
        LogThread stdErr = null;

        if (authMethod == AuthMethod.PASSWORD) {
            if (password == null) {
                throw new IllegalArgumentException("Password is necessary for given SSH auth method: " + AuthMethod.PASSWORD);
            }
        }
        else if (authMethod == AuthMethod.PUBLIC_KEY) {
            if (privateKey == null) {
                throw new IllegalArgumentException("Private key is necessary for given SSH auth method: " + AuthMethod.PASSWORD);
            }
        }

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

            jsch = new JSch();
            session = jsch.getSession(runContext.render(username), renderedHost, Integer.parseInt(renderedPort));

            if (authMethod == AuthMethod.PASSWORD) {
                session.setConfig("PreferredAuthentications", "password");
                session.setPassword(runContext.render(password));
            } else if (authMethod == AuthMethod.PUBLIC_KEY) {
                session.setConfig("PreferredAuthentications", "publickey");
                var privateKeyBytes = runContext.render(privateKey).getBytes();
                if (privateKeyPassphrase != null) {
                    jsch.addIdentity("primary", privateKeyBytes, null, runContext.render(privateKeyPassphrase).getBytes());
                } else {
                    jsch.addIdentity("primary", privateKeyBytes, null, null);
                }
            }
            session.setConfig("StrictHostKeyChecking", strictHostKeyChecking);
            session.connect();

            channel = (ChannelExec) session.openChannel("exec");
            channel.setCommand(String.join("\n", renderedCommands));
            channel.setOutputStream(outStream);
            channel.setErrStream(errStream);
            stdOut = threadLogSupplier(runContext).apply(inStream, false);
            stdErr = threadLogSupplier(runContext).apply(inErrStream, true);

            if (this.env != null) {
                for(var entry : env.entrySet()) {
                    channel.setEnv(runContext.render(entry.getKey()), runContext.render(entry.getValue()));
                }
            }

            channel.connect();
            while (channel.isConnected()) {
                Thread.sleep(SLEEP_DELAY_MS);
            }

            outStream.flush();
            errStream.flush();
            stdOut.join();
            stdErr.join();

            if(channel.getExitStatus() != 0) {
                throw new Exception("SSH command fails with exit status " + channel.getExitStatus());
            }

            Map<String, Object> vars = new HashMap<>();
            vars.putAll(stdOut.outputs);
            vars.putAll(stdErr.outputs);

            return ScriptOutput
                .builder()
                .exitCode(channel.getExitStatus())
                .stdOutLineCount(stdOut.count.get())
                .stdErrLineCount(stdErr.count.get())
                .warningOnStdErr(this.warningOnStdErr)
                .vars(vars)
                .build();
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

        private volatile AtomicInteger count = new AtomicInteger(0);

        private volatile Map<String, Object> outputs = new ConcurrentHashMap<>();

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
                        count.incrementAndGet();
                        outputs.putAll(PluginUtilsService.parseOut(line, runContext.logger(), runContext));

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

    @Builder
    @Getter
    public static class ScriptOutput implements Output {
        @Schema(
            title = "The value extracted from the output of the executed `commands`"
        )
        private final Map<String, Object> vars;

        @Schema(
            title = "The exit code of the entire Flow Execution"
        )
        @NotNull
        private final int exitCode;

        @JsonIgnore
        private final int stdOutLineCount;

        @JsonIgnore
        private final int stdErrLineCount;

        @JsonIgnore
        private Boolean warningOnStdErr;

        @Override
        public Optional<State.Type> finalState() {
            return this.warningOnStdErr != null && this.warningOnStdErr && this.stdErrLineCount > 0 ? Optional.of(State.Type.WARNING) : Output.super.finalState();
        }
    }
}
