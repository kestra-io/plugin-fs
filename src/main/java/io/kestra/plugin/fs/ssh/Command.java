package io.kestra.plugin.fs.ssh;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.jcraft.jsch.*;
import io.kestra.core.models.WorkerJobLifecycle;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.runners.PluginUtilsService;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Run commands over SSH",
    description = "Executes one or more commands on a remote host via SSH. Supports PASSWORD, PUBLIC_KEY, or OPEN_SSH auth. Default port 22 and strict host key checking off (`no`). Allow weak rsa-sha1 only when `enableSshRsa1` is true."
)
@Plugin(
    examples = {
        @Example(
            title = "Run SSH command using password authentication",
            full = true,
            code = """
                id: fs_ssh_command
                namespace: company.team

                tasks:
                  - id: command
                    type: io.kestra.plugin.fs.ssh.Command
                    host: localhost
                    port: "22"
                    authMethod: PASSWORD
                    username: foo
                    password: "{{ secret('SSH_PASSWORD') }}"
                    commands:
                      - ls
                """
        ),
        @Example(
            title = "Run SSH command using public key authentication (must be an OpenSSH private key)",
            full = true,
            code = """
                id: fs_ssh_command
                namespace: company.team

                tasks:
                  - id: command
                    type: io.kestra.plugin.fs.ssh.Command
                    host: localhost
                    port: "22"
                    authMethod: PUBLIC_KEY
                    username: root
                    privateKey: "{{ secret('SSH_RSA_PRIVATE_KEY') }}"
                    commands:
                      - touch kestra_was_here
                """
        ),
        @Example(
            title = "Run SSH command through a proxy command",
            full = true,
            code = """
                id: fs_ssh_proxy_command
                namespace: company.team

                tasks:
                  - id: command
                    type: io.kestra.plugin.fs.ssh.Command
                    host: host
                    username: user
                    authMethod: PASSWORD
                    password: "{{ secret('SSH_PASSWORD') }}"
                    proxyCommand: |
                      cloudflared access ssh --service-token-id {{ secret('SSH_PROXY_SERVICE_TOKEN_ID') }} --service-token-secret {{ secret('SSH_PROXY_SERVICE_TOKEN_SECRET') }} --hostname proxy_host
                    commands:
                      - mycmd
                """
        ),
        @Example(
            title = "Run SSH command using the local OpenSSH configuration",
            full = true,
            code = """
                id: ssh
                namespace: company.team
                tasks:
                  - id: ssh
                    type: io.kestra.plugin.fs.ssh.Command
                    authMethod: OPEN_SSH
                    host: localhost
                    password: "{{ secret('SSH_PASSWORD') }}"
                    commands:
                      - echo "Hello World\""""
        )
    }
)
public class Command extends Task implements SshInterface, RunnableTask<Command.Output>, WorkerJobLifecycle {
    private static final long SLEEP_DELAY_MS = 25L;
    private static final Duration READER_JOIN_TIMEOUT = Duration.ofSeconds(5);

    /**
     * Tracks whether {@link #kill()}/{@link #stop()} has terminated the SSH channel/session, so the
     * wait loop can unwind and the misleading exit-status check is not reached.
     */
    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final AtomicBoolean killed = new AtomicBoolean(false);

    /**
     * Records the channel currently executing the remote command, so that {@link #kill()} or
     * {@link #stop()} can abort the live SSH channel instead of a stale one from a previous retry attempt.
     */
    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final AtomicReference<ChannelExec> trackedChannel = new AtomicReference<>();

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final AtomicReference<Session> trackedSession = new AtomicReference<>();

    /**
     * Tracks the {@link RunContext} logger of the in-flight {@link #run(RunContext)} call, so that
     * {@link #terminate()} can log through it (per plugin guidelines, `runContext.logger()` is the
     * only supported logging channel) instead of a static class logger.
     */
    @JsonIgnore
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @Builder.Default
    private final AtomicReference<Logger> trackedLogger = new AtomicReference<>();

    @PluginProperty(group = "main")
    private Property<String> host;

    // Password Auth method
    @ToString.Exclude
    @PluginProperty(secret = true, group = "connection")
    private Property<String> username;
    @ToString.Exclude
    @PluginProperty(secret = true, group = "connection")
    private Property<String> password;

    // PubKey Auth method
    @ToString.Exclude
    @PluginProperty(secret = true, group = "connection")
    private Property<String> privateKey;
    @ToString.Exclude
    @PluginProperty(secret = true, group = "connection")
    private Property<String> privateKeyPassphrase;

    // OpenSSH config
    @Builder.Default
    @Schema(
        title = "OpenSSH config directory (deprecated)",
        description = "Deprecated; use `openSSHConfigPath` instead."
    )
    @Deprecated
    @PluginProperty(group = "deprecated")
    private Property<String> openSSHConfigDir = Property.ofValue("~/.ssh/config");

    @Schema(
        title = "OpenSSH config file path",
        description = "Used when `authMethod` is OPEN_SSH. Access must be allowed via plugin configuration."
    )
    @PluginProperty(group = "advanced")
    private Property<String> openSSHConfigPath;

    @Schema(
        title = "Proxy command",
        description = """
            Optional local command used to establish the SSH transport (OpenSSH `ProxyCommand` semantics).
            Example: `cloudflared access ssh --service-token-id ... --service-token-secret ... --hostname ...`
            """
    )
    private Property<String> proxyCommand;

    @Schema(
        title = "SSH authentication configuration",
        description = """
            When `authMethod` is OPEN_SSH, access to local SSH config must be allowed with `allow-open-ssh-config: true` in plugin defaults:
            ```yaml
            kestra:
              plugins:
                configurations:
                  - type: io.kestra.plugin.fs.ssh.Command
                    values:
                      allow-open-ssh-config: true
            ```
            """
    )
    @Builder.Default
    @PluginProperty(group = "connection")
    private Property<AuthMethod> authMethod = Property.ofValue(AuthMethod.PASSWORD);

    @Builder.Default
    @PluginProperty(group = "connection")
    private Property<String> port = Property.ofValue("22");

    @Schema(title = "Commands to execute")
    @PluginProperty(dynamic = true, group = "main")
    @NotNull
    @NotEmpty
    private String[] commands;

    @Schema(title = "Strict host key checking", description = "One of yes|no|ask. Default no.")
    @Builder.Default
    @PluginProperty(group = "connection")
    private Property<String> strictHostKeyChecking = Property.ofValue("no");

    @Schema(
        title = "Environment variables to pass to the SSH process"
    )
    @PluginProperty(group = "execution")
    private Property<Map<String, String>> env;

    @Schema(
        title = "Not used anymore, will be removed soon"
    )
    @Deprecated
    @PluginProperty(group = "deprecated")
    private Property<Boolean> warningOnStdErr;

    @Builder.Default
    @Schema(
        title = "Enable the disabled by default RSA/SHA1 algorithm"
    )
    @NotNull
    @PluginProperty(group = "advanced")
    private Property<Boolean> enableSshRsa1 = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        // Reset from a possible previous run() of this same task instance (e.g. a retry) before any
        // connection is opened. This must NOT be done in `finally`, otherwise a kill() racing with
        // cleanup of the current run() would be silently undone and the killed run() would fall through
        // to the misleading exit-status check instead of the clear "killed" message.
        killed.set(false);
        trackedLogger.set(runContext.logger());

        JSch jsch;
        Session session = null;
        ChannelExec channel = null;
        Thread stdOut = null;
        Thread stdErr = null;

        final AuthMethod renderedAuthMethod = runContext.render(this.authMethod).as(AuthMethod.class).orElseThrow();
        if (AuthMethod.PASSWORD.equals(renderedAuthMethod) && runContext.render(this.password).as(String.class).isEmpty()) {
            throw new IllegalArgumentException("Password is necessary for given SSH auth method: " + AuthMethod.PASSWORD);
        }

        if (AuthMethod.PUBLIC_KEY.equals(renderedAuthMethod) && runContext.render(this.privateKey).as(String.class).isEmpty()) {
            throw new IllegalArgumentException("Private key is necessary for given SSH auth method: " + AuthMethod.PUBLIC_KEY);
        }

        if (AuthMethod.OPEN_SSH.equals(renderedAuthMethod) &&
            (runContext.pluginConfiguration(ALLOW_OPEN_SSH_CONFIG).isEmpty() ||
                !runContext.<Boolean>pluginConfiguration(ALLOW_OPEN_SSH_CONFIG).orElse(false))) {
            throw new IllegalArgumentException("You need to allow access to the host OpenSSH configuration via the plugin configuration `" + ALLOW_OPEN_SSH_CONFIG + "`");
        }

        try (
            var outStream = new PipedOutputStream();
            var inStream = new PipedInputStream(outStream);
            var errStream = new PipedOutputStream();
            var inErrStream = new PipedInputStream(errStream)
        ) {
            var renderedHost = runContext.render(host).as(String.class).orElseThrow();
            var renderedPort = runContext.render(port).as(String.class).orElse("22");
            List<String> renderedCommands = new ArrayList<>(commands.length);
            for (String command : commands) {
                renderedCommands.add(runContext.render(command));
            }

            jsch = new JSch();

            if (AuthMethod.OPEN_SSH.equals(renderedAuthMethod)) {
                var rOpenSSHConfigPath = runContext.render(openSSHConfigPath).as(String.class);
                String configPath;
                if (rOpenSSHConfigPath.isPresent()) {
                    configPath = rOpenSSHConfigPath.orElseThrow();
                } else {
                    configPath = runContext.render(openSSHConfigDir).as(String.class).orElseThrow();
                }
                ConfigRepository configRepository = OpenSSHConfig.parseFile(configPath);
                jsch.setConfigRepository(configRepository);
            }

            session = jsch.getSession(
                runContext.render(username).as(String.class).orElse(null),
                renderedHost, Integer.parseInt(renderedPort)
            );
            var rProxyCommand = runContext.render(proxyCommand).as(String.class);
            if (rProxyCommand.isPresent()) {
                session.setProxy(new ProcessProxyCommand(rProxyCommand.orElseThrow(), session.getUserName()));
            }

            // enable disabled by default weak RSA/SHA1 algorithm
            if (runContext.render(enableSshRsa1).as(Boolean.class).orElseThrow()) {
                runContext.logger().info("RSA/SHA1 is enabled, be advise that SHA1 is no longer considered secure by the general cryptographic community.");
                session.setConfig("server_host_key", session.getConfig("server_host_key") + ",ssh-rsa");
                session.setConfig("PubkeyAcceptedAlgorithms", session.getConfig("PubkeyAcceptedAlgorithms") + ",ssh-rsa");
            }

            var rPassword = runContext.render(this.password).as(String.class);
            var rPrivateKeyPassphrase = runContext.render(this.privateKeyPassphrase).as(String.class);

            switch (renderedAuthMethod) {
                case PASSWORD:
                    session.setConfig("PreferredAuthentications", "password");
                    session.setPassword(rPassword.orElseThrow());
                    break;
                case PUBLIC_KEY:
                    session.setConfig("PreferredAuthentications", "publickey");
                    var privateKeyBytes = runContext.render(this.privateKey).as(String.class).orElseThrow().getBytes();
                    jsch.addIdentity("primary", privateKeyBytes, null, rPrivateKeyPassphrase.map(String::getBytes).orElse(null));
                    break;
                case OPEN_SSH:
                    rPassword.ifPresent(session::setPassword);
                    if (rPrivateKeyPassphrase.isPresent()) {
                        session.setUserInfo(new BasicUserInfo(rPrivateKeyPassphrase.get()));
                    }
                    break;
            }

            session.setConfig("StrictHostKeyChecking", runContext.render(strictHostKeyChecking).as(String.class).orElse(null));
            session.connect();
            trackedSession.set(session);

            if (killed.get()) {
                // kill() raced in between session.connect() and here: terminate() has already (or is about to)
                // disconnect the session, so surface the clear message instead of letting a stale session be used.
                throw new Exception("SSH command was killed before completion");
            }

            LogRunnable stdOutRunnable;
            LogRunnable stdErrRunnable;
            try {
                channel = (ChannelExec) session.openChannel("exec");
                trackedChannel.set(channel);
                channel.setCommand(String.join("\n", renderedCommands));
                channel.setOutputStream(outStream);
                channel.setErrStream(errStream);
                stdOutRunnable = new LogRunnable(inStream, false, runContext);
                stdErrRunnable = new LogRunnable(inErrStream, true, runContext);
                stdOut = Thread.ofVirtual().name("ssh-log-out").start(stdOutRunnable);
                stdErr = Thread.ofVirtual().name("ssh-log-err").start(stdErrRunnable);

                final Map<String, String> renderedEnv = runContext.render(this.env).asMap(String.class, String.class);
                for (var entry : renderedEnv.entrySet()) {
                    channel.setEnv(runContext.render(entry.getKey()), runContext.render(entry.getValue()));
                }

                channel.connect();
            } catch (JSchException e) {
                // A kill() racing in between `trackedSession.set(session)`/`trackedChannel.set(channel)` above and
                // here disconnects the channel/session concurrently, which otherwise surfaces here as a raw,
                // confusing JSchException (e.g. "session is not connected") instead of the intended killed message.
                if (killed.get()) {
                    throw new Exception("SSH command was killed before completion", e);
                }
                throw e;
            }

            if (killed.get()) {
                throw new Exception("SSH command was killed before completion");
            }

            while (channel.isConnected() && !killed.get()) {
                Thread.sleep(SLEEP_DELAY_MS);
            }

            if (killed.get()) {
                throw new Exception("SSH command was killed before completion");
            }

            outStream.flush();
            errStream.flush();
            stdOut.join(READER_JOIN_TIMEOUT);
            stdErr.join(READER_JOIN_TIMEOUT);

            if (channel.getExitStatus() != 0) {
                throw new Exception("SSH command fails with exit status " + channel.getExitStatus());
            }

            Map<String, Object> vars = new HashMap<>();
            vars.putAll(stdOutRunnable.outputs);
            vars.putAll(stdErrRunnable.outputs);

            return Output
                .builder()
                .exitCode(channel.getExitStatus())
                .stdOutLineCount(stdOutRunnable.count.get())
                .stdErrLineCount(stdErrRunnable.count.get())
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
                stdOut.join(READER_JOIN_TIMEOUT);
            }
            if (stdErr != null) {
                stdErr.join(READER_JOIN_TIMEOUT);
            }
            // Clear tracked references so a retry attempt of this same task instance never cancels a stale channel/session.
            trackedChannel.set(null);
            trackedSession.set(null);
            trackedLogger.set(null);
        }
    }

    /**
     * Aborts the remote SSH command in flight. Delegates to {@link #terminate()}, which is idempotent
     * and safe to call from a killer thread while {@link #run(RunContext)} is still executing.
     */
    @Override
    public void kill() {
        terminate();
    }

    @Override
    public void stop() {
        terminate();
    }

    private void terminate() {
        if (!killed.compareAndSet(false, true)) {
            return;
        }

        Logger logger = trackedLogger.get();

        ChannelExec channel = trackedChannel.get();
        if (channel != null) {
            try {
                channel.disconnect();
            } catch (Exception e) {
                // WARN, not DEBUG: a failed disconnect here means the remote SSH process may keep running
                // (and billing) after a kill, which is the exact failure mode this feature exists to prevent.
                if (logger != null) {
                    logger.warn("Failed to disconnect SSH channel while killing task", e);
                }
            }
        }

        Session session = trackedSession.get();
        if (session != null) {
            try {
                // Session#disconnect() also closes any configured Proxy (e.g. proxyCommand), which destroys the local helper process.
                session.disconnect();
            } catch (Exception e) {
                if (logger != null) {
                    logger.warn("Failed to disconnect SSH session while killing task", e);
                }
            }
        }
    }

    // Can be extended for Password AuthMethod as well
    @Slf4j
    private record BasicUserInfo(String passphrase) implements UserInfo {

        @Override
        public String getPassphrase() {
            return passphrase;
        }

        @Override
        public String getPassword() {
            return null;
        }

        @Override
        public boolean promptPassword(String message) {
            return false;
        }

        @Override
        public boolean promptPassphrase(String message) {
            return true;
        }

        @Override
        public boolean promptYesNo(String message) {
            return false;
        }

        @Override
        public void showMessage(String message) {
            log.debug(message);
        }
    }

    private static final class ProcessProxyCommand implements Proxy {
        // %h/%p/%r are substituted verbatim into a shell command; only allow characters that cannot break out of it
        private static final Pattern SAFE_VALUE = Pattern.compile("[a-zA-Z0-9._@-]+");

        private final String command;
        @PluginProperty(secret = true, group = "connection")
        private final String username;

        private Process process;
        private InputStream inputStream;
        private OutputStream outputStream;

        private ProcessProxyCommand(String command, String username) {
            this.command = command;
            this.username = username;
        }

        @Override
        public void connect(SocketFactory socketFactory, String host, int port, int timeout) throws Exception {
            var sanitizedHost = sanitize(host, "host");
            var sanitizedUsername = username == null ? "" : sanitize(username, "username");

            var resolvedCommand = command
                .replace("%h", sanitizedHost)
                .replace("%p", String.valueOf(port))
                .replace("%r", sanitizedUsername);

            var processBuilder = new ProcessBuilder(shellCommand(resolvedCommand));
            processBuilder.redirectError(ProcessBuilder.Redirect.DISCARD);
            process = processBuilder.start();
            inputStream = process.getInputStream();
            outputStream = process.getOutputStream();

            if (!process.isAlive()) {
                throw new JSchException("Proxy command exited immediately: " + resolvedCommand);
            }
        }

        private static String sanitize(String value, String fieldName) {
            if (!SAFE_VALUE.matcher(value).matches()) {
                throw new IllegalArgumentException(
                    "Invalid SSH " + fieldName + " '" + value + "': only letters, digits, '.', '_', '@' and '-' are allowed when a `proxyCommand` is configured, to prevent shell injection."
                );
            }
            return value;
        }

        private static String[] shellCommand(String command) {
            if (System.getProperty("os.name").toLowerCase().contains("win")) {
                return new String[] {"cmd.exe", "/c", command};
            }

            return new String[] {"/bin/sh", "-c", command};
        }

        @Override
        public InputStream getInputStream() {
            return inputStream;
        }

        @Override
        public OutputStream getOutputStream() {
            return outputStream;
        }

        @Override
        public Socket getSocket() {
            return null;
        }

        @Override
        public void close() {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException ignored) {
                    // no-op
                }
            }

            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException ignored) {
                    // no-op
                }
            }

            if (process != null) {
                process.destroy();
            }
        }
    }

    private static class LogRunnable implements Runnable {
        private final InputStream inputStream;

        private final boolean isStdErr;

        private final RunContext runContext;

        private final AtomicInteger count = new AtomicInteger(0);

        private final Map<String, Object> outputs = new ConcurrentHashMap<>();

        protected LogRunnable(InputStream inputStream, boolean isStdErr, RunContext runContext) {
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
                        outputs.putAll(PluginUtilsService.parseOut(line, runContext.logger(), runContext, isStdErr, null));
                    }
                }
            } catch (Exception e) {
                // silently fail if we cannot log a line
            }
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The values extracted from executed `commands` using the [Kestra outputs](https://kestra.io/docs/scripts/outputs-metrics#outputs-and-metrics-in-script-and-commands-tasks) format"
        )
        @JsonInclude(JsonInclude.Include.ALWAYS) // always include vars so it's easier to reason about in expressions
        private final Map<String, Object> vars;

        @Schema(
            title = "The exit code of the entire flow execution"
        )
        @NotNull
        private final int exitCode;

        @JsonIgnore
        private final int stdOutLineCount;

        @JsonIgnore
        private final int stdErrLineCount;
    }
}
