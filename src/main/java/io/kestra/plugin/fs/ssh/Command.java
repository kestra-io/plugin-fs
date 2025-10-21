package io.kestra.plugin.fs.ssh;

import com.jcraft.jsch.*;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.runners.PluginUtilsService;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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
            title = "Run SSH command using the local OpenSSH configuration",
            full = true,
            code = """
                id: ssh
                namespace: company.team
                tasks:
                  - id: ssh
                    type: io.kestra.plugin.fs.ssh.Command
                    authMethod: OPEN_SSH
                    useOpenSSHConfig: true
                    host: localhost
                    password: "{{ secret('SSH_PASSWORD') }}"
                    commands:
                      - echo "Hello World\""""
        )
    }
)
public class Command extends Task implements SshInterface, RunnableTask<ScriptOutput> {
    private static final long SLEEP_DELAY_MS = 25L;

    private Property<String> host;

    // Password Auth method
    private Property<String> username;
    private Property<String> password;

    // PubKey Auth method
    private Property<String> privateKey;
    private Property<String> privateKeyPassphrase;

    // OpenSSH config
    @Builder.Default
    @Schema(
        title = "OpenSSH configuration directory (deprecated).",
        description = "Deprecated. Use openSSHConfigPath instead."
    )
    @Deprecated
    private Property<String> openSSHConfigDir = Property.ofValue("~/.ssh/config");

    @Schema(
        title = "OpenSSH configuration file path used when the authentication method is `OPEN_SSH`."
    )
    private Property<String> openSSHConfigPath;

    @Schema(
        title = "SSH authentication configuration",
        description = """
            When the authentication method is set to `OPEN_SSH`, access to the local OpenSSH host configuration must be explicitly allowed.
            This ensures that the plugin can use SSH settings (such as `Host`, `User`, `Port`, or `IdentityFile`) defined in the user's OpenSSH configuration file.

            To enable this, configure the plugin using "allow-open-ssh-config" in the plugin configuration, as shown below:

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
    private Property<AuthMethod> authMethod = Property.ofValue(AuthMethod.PASSWORD);

    @Builder.Default
    private Property<String> port = Property.ofValue("22");

    @Schema(title = "The list of commands to run on the remote server")
    @PluginProperty(dynamic = true)
    @NotNull
    @NotEmpty
    private String[] commands;

    @Schema(title = "Whether to check if the host public key could be found among known host, one of 'yes', 'no', 'ask'")
    @Builder.Default
    private Property<String> strictHostKeyChecking = Property.ofValue("no");

    @Schema(
        title = "Environment variables to pass to the SSH process."
    )
    private Property<Map<String, String>> env;

    @Schema(
        title = "Not used anymore, will be removed soon"
    )
    @Deprecated
    private Property<Boolean> warningOnStdErr;

    @Builder.Default
    @Schema(
        title = "Enable the disabled by default RSA/SHA1 algorithm"
    )
    @NotNull
    private Property<Boolean> enableSshRsa1 = Property.ofValue(false);

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
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

            channel = (ChannelExec) session.openChannel("exec");
            channel.setCommand(String.join("\n", renderedCommands));
            channel.setOutputStream(outStream);
            channel.setErrStream(errStream);
            LogRunnable stdOutRunnable = new LogRunnable(inStream, false, runContext);
            LogRunnable stdErrRunnable = new LogRunnable(inErrStream, true, runContext);
            stdOut = Thread.ofVirtual().name("ssh-log-out").start(stdOutRunnable);
            stdErr = Thread.ofVirtual().name("ssh-log-err").start(stdErrRunnable);

            final Map<String, String> renderedEnv = runContext.render(this.env).asMap(String.class, String.class);
            for (var entry : renderedEnv.entrySet()) {
                channel.setEnv(runContext.render(entry.getKey()), runContext.render(entry.getValue()));
            }

            channel.connect();
            while (channel.isConnected()) {
                Thread.sleep(SLEEP_DELAY_MS);
            }

            outStream.flush();
            errStream.flush();
            stdOut.join();
            stdErr.join();

            if (channel.getExitStatus() != 0) {
                throw new Exception("SSH command fails with exit status " + channel.getExitStatus());
            }

            Map<String, Object> vars = new HashMap<>();
            vars.putAll(stdOutRunnable.outputs);
            vars.putAll(stdErrRunnable.outputs);

            return ScriptOutput
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
                stdOut.join();
            }
            if (stdErr != null) {
                stdErr.join();
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
}
