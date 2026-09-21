package io.kestra.plugin.fs.ssh;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.ssh.SshInterface.AuthMethod;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

// WARNING, the 'setpasswd.sh' script must be runnable for the test to pass, if the test fail try launching:
// chmod go+x src/test/resources/ssh/setpasswd.sh
@KestraTest
class CommandTest {
    public static final Property<String> USERNAME = Property.ofValue("foo");
    public static final Property<String> PASSWORD = Property.ofValue("O7m)&H/0Em4/T8RqCa!Al=M@N6^;@+");

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run_passwordMethod() throws Exception {
        Command command = Command.builder()
            .id(IdUtils.create())
            .type(Command.class.getName())
            .host(Property.ofValue("localhost"))
            .username(USERNAME)
            .authMethod(Property.ofValue(AuthMethod.PASSWORD))
            .password(PASSWORD)
            .port(Property.ofValue("2222"))
            .commands(new String[] {
                "echo 0",
                "echo 1",
                ">&2 echo 2",
                "echo '::{\"outputs\":{\"out\":\"1\"}}::'",
                ">&2 echo '::{\"outputs\":{\"err\":\"2\"}}::'",
            })
            .build();

        Command.Output run = command.run(TestsUtils.mockRunContext(runContextFactory, command, Map.of()));

        Thread.sleep(500);

        assertThat(run.getExitCode(), is(0));
        assertThat(run.getStdOutLineCount(), is(3));
        assertThat(run.getStdErrLineCount(), is(2));
        assertThat(run.getVars().get("out"), is("1"));
        assertThat(run.getVars().get("err"), is("2"));
    }

    @Test
    void run_passwordMethod_withProxyCommand() throws Exception {
        var command = Command.builder()
            .id(IdUtils.create())
            .type(Command.class.getName())
            .host(Property.ofValue("unreachable.invalid"))
            .username(USERNAME)
            .authMethod(Property.ofValue(AuthMethod.PASSWORD))
            .password(PASSWORD)
            .port(Property.ofValue("2222"))
            .proxyCommand(Property.ofValue("nc {{ inputs.proxyHost }} {{ inputs.proxyPort }}"))
            .commands(new String[] {
                "echo 0",
                "echo 1",
                ">&2 echo 2",
                "echo '::{\"outputs\":{\"out\":\"1\"}}::'",
                ">&2 echo '::{\"outputs\":{\"err\":\"2\"}}::'",
            })
            .build();

        var exception = Assertions.assertThrows(Exception.class, () -> command.run(TestsUtils.mockRunContext(
            runContextFactory,
            command,
            Map.of(
                "proxyHost", "127.0.0.1",
                "proxyPort", "1"
            )
        )));

        assertThat(String.valueOf(exception.getMessage()).contains("UnknownHostException"), is(false));
    }

    @Test
    void toString_shouldNotLeakSecrets() {
        String password = "O7m)&H/0Em4/T8RqCa!Al=M@N6^;@+";
        String privateKey = "-----BEGIN OPENSSH PRIVATE KEY-----\nfakeKeyMaterialForTestingPurposesOnly\n-----END OPENSSH PRIVATE KEY-----";
        String privateKeyPassphrase = "s3cr3t-passphrase";

        Command command = Command.builder()
            .id(IdUtils.create())
            .type(Command.class.getName())
            .host(Property.ofValue("localhost"))
            .username(USERNAME)
            .authMethod(Property.ofValue(AuthMethod.PUBLIC_KEY))
            .password(Property.ofValue(password))
            .privateKey(Property.ofValue(privateKey))
            .privateKeyPassphrase(Property.ofValue(privateKeyPassphrase))
            .port(Property.ofValue("2222"))
            .commands(new String[] {"echo 0"})
            .build();

        String toString = command.toString();

        assertThat(toString, not(containsString(password)));
        assertThat(toString, not(containsString(privateKey)));
        assertThat(toString, not(containsString(privateKeyPassphrase)));
    }

    @Test
    void run_proxyCommand_rejectsShellMetacharactersInHost() {
        for (String maliciousHost : new String[] {"127.0.0.1; touch /tmp/pwned", "127.0.0.1 | id", "$(id)"}) {
            Command command = Command.builder()
                .id(IdUtils.create())
                .type(Command.class.getName())
                .host(Property.ofValue(maliciousHost))
                .username(USERNAME)
                .authMethod(Property.ofValue(AuthMethod.PASSWORD))
                .password(PASSWORD)
                .port(Property.ofValue("2222"))
                .proxyCommand(Property.ofValue("echo %h"))
                .commands(new String[] {"echo 0"})
                .build();

            IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> command.run(TestsUtils.mockRunContext(runContextFactory, command, Map.of())),
                "Expected rejection for host: " + maliciousHost
            );

            assertThat(exception.getMessage(), containsString("host"));
        }
    }

    @Test
    void run_proxyCommand_rejectsShellMetacharactersInUsername() {
        Command command = Command.builder()
            .id(IdUtils.create())
            .type(Command.class.getName())
            .host(Property.ofValue("127.0.0.1"))
            .username(Property.ofValue("root; touch /tmp/pwned"))
            .authMethod(Property.ofValue(AuthMethod.PASSWORD))
            .password(PASSWORD)
            .port(Property.ofValue("2222"))
            .proxyCommand(Property.ofValue("echo %r"))
            .commands(new String[] {"echo 0"})
            .build();

        IllegalArgumentException exception = Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> command.run(TestsUtils.mockRunContext(runContextFactory, command, Map.of()))
        );

        assertThat(exception.getMessage(), containsString("username"));
    }

    @Test
    void run_proxyCommand_allowsLegitimateHostAndUsername() {
        Command command = Command.builder()
            .id(IdUtils.create())
            .type(Command.class.getName())
            .host(Property.ofValue("my-host.example.com"))
            .username(Property.ofValue("svc_user@example.com"))
            .authMethod(Property.ofValue(AuthMethod.PASSWORD))
            .password(PASSWORD)
            .port(Property.ofValue("2222"))
            .proxyCommand(Property.ofValue("echo %h %r"))
            .commands(new String[] {"echo 0"})
            .build();

        // A legitimate host/username must pass sanitization and reach the (fake) proxy shell command;
        // the subsequent SSH handshake still fails since `echo` doesn't speak SSH, but never with IllegalArgumentException.
        Exception exception = Assertions.assertThrows(
            Exception.class,
            () -> command.run(TestsUtils.mockRunContext(runContextFactory, command, Map.of()))
        );

        assertThat(exception, is(not(instanceOf(IllegalArgumentException.class))));
    }

    @Test
    void run_pubkeyMethod() throws Exception {
        Path tempDir = Files.createTempDirectory("ssh-key");
        Path privateKeyPath = tempDir.resolve("id_ed25519");
        Path publicKeyPath = tempDir.resolve("id_ed25519.pub");

        Process keygen = new ProcessBuilder(
            "ssh-keygen",
            "-t", "ed25519",
            "-N", "",
            "-f", privateKeyPath.toString()
        ).redirectErrorStream(true).start();

        if (keygen.waitFor() != 0) {
            try (InputStream errorStream = keygen.getInputStream()) {
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                errorStream.transferTo(buffer);
                String output = buffer.toString(StandardCharsets.UTF_8);
                throw new IllegalStateException("ssh-keygen failed: " + output);
            }
        }

        String keyFileContent = Files.readString(privateKeyPath, StandardCharsets.UTF_8);
        String publicKeyContent = Files.readString(publicKeyPath, StandardCharsets.UTF_8).trim();

        Command setupAuthorizedKey = Command.builder()
            .id(IdUtils.create())
            .type(Command.class.getName())
            .host(Property.ofValue("localhost"))
            .username(USERNAME)
            .authMethod(Property.ofValue(AuthMethod.PASSWORD))
            .password(PASSWORD)
            .port(Property.ofValue("2222"))
            .commands(new String[] {
                "mkdir -p ~/.ssh",
                "chmod 700 ~/.ssh",
                "printf '%s\\n' '" + publicKeyContent + "' >> ~/.ssh/authorized_keys",
                "chmod 600 ~/.ssh/authorized_keys"
            })
            .build();

        setupAuthorizedKey.run(TestsUtils.mockRunContext(runContextFactory, setupAuthorizedKey, Map.of()));

        Command command = Command.builder()
            .id(IdUtils.create())
            .type(Command.class.getName())
            .host(Property.ofValue("localhost"))
            .username(USERNAME)
            .authMethod(Property.ofValue(AuthMethod.PUBLIC_KEY))
            .privateKey(Property.ofValue(keyFileContent))
            .port(Property.ofValue("2222"))
            .commands(new String[] {
                "echo 0",
                "echo 1",
                ">&2 echo 2",
                "echo '::{\"outputs\":{\"out\":\"1\"}}::'",
                ">&2 echo '::{\"outputs\":{\"err\":\"2\"}}::'",
            })
            .build();

        Command.Output run = command.run(TestsUtils.mockRunContext(runContextFactory, command, Map.of()));

        Thread.sleep(500);

        assertThat(run.getExitCode(), is(0));
        assertThat(run.getStdOutLineCount(), is(3));
        assertThat(run.getStdErrLineCount(), is(2));
        assertThat(run.getVars().get("out"), is("1"));
        assertThat(run.getVars().get("err"), is("2"));
    }

    @Test
    void openSSHConfigDir_shouldBeNullWhenNotSet() {
        Command passwordCommand = Command.builder()
            .id(IdUtils.create())
            .type(Command.class.getName())
            .host(Property.ofValue("localhost"))
            .username(USERNAME)
            .authMethod(Property.ofValue(AuthMethod.PASSWORD))
            .password(PASSWORD)
            .port(Property.ofValue("2222"))
            .commands(new String[] {"echo 0"})
            .build();

        assertThat(passwordCommand.getOpenSSHConfigDir(), is(nullValue()));

        Command publicKeyCommand = Command.builder()
            .id(IdUtils.create())
            .type(Command.class.getName())
            .host(Property.ofValue("localhost"))
            .username(USERNAME)
            .authMethod(Property.ofValue(AuthMethod.PUBLIC_KEY))
            .privateKey(Property.ofValue("fake-key"))
            .port(Property.ofValue("2222"))
            .commands(new String[] {"echo 0"})
            .build();

        assertThat(publicKeyCommand.getOpenSSHConfigDir(), is(nullValue()));
    }

    @Test
    void run_openSSHMethod_withResolvedConfigPathProceedsToConnectionAttempt() throws Exception {
        Path tempConfig = Files.createTempFile("ssh-config", "");
        Files.writeString(tempConfig, "Host *\n");

        Command command = Command.builder()
            .id(IdUtils.create())
            .type(Command.class.getName())
            .host(Property.ofValue("unreachable.invalid"))
            .username(USERNAME)
            .password(PASSWORD)
            .openSSHConfigPath(Property.ofValue(tempConfig.toString()))
            .authMethod(Property.ofValue(AuthMethod.OPEN_SSH))
            .port(Property.ofValue("2222"))
            .commands(new String[] {"echo 0"})
            .build();

        // The temp fixture guarantees the OpenSSH config file actually exists on disk (unlike the
        // real `~/.ssh/config`, whose presence depends on the machine running the test), so parsing
        // it never throws `FileNotFoundException`, and the `openSSHConfigPath` -> `openSSHConfigDir`
        // -> default `Optional` resolution chain never throws `NoSuchElementException` either.
        // Execution genuinely reaches an SSH connection attempt, which fails against the unreachable
        // host with a connection error.
        Exception exception = Assertions.assertThrows(
            Exception.class,
            () -> command.run(TestsUtils.mockRunContext(runContextFactory, command, Map.of()))
        );

        assertThat(exception, is(not(instanceOf(NoSuchElementException.class))));
        assertThat(exception, is(not(instanceOf(FileNotFoundException.class))));
        assertThat(exception.getMessage(), not(containsString("No value present")));
    }

    @Test
    void run_openSSHMethod_withoutConfigPropertiesFallsBackToDefaultPath() throws Exception {
        Path tempHome = Files.createTempDirectory("ssh-home");
        Path sshDir = Files.createDirectory(tempHome.resolve(".ssh"));
        Files.writeString(sshDir.resolve("config"), "Host *\n");

        String previousUserHome = System.getProperty("user.home");
        System.setProperty("user.home", tempHome.toString());
        try {
            Command command = Command.builder()
                .id(IdUtils.create())
                .type(Command.class.getName())
                .host(Property.ofValue("unreachable.invalid"))
                .username(USERNAME)
                .password(PASSWORD)
                .authMethod(Property.ofValue(AuthMethod.OPEN_SSH))
                .port(Property.ofValue("2222"))
                .commands(new String[] {"echo 0"})
                .build();

            // Neither `openSSHConfigPath` nor `openSSHConfigDir` is set, so the task must fall back to
            // the default `~/.ssh/config` computed from `user.home` at run time (overridden here to a
            // temp directory containing a real config fixture). Parsing it never throws
            // `FileNotFoundException`, and the resolution chain never throws `NoSuchElementException`
            // either: execution genuinely reaches an SSH connection attempt, which fails against the
            // unreachable host with a connection error.
            Exception exception = Assertions.assertThrows(
                Exception.class,
                () -> command.run(TestsUtils.mockRunContext(runContextFactory, command, Map.of()))
            );

            assertThat(exception, is(not(instanceOf(NoSuchElementException.class))));
            assertThat(exception, is(not(instanceOf(FileNotFoundException.class))));
            assertThat(exception.getMessage(), not(containsString("No value present")));
        } finally {
            System.setProperty("user.home", previousUserHome);
        }
    }

    @Test
    void run_openSSHMethod() throws Exception {
        Command command = Command.builder()
            .id(IdUtils.create())
            .type(Command.class.getName())
            .host(Property.ofValue("localhost"))
            .openSSHConfigPath(Property.ofValue("src/test/resources/ssh/config"))
            .password(PASSWORD)
            .authMethod(Property.ofValue(AuthMethod.OPEN_SSH))
            .port(Property.ofValue("2222"))
            .commands(new String[] {
                "echo 0",
                "echo 1",
                ">&2 echo 2",
                "echo '::{\"outputs\":{\"out\":\"1\"}}::'",
                ">&2 echo '::{\"outputs\":{\"err\":\"2\"}}::'",
            })
            .build();

        Command.Output run = command.run(TestsUtils.mockRunContext(runContextFactory, command, Map.of()));

        Thread.sleep(500);

        assertThat(run.getExitCode(), is(0));
        assertThat(run.getStdOutLineCount(), is(3));
        assertThat(run.getStdErrLineCount(), is(2));
        assertThat(run.getVars().get("out"), is("1"));
        assertThat(run.getVars().get("err"), is("2"));
    }

    @Test
    void kill_stopsRunningCommand() throws Exception {
        Command command = Command.builder()
            .id(IdUtils.create())
            .type(Command.class.getName())
            .host(Property.ofValue("localhost"))
            .username(USERNAME)
            .authMethod(Property.ofValue(AuthMethod.PASSWORD))
            .password(PASSWORD)
            .port(Property.ofValue("2222"))
            .commands(new String[] {"sleep 30"})
            .build();

        AtomicReference<Exception> thrown = new AtomicReference<>();
        long start = System.nanoTime();

        Thread runner = new Thread(() -> {
            try {
                command.run(TestsUtils.mockRunContext(runContextFactory, command, Map.of()));
            } catch (Exception e) {
                thrown.set(e);
            }
        }, "ssh-command-kill-test");
        runner.start();

        // Let the SSH connection establish and the remote `sleep 30` actually start before killing it.
        Thread.sleep(2000);

        command.kill();

        runner.join(Duration.ofSeconds(10).toMillis());
        long elapsedSeconds = Duration.ofNanos(System.nanoTime() - start).toSeconds();

        assertThat("run() should have unwound well before the full 30s sleep completes", runner.isAlive(), is(false));
        assertThat(elapsedSeconds, lessThan(15L));
        assertThat(thrown.get(), is(notNullValue()));
        assertThat(thrown.get().getMessage(), containsString("killed"));
    }

    @Test
    void run_succeedsAfterPriorKill_retrySafety() throws Exception {
        // Same task instance killed once, then re-run: `killed` must be reset at the start of run()
        // so the fresh attempt is not short-circuited by the previous kill.
        Command command = Command.builder()
            .id(IdUtils.create())
            .type(Command.class.getName())
            .host(Property.ofValue("localhost"))
            .username(USERNAME)
            .authMethod(Property.ofValue(AuthMethod.PASSWORD))
            .password(PASSWORD)
            .port(Property.ofValue("2222"))
            .commands(new String[] {"sleep 5"})
            .build();

        AtomicReference<Exception> thrown = new AtomicReference<>();
        Thread runner = new Thread(() -> {
            try {
                command.run(TestsUtils.mockRunContext(runContextFactory, command, Map.of()));
            } catch (Exception e) {
                thrown.set(e);
            }
        }, "ssh-command-retry-kill-test");
        runner.start();

        Thread.sleep(1000);
        command.kill();
        runner.join(Duration.ofSeconds(10).toMillis());

        assertThat(runner.isAlive(), is(false));
        assertThat(thrown.get(), is(notNullValue()));
        assertThat(thrown.get().getMessage(), containsString("killed"));

        // Retry: same task instance, allowed to run to completion this time (no kill() called).
        Command.Output run = command.run(TestsUtils.mockRunContext(runContextFactory, command, Map.of()));

        assertThat(run.getExitCode(), is(0));
    }

    @Test
    void kill_isNoOpBeforeRunAndWhenCalledTwice() {
        Command command = Command.builder()
            .id(IdUtils.create())
            .type(Command.class.getName())
            .host(Property.ofValue("localhost"))
            .username(USERNAME)
            .authMethod(Property.ofValue(AuthMethod.PASSWORD))
            .password(PASSWORD)
            .port(Property.ofValue("2222"))
            .commands(new String[] {"echo 0"})
            .build();

        // Nothing was ever tracked, kill()/stop() must simply no-op instead of throwing.
        assertDoesNotThrow(command::kill);
        assertDoesNotThrow(command::stop);
        // A second kill() after the first must also be a no-op (compareAndSet guard).
        assertDoesNotThrow(command::kill);
    }
}
