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
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

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
}
