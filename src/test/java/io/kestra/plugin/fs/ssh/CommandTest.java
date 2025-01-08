package io.kestra.plugin.fs.ssh;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.ssh.SshInterface.AuthMethod;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

// WARNING, the 'setpasswd.sh' script must be runnable for the test to pass, if the test fail try launching:
// chmod go+x src/test/resources/ssh/setpasswd.sh
@KestraTest
class CommandTest {
    public static final Property<String> USERNAME = Property.of("foo");
    public static final Property<String> PASSWORD = Property.of("O7m)&H/0Em4/T8RqCa!Al=M@N6^;@+");

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run_passwordMethod() throws Exception {
        Command command = Command.builder()
            .id(IdUtils.create())
            .type(CommandTest.class.getName())
            .host(Property.of("localhost"))
            .username(USERNAME)
            .authMethod(Property.of(AuthMethod.PASSWORD))
            .password(PASSWORD)
            .port(Property.of("2222"))
            .commands(new String[] {
                "echo 0",
                "echo 1",
                ">&2 echo 2",
                "echo '::{\"outputs\":{\"out\":\"1\"}}::'",
                ">&2 echo '::{\"outputs\":{\"err\":\"2\"}}::'",
            })
            .build();

        Command.ScriptOutput run = command.run(TestsUtils.mockRunContext(runContextFactory, command, Map.of()));

        Thread.sleep(500);

        assertThat(run.getExitCode(), is(0));
        assertThat(run.getStdOutLineCount(), is(3));
        assertThat(run.getStdErrLineCount(), is(2));
        assertThat(run.getVars().get("out"), is("1"));
        assertThat(run.getVars().get("err"), is("2"));
    }

    @Test
    void run_pubkeyMethod() throws Exception {
        File file = new File("src/test/resources/ssh/id_ed25519");
        byte[] data;
        try (FileInputStream fis = new FileInputStream(file)) {
            data = new byte[(int) file.length()];
            fis.read(data);
        }
        String keyFileContent = new String(data, StandardCharsets.UTF_8);

        Command command = Command.builder()
            .id(IdUtils.create())
            .type(CommandTest.class.getName())
            .host(Property.of("localhost"))
            .username(USERNAME)
            .authMethod(Property.of(AuthMethod.PUBLIC_KEY))
            .privateKey(Property.of(keyFileContent))
            .port(Property.of("2222"))
            .commands(new String[] {
                "echo 0",
                "echo 1",
                ">&2 echo 2",
                "echo '::{\"outputs\":{\"out\":\"1\"}}::'",
                ">&2 echo '::{\"outputs\":{\"err\":\"2\"}}::'",
            })
            .build();

        Command.ScriptOutput run = command.run(TestsUtils.mockRunContext(runContextFactory, command, Map.of()));

        Thread.sleep(500);

        assertThat(run.getExitCode(), is(0));
        assertThat(run.getStdOutLineCount(), is(3));
        assertThat(run.getStdErrLineCount(), is(2));
        assertThat(run.getVars().get("out"), is("1"));
        assertThat(run.getVars().get("err"), is("2"));
    }

    @Test
    @Disabled("Cannot work on CI")
    void run_openSSHMethod() throws Exception {
        Command command = Command.builder()
            .id(CommandTest.class.getName())
            .type(CommandTest.class.getName())
            .host(Property.of("localhost"))
            .password(PASSWORD)
            .authMethod(Property.of(AuthMethod.OPEN_SSH))
            .port(Property.of("2222"))
            .commands(new String[] {
                "echo 0",
                "echo 1",
                ">&2 echo 2",
                "echo '::{\"outputs\":{\"out\":\"1\"}}::'",
                ">&2 echo '::{\"outputs\":{\"err\":\"2\"}}::'",
            })
            .build();

        Command.ScriptOutput run = command.run(TestsUtils.mockRunContext(runContextFactory, command, Map.of()));

        Thread.sleep(500);

        assertThat(run.getExitCode(), is(0));
        assertThat(run.getStdOutLineCount(), is(3));
        assertThat(run.getStdErrLineCount(), is(2));
        assertThat(run.getVars().get("out"), is("1"));
        assertThat(run.getVars().get("err"), is("2"));
    }
}
