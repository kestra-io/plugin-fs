package io.kestra.plugin.fs.ssh;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.ssh.SshInterface.AuthMethod;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import java.nio.charset.*;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

// WARNING, the 'setpasswd.sh' script must be runnable for the test to pass, if the test fail try launching:
// chmod go+x src/test/resources/ssh/setpasswd.sh
@KestraTest
class CommandTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run_passwordMethod() throws Exception {
        Command command = Command.builder()
            .id(CommandTest.class.getName())
            .type(CommandTest.class.getName())
            .host("localhost")
            .username("foo")
            .authMethod(AuthMethod.PASSWORD)
            .password("password")
            .port("2222")
            .commands(new String[] {
                "echo 0",
                "echo 1",
                ">&2 echo 2",
                "echo '::{\"outputs\":{\"out\":\"1\"}}::'",
                ">&2 echo '::{\"outputs\":{\"err\":\"2\"}}::'",
            })
            .build();

        Command.ScriptOutput run = command.run(TestsUtils.mockRunContext(runContextFactory, command, ImmutableMap.of()));

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
            .id(CommandTest.class.getName())
            .type(CommandTest.class.getName())
            .host("localhost")
            .username("foo")
            .authMethod(AuthMethod.PUBLIC_KEY)
            .privateKey(keyFileContent)
            .port("2222")
            .commands(new String[] {
                "echo 0",
                "echo 1",
                ">&2 echo 2",
                "echo '::{\"outputs\":{\"out\":\"1\"}}::'",
                ">&2 echo '::{\"outputs\":{\"err\":\"2\"}}::'",
            })
            .build();

        Command.ScriptOutput run = command.run(TestsUtils.mockRunContext(runContextFactory, command, ImmutableMap.of()));

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
            .host("localhost")
            .password("password")
            .authMethod(AuthMethod.OPEN_SSH)
            .port("2222")
            .commands(new String[] {
                "echo 0",
                "echo 1",
                ">&2 echo 2",
                "echo '::{\"outputs\":{\"out\":\"1\"}}::'",
                ">&2 echo '::{\"outputs\":{\"err\":\"2\"}}::'",
            })
            .build();

        Command.ScriptOutput run = command.run(TestsUtils.mockRunContext(runContextFactory, command, ImmutableMap.of()));

        Thread.sleep(500);

        assertThat(run.getExitCode(), is(0));
        assertThat(run.getStdOutLineCount(), is(3));
        assertThat(run.getStdErrLineCount(), is(2));
        assertThat(run.getVars().get("out"), is("1"));
        assertThat(run.getVars().get("err"), is("2"));
    }
}
