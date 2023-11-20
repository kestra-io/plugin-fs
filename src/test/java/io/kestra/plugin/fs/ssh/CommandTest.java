package io.kestra.plugin.fs.ssh;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

// WARNING, the 'setpasswd.sh' script must be runnable for the test to pass, if the test fail try launching:
// chmod go+x src/test/resources/ssh/setpasswd.sh
@MicronautTest
class CommandTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    @Named(QueueFactoryInterface.WORKERTASKLOG_NAMED)
    private QueueInterface<LogEntry> logQueue;

    @Test
    void run() throws Exception {
        List<LogEntry> logs = new ArrayList<>();
        logQueue.receive(l -> logs.add(l.getLeft()));

        Command command = Command.builder()
            .id(CommandTest.class.getName())
            .type(CommandTest.class.getName())
            .host("localhost")
            .username("foo")
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
}
