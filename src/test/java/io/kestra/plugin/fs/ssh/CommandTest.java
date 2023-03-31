package io.kestra.plugin.fs.ssh;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.fs.sftp.List;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

// WARNING, the 'setpasswd.sh' script must be runnable for the test to pass, if the test fail try launching:
// chmod go+x src/test/resources/ssh/setpasswd.sh
@MicronautTest
class CommandTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        Command command = Command.builder()
            .id(CommandTest.class.getName())
            .type(CommandTest.class.getName())
            .host("localhost")
            .username("foo")
            .password("password")
            .port("2222")
            .commands(new String[] {"echo 'Hello World'", "echo 'Hello World'"})
            .build();

        command.run(TestsUtils.mockRunContext(runContextFactory, command, ImmutableMap.of()));
    }
}