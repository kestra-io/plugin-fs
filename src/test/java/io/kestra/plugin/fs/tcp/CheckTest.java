package io.kestra.plugin.fs.tcp;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.fs.ReachabilityExpectation;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.time.Duration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class CheckTest {
    @Inject
    private RunContextFactory runContextFactory;

    private Check check(int port, ReachabilityExpectation expect) {
        return Check.builder()
            .host(Property.ofValue("127.0.0.1"))
            .port(Property.ofValue(port))
            .timeout(Property.ofValue(Duration.ofSeconds(1)))
            .expect(Property.ofValue(expect))
            .build();
    }

    private int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    @Test
    void openPort() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            Check.Output output = check(server.getLocalPort(), ReachabilityExpectation.ANY).run(runContextFactory.of());

            assertThat(output.isOpen(), is(true));
            assertThat(output.getConnectTime(), notNullValue());
            assertThat(output.getError(), nullValue());
        }
    }

    @Test
    void closedPort() throws Exception {
        Check.Output output = check(freePort(), ReachabilityExpectation.ANY).run(runContextFactory.of());

        assertThat(output.isOpen(), is(false));
        assertThat(output.getConnectTime(), nullValue());
        assertThat(output.getError(), notNullValue());
    }

    @Test
    void expectReachableThrowsOnClosedPort() throws Exception {
        int port = freePort();
        assertThrows(IllegalStateException.class,
            () -> check(port, ReachabilityExpectation.REACHABLE).run(runContextFactory.of()));
    }

    @Test
    void expectUnreachableThrowsOnOpenPort() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            int port = server.getLocalPort();
            assertThrows(IllegalStateException.class,
                () -> check(port, ReachabilityExpectation.UNREACHABLE).run(runContextFactory.of()));
        }
    }
}
