package io.kestra.plugin.fs.tcp;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.fs.ReachabilityExpectation;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.Map;

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
            .connectTimeout(Property.ofValue(Duration.ofSeconds(1)))
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
            var output = check(server.getLocalPort(), ReachabilityExpectation.ANY).run(runContextFactory.of());

            assertThat(output.isOpen(), is(true));
            assertThat(output.getConnectTime(), notNullValue());
            assertThat(output.getError(), nullValue());
        }
    }

    @Test
    void closedPort() throws Exception {
        var output = check(freePort(), ReachabilityExpectation.ANY).run(runContextFactory.of());

        assertThat(output.isOpen(), is(false));
        assertThat(output.getConnectTime(), nullValue());
        assertThat(output.getError(), notNullValue());
    }

    @Test
    void expectReachableThrowsOnClosedPort() throws Exception {
        var port = freePort();
        assertThrows(IllegalStateException.class,
            () -> check(port, ReachabilityExpectation.REACHABLE).run(runContextFactory.of()));
    }

    @Test
    void expectUnreachableThrowsOnOpenPort() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            var port = server.getLocalPort();
            assertThrows(IllegalStateException.class,
                () -> check(port, ReachabilityExpectation.UNREACHABLE).run(runContextFactory.of()));
        }
    }

    @Test
    void rendersExpressions() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            var task = Check.builder()
                .host(Property.ofExpression("{{ inputs.host }}"))
                .port(Property.ofExpression("{{ inputs.port }}"))
                .build();

            var output = task.run(runContextFactory.of(Map.of("inputs", Map.of("host", "127.0.0.1", "port", server.getLocalPort()))));

            assertThat(output.isOpen(), is(true));
        }
    }

    @Test
    void blankHostIsRejected() {
        var task = Check.builder().host(Property.ofExpression("{{ inputs.host }}")).port(Property.ofValue(80)).build();
        assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of(Map.of("inputs", Map.of("host", "")))));
    }

    @Test
    void invalidPortFailsInsteadOfReportingClosed() {
        for (int port : new int[]{0, 70000}) {
            assertThrows(IllegalArgumentException.class,
                () -> check(port, ReachabilityExpectation.UNREACHABLE).run(runContextFactory.of()));
        }
    }

    @Test
    void nonPositiveTimeoutIsRejected() {
        var task = Check.builder().host(Property.ofValue("127.0.0.1")).port(Property.ofValue(80)).connectTimeout(Property.ofValue(Duration.ZERO)).build();
        assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
    }

    @Test
    void subMillisecondConnectTimeoutIsRejected() {
        var task = Check.builder().host(Property.ofValue("127.0.0.1")).port(Property.ofValue(80)).connectTimeout(Property.ofValue(Duration.parse("PT0.0005S"))).build();
        assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
    }

    @Test
    void expectUnreachableFailsWhenHostCannotBeResolved() {
        var task = Check.builder().host(Property.ofValue("no-such-host.invalid")).port(Property.ofValue(80))
            .expect(Property.ofValue(ReachabilityExpectation.UNREACHABLE)).build();
        var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of()));
        assertThat(e.getMessage(), is("Expected UNREACHABLE but no-such-host.invalid:80 could not be probed (Cannot resolve host)"));
    }

    @Test
    void expectUnreachablePassesOnRefusedConnection() throws Exception {
        var output = check(freePort(), ReachabilityExpectation.UNREACHABLE).run(runContextFactory.of());
        assertThat(output.isOpen(), is(false));
    }
}
