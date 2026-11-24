package io.kestra.plugin.fs.icmp;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.validations.ModelValidator;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.fs.ReachabilityExpectation;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class PingTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private ModelValidator modelValidator;

    private Ping ping(String host, ReachabilityExpectation expect) {
        return Ping.builder()
            .hosts(Property.ofValue(List.of(host)))
            .count(Property.ofValue(2))
            .probeTimeout(Property.ofValue(Duration.ofMillis(200)))
            .interval(Property.ofValue(Duration.ZERO))
            .expect(Property.ofValue(expect))
            .build();
    }

    @Test
    void localhostIsReachable() throws Exception {
        var output = ping("127.0.0.1", ReachabilityExpectation.ANY).run(runContextFactory.of());

        assertThat(output.isAllReachable(), is(true));
        var r = output.getResults().getFirst();
        assertThat(r.isReachable(), is(true));
        assertThat(r.getPacketsSent(), is(2));
        assertThat(r.getPacketsReceived(), is(2));
        assertThat(r.getPacketLoss(), is(0.0));
        assertThat(r.getRttMin(), notNullValue());
    }

    @Test
    void unroutableHostIsUnreachable() throws Exception {
        // 192.0.2.1 is TEST-NET-1 (RFC 5737), never routed.
        var output = ping("192.0.2.1", ReachabilityExpectation.ANY).run(runContextFactory.of());

        assertThat(output.isAllReachable(), is(false));
        var r = output.getResults().getFirst();
        assertThat(r.isReachable(), is(false));
        assertThat(r.getPacketLoss(), is(100.0));
        assertThat(r.getRttAvg(), nullValue());
    }

    @Test
    void unknownHostGivesErrorWithoutThrowing() throws Exception {
        var output = ping("no-such-host.invalid", ReachabilityExpectation.ANY).run(runContextFactory.of());

        assertThat(output.getResults().getFirst().isReachable(), is(false));
        assertThat(output.getResults().getFirst().getError(), notNullValue());
    }

    @Test
    void expectReachableThrowsOnUnreachable() {
        var e = assertThrows(IllegalStateException.class,
            () -> ping("192.0.2.1", ReachabilityExpectation.REACHABLE).run(runContextFactory.of()));
        assertThat(e.getMessage(), containsString("Expected REACHABLE but 192.0.2.1"));
    }

    @Test
    void expectUnreachableThrowsOnLocalhost() {
        var e = assertThrows(IllegalStateException.class,
            () -> ping("127.0.0.1", ReachabilityExpectation.UNREACHABLE).run(runContextFactory.of()));
        assertThat(e.getMessage(), is("Expected UNREACHABLE but 127.0.0.1 answered 2/2 probes"));
    }

    @Test
    void emptyHostsIsRejected() {
        var task = Ping.builder().hosts(Property.ofValue(List.of())).build();
        assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
    }

    @Test
    void outOfRangeCountIsRejected() {
        for (int count : new int[]{0, 101}) {
            var task = Ping.builder().hosts(Property.ofValue(List.of("127.0.0.1"))).count(Property.ofValue(count)).build();
            var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
            assertThat(e.getMessage(), containsString("between 1 and 100"));
        }
    }

    @Test
    void passesKestraValidation() {
        // Flow creation runs bean validation; constraints on Property<...> fields used to fail it with HV000030.
        var task = Ping.builder().id("ping").type(Ping.class.getName()).hosts(Property.ofValue(List.of("127.0.0.1"))).build();
        assertThat(modelValidator.isValid(task).isPresent(), is(false));
    }

    @Test
    void probeTimeoutDoesNotCollideWithTaskTimeout() throws Exception {
        var task = Ping.builder()
            .hosts(Property.ofValue(List.of("127.0.0.1")))
            .count(Property.ofValue(2))
            .interval(Property.ofValue(Duration.ofMillis(300)))
            .build();

        // Core Task.timeout must stay unset: it is the overall limit, not the per-probe one.
        assertThat(task.getTimeout(), nullValue());
        assertThat(task.getProbeTimeout(), notNullValue());
        assertThat(task.run(runContextFactory.of()).isAllReachable(), is(true));
    }

    @Test
    void rendersExpressions() throws Exception {
        var task = Ping.builder()
            .hosts(Property.ofExpression("{{ inputs.hosts }}"))
            .count(Property.ofExpression("{{ inputs.count }}"))
            .expect(Property.ofExpression("{{ inputs.expect }}"))
            .interval(Property.ofValue(Duration.ZERO))
            .build();

        var output = task.run(runContextFactory.of(Map.of("inputs", Map.of("hosts", List.of("127.0.0.1"), "count", 3, "expect", "REACHABLE"))));

        assertThat(output.getResults().getFirst().getPacketsSent(), is(3));
    }

    @Test
    void blankRenderedHostIsRejected() {
        var task = Ping.builder().hosts(Property.ofExpression("{{ inputs.hosts }}")).build();
        var e = assertThrows(IllegalArgumentException.class,
            () -> task.run(runContextFactory.of(Map.of("inputs", Map.of("hosts", List.of(""))))));
        assertThat(e.getMessage(), containsString("empty entry"));
    }

    @Test
    void tooManyHostsIsRejected() {
        var task = Ping.builder().hosts(Property.ofValue(Collections.nCopies(257, "127.0.0.1"))).build();
        var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(e.getMessage(), containsString("maximum is 256"));
    }

    @Test
    void invalidDurationsAreRejected() {
        var zeroTimeout = Ping.builder().hosts(Property.ofValue(List.of("127.0.0.1"))).probeTimeout(Property.ofValue(Duration.ZERO)).build();
        assertThrows(IllegalArgumentException.class, () -> zeroTimeout.run(runContextFactory.of()));

        var negativeInterval = Ping.builder().hosts(Property.ofValue(List.of("127.0.0.1"))).interval(Property.ofValue(Duration.ofSeconds(-1))).build();
        assertThrows(IllegalArgumentException.class, () -> negativeInterval.run(runContextFactory.of()));
    }

    @Test
    void subMillisecondProbeTimeoutIsRejected() {
        var task = Ping.builder().hosts(Property.ofValue(List.of("127.0.0.1"))).probeTimeout(Property.ofValue(Duration.parse("PT0.0005S"))).build();
        assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
    }
}
