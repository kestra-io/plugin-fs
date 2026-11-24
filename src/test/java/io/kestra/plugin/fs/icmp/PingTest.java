package io.kestra.plugin.fs.icmp;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.validations.ModelValidator;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.fs.ReachabilityExpectation;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class PingTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private ModelValidator modelValidator;

    private Ping.PingBuilder<?, ?> ping(String host, ReachabilityExpectation expect) {
        return Ping.builder()
            .hosts(Property.ofValue(List.of(host)))
            .count(Property.ofValue(2))
            .timeout(Property.ofValue(Duration.ofMillis(200)))
            .interval(Property.ofValue(Duration.ZERO))
            .expect(Property.ofValue(expect));
    }

    @Test
    void localhostIsReachable() throws Exception {
        Ping.Output output = ping("127.0.0.1", ReachabilityExpectation.ANY).build().run(runContextFactory.of());

        assertThat(output.isAllReachable(), is(true));
        Ping.HostResult r = output.getResults().getFirst();
        assertThat(r.isReachable(), is(true));
        assertThat(r.getPacketsSent(), is(2));
        assertThat(r.getPacketsReceived(), is(2));
        assertThat(r.getPacketLoss(), is(0.0));
        assertThat(r.getRttMin(), notNullValue());
    }

    @Test
    void unroutableHostIsUnreachable() throws Exception {
        // 192.0.2.1 is TEST-NET-1 (RFC 5737), never routed.
        Ping.Output output = ping("192.0.2.1", ReachabilityExpectation.ANY).build().run(runContextFactory.of());

        assertThat(output.isAllReachable(), is(false));
        Ping.HostResult r = output.getResults().getFirst();
        assertThat(r.isReachable(), is(false));
        assertThat(r.getPacketLoss(), is(100.0));
        assertThat(r.getRttAvg(), nullValue());
    }

    @Test
    void unknownHostGivesErrorWithoutThrowing() throws Exception {
        Ping.Output output = ping("no-such-host.invalid", ReachabilityExpectation.ANY).build().run(runContextFactory.of());

        assertThat(output.getResults().getFirst().isReachable(), is(false));
        assertThat(output.getResults().getFirst().getError(), notNullValue());
    }

    @Test
    void expectReachableThrowsOnUnreachable() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> ping("192.0.2.1", ReachabilityExpectation.REACHABLE).build().run(runContextFactory.of()));
        assertThat(e.getMessage(), containsString("Expected REACHABLE but 192.0.2.1"));
    }

    @Test
    void expectUnreachableThrowsOnLocalhost() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> ping("127.0.0.1", ReachabilityExpectation.UNREACHABLE).build().run(runContextFactory.of()));
        assertThat(e.getMessage(), is("Expected UNREACHABLE but 127.0.0.1 answered 2/2 probes"));
    }

    @Test
    void emptyHostsIsRejected() {
        Ping task = Ping.builder().hosts(Property.ofValue(List.of())).build();
        assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
    }

    @Test
    void outOfRangeCountIsRejected() {
        for (int count : new int[]{0, 101}) {
            Ping task = ping("127.0.0.1", ReachabilityExpectation.ANY).count(Property.ofValue(count)).build();
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
            assertThat(e.getMessage(), containsString("between 1 and 100"));
        }
    }

    @Test
    void passesKestraValidation() {
        // Flow creation runs bean validation; constraints on Property<...> fields used to fail it with HV000030.
        Ping task = ping("127.0.0.1", ReachabilityExpectation.ANY).id("ping").type(Ping.class.getName()).build();
        assertThat(modelValidator.isValid(task).isPresent(), is(false));
    }
}
