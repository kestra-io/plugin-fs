package io.kestra.plugin.fs.icmp;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.fs.ReachabilityExpectation;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Check if hosts answer to ICMP ping",
    description = """
        Probes each host sequentially with `InetAddress.isReachable` from the worker and returns a typed result per host: packets sent and received, loss, and min/avg/max round-trip time.

        A host is reachable when at least one probe is answered. Use `expect` to fail the task when the outcome is not the one you want; for an "address must be free" check, set `expect: UNREACHABLE`. With the default `ANY` the task never fails because of reachability. All hosts are probed before the expectation is applied, and a single error lists every host that broke it.

        **ICMP privileges.** The JDK does not run the `ping` binary. On Linux it first tries a raw ICMP socket, which needs root or the `CAP_NET_RAW` capability (in a container, add it with `--cap-add=NET_RAW`). If that is not allowed it tries an unprivileged ICMP datagram socket, which requires the worker group id to be inside `net.ipv4.ping_group_range`. If both are denied, the JDK silently falls back to a TCP connection to port 7 (echo), which most hosts refuse or filter; the host may then be reported unreachable even though it is up. Make sure the worker has one of the two privileges before relying on the result."""
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Ping a list of hosts and log the result.",
            code = """
                id: icmp_ping
                namespace: company.team

                tasks:
                  - id: ping
                    type: io.kestra.plugin.fs.icmp.Ping
                    hosts:
                      - 192.168.1.1
                      - example.com
                    count: 4
                    timeout: PT1S

                  - id: log
                    type: io.kestra.plugin.core.log.Log
                    message: "All reachable: {{ outputs.ping.allReachable }}"
                """
        ),
        @Example(
            full = true,
            title = "Fail if an IP address is already in use.",
            code = """
                id: icmp_ip_must_be_free
                namespace: company.team

                tasks:
                  - id: ip_is_free
                    type: io.kestra.plugin.fs.icmp.Ping
                    hosts:
                      - 10.0.0.12
                    expect: UNREACHABLE
                """
        ),
        @Example(
            full = true,
            title = "Wait until a device is back online.",
            code = """
                id: icmp_wait_for_device
                namespace: company.team

                tasks:
                  - id: wait_for_device
                    type: io.kestra.plugin.core.flow.WaitFor
                    condition: "{{ outputs.ping.results[0].reachable }}"
                    interval: PT10S
                    timeout: PT10M
                    tasks:
                      - id: ping
                        type: io.kestra.plugin.fs.icmp.Ping
                        hosts:
                          - 192.168.1.50
                        count: 1
                """
        )
    }
)
public class Ping extends Task implements RunnableTask<Ping.Output> {

    @Schema(title = "Hosts to ping", description = "Host names or IPv4/IPv6 addresses. Probed one after the other.")
    @NotNull
    @PluginProperty(group = "main")
    private Property<List<String>> hosts;

    @Schema(title = "Number of probes per host", description = "Between 1 and 100.")
    @Builder.Default
    @Min(1)
    @Max(100)
    @PluginProperty(group = "execution")
    private Property<Integer> count = Property.ofValue(4);

    @Schema(title = "Timeout per probe")
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Duration> timeout = Property.ofValue(Duration.ofSeconds(1));

    @Schema(title = "Delay between two probes of the same host")
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Duration> interval = Property.ofValue(Duration.ofSeconds(1));

    @Schema(
        title = "Expected outcome",
        description = "`REACHABLE` fails the task if a host does not answer, `UNREACHABLE` fails it if a host answers, `ANY` never fails on reachability."
    )
    @Builder.Default
    @PluginProperty(group = "reliability")
    private Property<ReachabilityExpectation> expect = Property.ofValue(ReachabilityExpectation.ANY);

    @Override
    public Output run(RunContext runContext) throws Exception {
        List<String> rHosts = runContext.render(this.hosts).asList(String.class);
        if (rHosts.isEmpty()) {
            throw new IllegalArgumentException("`hosts` cannot be empty");
        }
        int rCount = runContext.render(this.count).as(Integer.class).orElse(4);
        if (rCount < 1 || rCount > 100) {
            throw new IllegalArgumentException("`count` must be between 1 and 100, got " + rCount);
        }
        Duration rTimeout = runContext.render(this.timeout).as(Duration.class).orElse(Duration.ofSeconds(1));
        Duration rInterval = runContext.render(this.interval).as(Duration.class).orElse(Duration.ofSeconds(1));
        ReachabilityExpectation rExpect = runContext.render(this.expect).as(ReachabilityExpectation.class).orElse(ReachabilityExpectation.ANY);

        // ponytail: hosts are probed sequentially to keep the worker footprint small;
        // probing them in parallel is the upgrade path for large host lists.
        List<HostResult> results = new ArrayList<>();
        for (String host : rHosts) {
            HostResult result = probe(host, rCount, rTimeout, rInterval);
            runContext.logger().info(
                "{}: {}/{} probes answered ({}% loss)",
                host, result.getPacketsReceived(), result.getPacketsSent(), result.getPacketLoss()
            );
            results.add(result);
        }

        List<String> violations = new ArrayList<>();
        for (HostResult r : results) {
            if (rExpect == ReachabilityExpectation.UNREACHABLE && r.isReachable()) {
                violations.add("Expected UNREACHABLE but " + r.getHost() + " answered " + r.getPacketsReceived() + "/" + r.getPacketsSent() + " probes");
            } else if (rExpect == ReachabilityExpectation.REACHABLE && !r.isReachable()) {
                violations.add("Expected REACHABLE but " + r.getHost() + " answered " + r.getPacketsReceived() + "/" + r.getPacketsSent() + " probes"
                    + (r.getError() != null ? " (" + r.getError() + ")" : ""));
            }
        }
        if (!violations.isEmpty()) {
            throw new IllegalStateException(String.join("; ", violations));
        }

        return Output.builder()
            .results(results)
            .allReachable(results.stream().allMatch(HostResult::isReachable))
            .build();
    }

    private static HostResult probe(String host, int count, Duration timeout, Duration interval) throws InterruptedException {
        HostResult.HostResultBuilder builder = HostResult.builder().host(host);

        InetAddress address;
        try {
            address = InetAddress.getByName(host);
        } catch (Exception e) {
            // DNS failure is a result, not a crash: the caller decides through `expect`.
            return builder.reachable(false).packetsSent(0).packetsReceived(0).packetLoss(100.0).error(e.toString()).build();
        }

        int received = 0;
        long min = Long.MAX_VALUE;
        long max = 0;
        long total = 0;
        String error = null;
        for (int i = 0; i < count; i++) {
            long start = System.nanoTime();
            try {
                if (address.isReachable((int) timeout.toMillis())) {
                    long rtt = System.nanoTime() - start;
                    received++;
                    min = Math.min(min, rtt);
                    max = Math.max(max, rtt);
                    total += rtt;
                }
            } catch (Exception e) {
                error = e.toString();
            }
            if (i < count - 1 && !interval.isZero() && !interval.isNegative()) {
                Thread.sleep(interval.toMillis());
            }
        }

        builder.reachable(received > 0)
            .packetsSent(count)
            .packetsReceived(received)
            .packetLoss(Math.round((count - received) * 1000.0 / count) / 10.0)
            .error(received == 0 ? error : null);
        if (received > 0) {
            builder.rttMin(Duration.ofNanos(min)).rttAvg(Duration.ofNanos(total / received)).rttMax(Duration.ofNanos(max));
        }
        return builder.build();
    }

    @Builder
    @Getter
    public static class HostResult {
        @Schema(title = "The probed host")
        private final String host;

        @Schema(title = "Whether at least one probe was answered")
        private final boolean reachable;

        @Schema(title = "Number of probes sent")
        private final int packetsSent;

        @Schema(title = "Number of probes answered")
        private final int packetsReceived;

        @Schema(title = "Packet loss in percent")
        private final double packetLoss;

        @Schema(title = "Minimum round-trip time", description = "Empty when no probe was answered.")
        private final Duration rttMin;

        @Schema(title = "Average round-trip time", description = "Empty when no probe was answered.")
        private final Duration rttAvg;

        @Schema(title = "Maximum round-trip time", description = "Empty when no probe was answered.")
        private final Duration rttMax;

        @Schema(title = "Error message, for example a DNS failure")
        private final String error;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "One result per host, in input order")
        private final List<HostResult> results;

        @Schema(title = "Whether every host is reachable")
        private final boolean allReachable;
    }
}
