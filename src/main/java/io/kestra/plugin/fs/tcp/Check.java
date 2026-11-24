package io.kestra.plugin.fs.tcp;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.fs.ReachabilityExpectation;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.inject.Inject;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.net.Socket;
import java.time.Duration;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Check if a TCP port is open",
    description = "Opens a TCP connection to `host:port` and closes it right away without sending any payload. The port is open when the connection is established within `timeout`. A filtered port ends at the timeout and is reported as closed. Use `expect` to fail the task when the outcome is not the one you want; with the default `ANY` the task never fails on reachability."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Check that a database port is open.",
            code = """
                id: tcp_check
                namespace: company.team

                tasks:
                  - id: check_db
                    type: io.kestra.plugin.fs.tcp.Check
                    host: db.example.com
                    port: 5432
                    timeout: PT3S
                    expect: REACHABLE
                """
        )
    }
)
public class Check extends Task implements RunnableTask<Check.Output> {

    @Inject
    @Builder.Default
    @PluginProperty(group = "advanced")
    @JsonIgnore
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private TcpService tcpService = TcpService.getInstance();

    @Schema(title = "Target host or IP")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> host;

    @Schema(title = "Target port")
    @NotNull
    @PluginProperty(group = "main")
    private Property<Integer> port;

    @Schema(title = "Connection timeout")
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Duration> timeout = Property.ofValue(Duration.ofSeconds(5));

    @Schema(
        title = "Expected outcome",
        description = "`REACHABLE` fails the task if the port is closed, `UNREACHABLE` fails it if the port is open, `ANY` never fails."
    )
    @Builder.Default
    @PluginProperty(group = "reliability")
    private Property<ReachabilityExpectation> expect = Property.ofValue(ReachabilityExpectation.ANY);

    @Override
    public Output run(RunContext runContext) throws Exception {
        String rHost = runContext.render(this.host).as(String.class).orElseThrow(() -> new IllegalArgumentException("`host` cannot be null or empty"));
        Integer rPort = runContext.render(this.port).as(Integer.class).orElseThrow(() -> new IllegalArgumentException("`port` cannot be null or empty"));
        Duration rTimeout = runContext.render(this.timeout).as(Duration.class).orElse(Duration.ofSeconds(5));
        ReachabilityExpectation rExpect = runContext.render(this.expect).as(ReachabilityExpectation.class).orElse(ReachabilityExpectation.ANY);

        boolean open = false;
        String error = null;
        long start = System.nanoTime();
        Duration connectTime;
        try (Socket ignored = tcpService.connect(rHost, rPort, rTimeout)) {
            connectTime = Duration.ofNanos(System.nanoTime() - start);
            open = true;
        } catch (Exception e) {
            connectTime = null;
            error = e.toString();
        }

        runContext.logger().info("{}:{} is {}", rHost, rPort, open ? "open" : "closed or unreachable");

        if (rExpect == ReachabilityExpectation.UNREACHABLE && open) {
            throw new IllegalStateException("Expected UNREACHABLE but " + rHost + ":" + rPort + " accepted the connection");
        }
        if (rExpect == ReachabilityExpectation.REACHABLE && !open) {
            throw new IllegalStateException("Expected REACHABLE but " + rHost + ":" + rPort + " is closed (" + error + ")");
        }

        return Output.builder().host(rHost).port(rPort).open(open).connectTime(connectTime).error(error).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The target host")
        private final String host;

        @Schema(title = "The target port")
        private final Integer port;

        @Schema(title = "Whether the connection was established")
        private final boolean open;

        @Schema(title = "Time to establish the connection", description = "Empty when the port is closed.")
        private final Duration connectTime;

        @Schema(title = "Error message when the connection failed")
        private final String error;
    }
}
