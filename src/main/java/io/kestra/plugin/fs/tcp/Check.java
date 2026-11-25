package io.kestra.plugin.fs.tcp;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.fs.ReachabilityExpectation;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.time.Duration;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Check if a TCP port is open",
    description = """
        Opens a TCP connection to `host:port` and closes it right away without sending any payload. The port is open when the connection is established within `connectTimeout`. A filtered port ends at `connectTimeout` and is reported as closed.

        Use `expect` to fail the task when the outcome is not the one you want; with the default `ANY` the task never fails on reachability. An invalid `host` or `port` fails the task instead of being reported as closed."""
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
                    connectTimeout: PT3S
                    expect: REACHABLE
                """
        )
    }
)
public class Check extends Task implements RunnableTask<Check.Output> {

    @Schema(title = "Target host or IP")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> host;

    @Schema(title = "Target port")
    @NotNull
    @PluginProperty(group = "main")
    private Property<Integer> port;

    @Schema(
        title = "Connection timeout",
        description = """
            Between 1 millisecond and 5 minutes. Applies to the connection attempt only; the inherited task-level `timeout` is the overall limit.""",
        defaultValue = "PT5S"
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Duration> connectTimeout = Property.ofValue(Duration.ofSeconds(5));

    @Schema(
        title = "Expected outcome",
        description = """
            `REACHABLE` fails the task if the port is closed, `UNREACHABLE` fails it if the port is open or the host cannot be probed (DNS failure, no route), `ANY` never fails. A refused connection or a timeout counts as not open.""",
        defaultValue = "ANY"
    )
    @Builder.Default
    @PluginProperty(group = "reliability")
    private Property<ReachabilityExpectation> expect = Property.ofValue(ReachabilityExpectation.ANY);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rHost = runContext.render(this.host).as(String.class).orElseThrow(() -> new IllegalArgumentException("`host` cannot be null or empty"));
        if (rHost.isBlank()) {
            throw new IllegalArgumentException("`host` cannot be blank, check the rendered value");
        }
        var rPort = runContext.render(this.port).as(Integer.class).orElseThrow(() -> new IllegalArgumentException("`port` cannot be null or empty"));
        if (rPort < 1 || rPort > 65535) {
            throw new IllegalArgumentException("`port` must be between 1 and 65535, got " + rPort);
        }
        var rTimeout = runContext.render(this.connectTimeout).as(Duration.class).orElse(Duration.ofSeconds(5));
        if (rTimeout.toMillis() < 1 || rTimeout.compareTo(Duration.ofMinutes(5)) > 0) {
            throw new IllegalArgumentException("`connectTimeout` must be between 1 millisecond and 5 minutes, got " + rTimeout);
        }
        var rExpect = runContext.render(this.expect).as(ReachabilityExpectation.class).orElse(ReachabilityExpectation.ANY);

        var open = false;
        String error = null;
        var probeFailed = false;
        Duration connectTime = null;
        var start = System.nanoTime();
        try (var ignored = TcpService.getInstance().connect(rHost, rPort, rTimeout)) {
            connectTime = Duration.ofNanos(System.nanoTime() - start);
            open = true;
        } catch (UnknownHostException e) {
            error = "Cannot resolve host";
            probeFailed = true;
        } catch (SocketTimeoutException e) {
            error = "Timed out after " + rTimeout;
        } catch (IOException e) {
            error = e.getMessage() != null ? e.getMessage() : "Connection failed";
            // ConnectException covers refused and OS-level timeouts, both real "not open" answers.
            // Other IO errors (NoRouteToHostException for host or network unreachable) are not.
            probeFailed = !(e instanceof ConnectException);
        }

        runContext.logger().info("{}:{} is {}", rHost, rPort, open ? "open" : "closed or unreachable");

        if (rExpect == ReachabilityExpectation.UNREACHABLE && open) {
            throw new IllegalStateException("Expected UNREACHABLE but " + rHost + ":" + rPort + " accepted the connection");
        }
        if (rExpect == ReachabilityExpectation.UNREACHABLE && probeFailed) {
            throw new IllegalStateException("Expected UNREACHABLE but " + rHost + ":" + rPort + " could not be probed (" + error + ")");
        }
        if (rExpect == ReachabilityExpectation.REACHABLE && !open) {
            throw new IllegalStateException("Expected REACHABLE but " + rHost + ":" + rPort + " is closed (" + error + ")");
        }

        return Output.builder().open(open).connectTime(connectTime).error(error).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Whether the connection was established")
        private final boolean open;

        @Schema(
            title = "Time to establish the connection",
            description = """
                Empty when the port is closed."""
        )
        private final Duration connectTime;

        @Schema(title = "Error message when the connection failed")
        private final String error;
    }
}
