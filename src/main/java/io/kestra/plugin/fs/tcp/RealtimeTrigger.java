package io.kestra.plugin.fs.tcp;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.inject.Inject;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger on incoming TCP messages",
    description = "Listens on a TCP port and fires immediately when data is received. Default bind host 0.0.0.0, UTF-8 decoding. Stops when trigger is canceled or kill/stop is called."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Trigger a flow whenever a TCP message is received.",
            code = """
                id: tcp_realtime
                namespace: dev

                triggers:
                  - id: tcp_listener
                    type: io.kestra.plugin.fs.tcp.RealtimeTrigger
                    host: 0.0.0.0
                    port: 9090

                tasks:
                  - id: log_message
                    type: io.kestra.plugin.core.log.Log
                    message: "Received {{ trigger.payload }} from {{ trigger.sourceIp }}"
                """
        )
    }
)
public class RealtimeTrigger extends AbstractTrigger
    implements RealtimeTriggerInterface, TriggerOutput<RealtimeTrigger.Output> {

    @Inject
    @Builder.Default
    private TcpService tcpService = TcpService.getInstance();

    @Schema(title = "Interface to bind", defaultValue = "0.0.0.0")
    @Builder.Default
    private Property<String> host = Property.ofValue("0.0.0.0");

    @Schema(title = "TCP port to listen on")
    @NotNull
    private Property<Integer> port;

    @Schema(title = "Encoding for incoming data", defaultValue = "UTF-8")
    @Builder.Default
    private Property<String> encoding = Property.ofValue(StandardCharsets.UTF_8.name());

    private transient final AtomicBoolean active = new AtomicBoolean(false);
    private transient ServerSocket serverSocket;

    @Override
    public Publisher<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        
        String rHost = runContext.render(this.host).as(String.class).orElse("0.0.0.0");
        Integer rPort = runContext.render(this.port).as(Integer.class)
            .orElseThrow(() -> new IllegalArgumentException("`port` is required"));
        String rEncoding = runContext.render(this.encoding).as(String.class).orElse(StandardCharsets.UTF_8.name());

        active.set(true);

        return Flux.<Execution>create(emitter -> {
            try {
                serverSocket = new ServerSocket();
                serverSocket.bind(new InetSocketAddress(rHost, rPort));
                logger.info("TCP RealtimeTrigger listening on {}:{}", rHost, rPort);

                while (active.get()) {
                    try (Socket client = serverSocket.accept()) {
                        if (!active.get()) break;

                        String sourceIp = client.getInetAddress().getHostAddress();
                        int sourcePort = client.getPort();

                        BufferedReader reader = new BufferedReader(
                            new InputStreamReader(client.getInputStream(), rEncoding)
                        );

                        StringBuilder sb = new StringBuilder();
                        String line;
                        while ((line = reader.readLine()) != null) {
                            sb.append(line);
                        }

                        String message = sb.toString().trim();
                        if (message.isEmpty()) continue;

                        logger.info("Received TCP message from {}:{} -> {}", sourceIp, sourcePort, message);

                        Output output = Output.builder()
                            .payload(message)
                            .timestamp(Instant.now())
                            .sourceIp(sourceIp)
                            .sourcePort(sourcePort)
                            .build();

                        emitter.next(TriggerService.generateRealtimeExecution(this, conditionContext, context, output));
                    } catch (SocketException se) {
                        if (active.get()) {
                            logger.warn("Socket exception: {}", se.getMessage());
                        }
                        break;
                    } catch (Exception e) {
                        if (active.get()) {
                            logger.error("Error handling TCP connection: {}", e.getMessage(), e);
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("TCP listener stopped due to error: {}", e.getMessage(), e);
                emitter.error(e);
            } finally {
                closeServerSocket(logger, rPort);
                emitter.complete();
            }
        }).doOnCancel(() -> {
            logger.info("TCP RealtimeTrigger cancelled by flow change or stop request.");
            stop();
        });
    }

    @Override
    public void stop() {
        if (active.compareAndSet(true, false)) {
            try {
                if (serverSocket != null && !serverSocket.isClosed()) {
                    serverSocket.close();
                }
                System.out.println("[TcpRealtimeTrigger] Socket closed via stop()");
            } catch (Exception e) {
                System.out.println("[TcpRealtimeTrigger] Error closing socket: " + e.getMessage());
            }
        }
    }

    @Override
    public void kill() {
        System.out.println("[TcpRealtimeTrigger] Kill signal received");
        stop();
    }

    private void closeServerSocket(Logger logger, Integer port) {
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
                if (logger != null && port != null) {
                    logger.info("TCP RealtimeTrigger stopped listening on port {}", port);
                } else {
                    System.out.println("[TcpRealtimeTrigger] Stopped listening on port " + port);
                }
            }
        } catch (Exception e) {
            if (logger != null) {
                logger.warn("Error closing server socket: {}", e.getMessage());
            } else {
                System.out.println("[TcpRealtimeTrigger] Error closing socket: " + e.getMessage());
            }
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The received TCP payload.")
        private final String payload;

        @Schema(title = "The timestamp when the message was received.")
        private final Instant timestamp;

        @Schema(title = "The IP address of the sender.")
        private final String sourceIp;

        @Schema(title = "The port of the sender.")
        private final Integer sourcePort;
    }
}
