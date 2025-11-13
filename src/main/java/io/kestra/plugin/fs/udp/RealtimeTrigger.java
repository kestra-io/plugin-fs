package io.kestra.plugin.fs.udp;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
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
    title = "Trigger a flow when a UDP message is received in real-time."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Trigger a flow whenever a UDP datagram is received.",
            code = """
                id: udp_listener
                namespace: dev

                triggers:
                  - id: udp_incoming
                    type: io.kestra.plugin.fs.udp.RealtimeTrigger
                    host: 0.0.0.0
                    port: 8091

                tasks:
                  - id: log_message
                    type: io.kestra.plugin.core.log.Log
                    message: "📩 Received UDP: {{ trigger.payload }} from {{ trigger.sourceIp }}:{{ trigger.sourcePort }}"
                """
        )
    }
)
public class RealtimeTrigger extends AbstractTrigger
    implements RealtimeTriggerInterface, TriggerOutput<RealtimeTrigger.Output> {

    @Schema(title = "The interface to bind.", defaultValue = "0.0.0.0")
    @Builder.Default
    private Property<String> host = Property.ofValue("0.0.0.0");

    @Schema(title = "The UDP port to listen on.")
    @NotNull
    private Property<Integer> port;

    @Schema(title = "Buffer size in bytes.", defaultValue = "1024")
    @Builder.Default
    private Property<Integer> bufferSize = Property.ofValue(1024);

    @Schema(title = "Encoding for incoming data.", defaultValue = "UTF-8")
    @Builder.Default
    private Property<String> encoding = Property.ofValue(StandardCharsets.UTF_8.name());

    private transient final AtomicBoolean active = new AtomicBoolean(false);
    private transient DatagramSocket socket;

    @Override
    public Publisher<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        String rHost = runContext.render(this.host).as(String.class).orElse("0.0.0.0");
        Integer rPort = runContext.render(this.port).as(Integer.class)
            .orElseThrow(() -> new IllegalArgumentException("`port` is required"));
        Integer rBuffer = runContext.render(this.bufferSize).as(Integer.class).orElse(1024);
        String rEncoding = runContext.render(this.encoding).as(String.class).orElse(StandardCharsets.UTF_8.name());

        active.set(true);

        return Flux.<Execution>create(emitter -> {
            try (DatagramSocket udpSocket = new DatagramSocket(rPort)) {
                this.socket = udpSocket;
                logger.info("UDP RealtimeTrigger listening on {}:{}", rHost, rPort);

                byte[] buffer = new byte[rBuffer];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

                while (active.get()) {
                    try {
                        udpSocket.receive(packet);
                        String payload = new String(packet.getData(), 0, packet.getLength(), rEncoding);
                        String sourceIp = packet.getAddress().getHostAddress();
                        int sourcePort = packet.getPort();

                        logger.info("Received UDP packet from {}:{} -> {}", sourceIp, sourcePort, payload);

                        Output output = Output.builder()
                            .payload(payload)
                            .timestamp(Instant.now())
                            .sourceIp(sourceIp)
                            .sourcePort(sourcePort)
                            .build();

                        emitter.next(TriggerService.generateRealtimeExecution(this, conditionContext, context, output));

                    } catch (SocketException se) {
                        if (active.get()) {
                            logger.warn("Socket exception while active: {}", se.getMessage());
                        }
                        break; 
                    } catch (Exception e) {
                        if (active.get()) {
                            logger.error("Error while receiving UDP packet: {}", e.getMessage(), e);
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("UDP listener stopped due to error: {}", e.getMessage(), e);
                emitter.error(e);
            } finally {
                logger.info("UDP RealtimeTrigger stopped listening on port {}", rPort);
                emitter.complete();
            }
        }).doOnCancel(() -> {
            logger.info("UDP RealtimeTrigger cancelled.");
            stop();
        });
    }

    @Override
    public void stop() {
        if (active.compareAndSet(true, false)) {
            if (socket != null && !socket.isClosed()) {
                try {
                    socket.close();
                } catch (Exception e) {
                    System.out.println("[UdpRealtimeTrigger] Error closing socket: " + e.getMessage());
                }
            }
        }
    }

    @Override
    public void kill() {
        stop();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The received UDP payload.")
        private final String payload;

        @Schema(title = "The timestamp when the message was received.")
        private final Instant timestamp;

        @Schema(title = "The IP address of the sender.")
        private final String sourceIp;

        @Schema(title = "The port of the sender.")
        private final Integer sourcePort;
    }
}
