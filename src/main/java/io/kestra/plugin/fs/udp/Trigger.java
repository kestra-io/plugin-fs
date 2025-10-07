package io.kestra.plugin.fs.udp;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.fs.realtime.RealtimeTrigger;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;

@Setter
@SuperBuilder
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
@Plugin(
    examples = {
        @Example(
            title = "Listen for UDP messages on port 5000",
            code = """
                id: udp_trigger_flow
                namespace: company.team

                tasks:
                  - id: log
                    type: io.kestra.plugin.core.debug.Return
                    format: "Received UDP: {{ trigger.message }}"

                triggers:
                  - id: udp
                    type: io.kestra.plugin.fs.udp.UdpRealtimeTrigger
                    port: 5000
                """
        )
    }
)
public class Trigger extends RealtimeTrigger<Trigger.Output> {
    @PluginProperty
    private Integer port;

    @PluginProperty
    @Builder.Default
    private Integer bufferSize = 4096;

    public Trigger() {
        super();
    }

    @Override
    protected void listen(RunContext runContext,
                          TriggerContext triggerContext,
                          ConditionContext conditionContext) {
        runContext.logger().info("UDP RealtimeTrigger listening on port {}", port);

        try (DatagramSocket socket = new DatagramSocket(port, InetAddress.getByName("0.0.0.0"))) {
            byte[] buffer = new byte[bufferSize];

            while (!Thread.currentThread().isInterrupted()) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                String remoteHost = packet.getAddress().getHostAddress();

                runContext.logger().debug("UDP packet from {}: {}", remoteHost, message);

                emitEvent(runContext, conditionContext, triggerContext, Output.builder()
                    .port(port)
                    .message(message.trim())
                    .remoteAddress(remoteHost)
                    .build());
            }
        } catch (Exception e) {
            runContext.logger().error("UDP Trigger error", e);
        }
    }

    @Override
    public Publisher<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        return Flux.create(emitter -> {
            try (DatagramSocket socket = new DatagramSocket(port, InetAddress.getByName("0.0.0.0"))) {
                byte[] buffer = new byte[bufferSize];
                while (!Thread.currentThread().isInterrupted()) {
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    socket.receive(packet);

                    String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                    String remoteHost = packet.getAddress().getHostAddress();

                    Output output = Output.builder()
                        .port(port)
                        .message(message.trim())
                        .remoteAddress(remoteHost)
                        .build();

                    emitter.next(TriggerService.generateRealtimeExecution(
                        this,
                        conditionContext,
                        context,
                        output
                    ));
                }
            } catch (Exception e) {
                emitter.error(e);
            }
        });
    }

    @Builder
    @Getter
    @ToString
    public static class Output implements io.kestra.core.models.tasks.Output {
        private final Integer port;
        private final String message;
        private final String remoteAddress;
    }
}
