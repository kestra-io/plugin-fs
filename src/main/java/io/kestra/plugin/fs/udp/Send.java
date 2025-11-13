package io.kestra.plugin.fs.udp;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Send a UDP datagram message to a specific host and port."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a UDP message",
            full = true,
            code = """
                id: udp_send_example
                namespace: dev

                tasks:
                  - id: send_udp
                    type: io.kestra.plugin.fs.udp.Send
                    host: 127.0.0.1
                    port: 8081
                    payload: "Hello via UDP"
                """
        )
    }
)
public class Send extends Task implements RunnableTask<Send.Output> {
    @Schema(title = "Target host or IP address.")
    @NotNull
    private Property<String> host;

    @Schema(title = "Target UDP port.")
    @NotNull
    private Property<Integer> port;

    @Schema(title = "Message payload to send.")
    @NotNull
    private Property<String> payload;

    @Schema(title = "Character encoding for the payload.", defaultValue = "UTF-8")
    @Builder.Default
    private Property<String> encoding = Property.ofValue("UTF-8");

    @Override
    public Output run(RunContext runContext) throws Exception {
        String rHost = runContext.render(this.host).as(String.class).orElseThrow();
        Integer rPort = runContext.render(this.port).as(Integer.class).orElseThrow();
        String rPayload = runContext.render(this.payload).as(String.class).orElse("");
        String rEncoding = runContext.render(this.encoding).as(String.class).orElse(StandardCharsets.UTF_8.name());

        runContext.logger().info("Sending UDP datagram to {}:{}...", rHost, rPort);

        byte[] data = rPayload.getBytes(rEncoding);
        InetAddress address = InetAddress.getByName(rHost);

        try (DatagramSocket socket = new DatagramSocket()) {
            DatagramPacket packet = new DatagramPacket(data, data.length, address, rPort);
            socket.send(packet);
            runContext.logger().info("Sent UDP packet of {} bytes to {}:{}", data.length, rHost, rPort);
        }

        return Output.builder()
            .host(rHost)
            .port(rPort)
            .sentBytes(data.length)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The target host.")
        private final String host;

        @Schema(title = "The target port.")
        private final Integer port;

        @Schema(title = "The number of bytes sent.")
        private final Integer sentBytes;
    }
}
