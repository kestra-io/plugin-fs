package io.kestra.plugin.fs.udp;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;

@Plugin(
    examples = {
        @Example(
            title = "Send a simple message to a UDP server",
            code = {
                "host: \"127.0.0.1\"",
                "port: 5000",
                "payload: \"Hello from Kestra UDP!\""
            }
        )
    }
)
@Getter
@ToString
@EqualsAndHashCode
@NoArgsConstructor
@SuperBuilder
public class Send extends Task implements RunnableTask<Send.Output> {
    @NotNull
    @PluginProperty
    private String host;

    @NotNull
    @PluginProperty
    private Integer port;

    @NotNull
    @PluginProperty
    private String payload;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String renderedHost = runContext.render(this.host);
        String renderedPayload = runContext.render(this.payload);

        byte[] messageBytes = renderedPayload.getBytes(StandardCharsets.UTF_8);

        InetAddress address = InetAddress.getByName(renderedHost);

        try (DatagramSocket socket = new DatagramSocket()) {
            DatagramPacket packet = new DatagramPacket(messageBytes, messageBytes.length, address, port);
            socket.send(packet);

            runContext.logger().info("Sent UDP message to {}:{} -> {}", renderedHost, port, renderedPayload);

            return Output.builder()
                .host(renderedHost)
                .port(port)
                .sentBytes(messageBytes.length)
                .build();
        }
    }

    @Override
    public void kill() {
        RunnableTask.super.kill();
    }

    @Override
    public void stop() {
        RunnableTask.super.stop();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        private String host;
        private Integer port;
        private Integer sentBytes;
    }
}
