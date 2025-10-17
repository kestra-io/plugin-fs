package io.kestra.plugin.fs.tcp;

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

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

@Plugin(
    examples = {
        @Example(
            title = "Send a simple message to a TCP server",
            code = {
                "host: \"127.0.0.1\"",
                "port: 5000",
                "payload: \"Hello from Kestra!\""
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

    @PluginProperty
    @Builder.Default
    private Integer timeoutMs = 5000;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String renderedHost = runContext.render(this.host);
        String renderedPayload = runContext.render(this.payload);

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(renderedHost, port), timeoutMs);

            try (OutputStream out = socket.getOutputStream()) {
                out.write((renderedPayload + "\n").getBytes(StandardCharsets.UTF_8));
                out.flush();
            }

            runContext.logger().info("Sent TCP message to {}:{} -> {}", renderedHost, port, renderedPayload);

            return Output.builder()
                .host(renderedHost)
                .port(port)
                .sentBytes(renderedPayload.length())
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
