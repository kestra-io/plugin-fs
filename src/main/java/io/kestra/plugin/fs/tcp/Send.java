package io.kestra.plugin.fs.tcp;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.inject.Inject;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Send data to a TCP server",
    description = "Opens a TCP socket, sends the payload, and closes the connection. Default encoding UTF-8; set encoding to BASE64 for binary. Connection timeout defaults to 10s."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Send a UTF-8 message to a TCP server.",
            code = """
                id: tcp_send_example
                namespace: dev

                tasks:
                  - id: send_tcp
                    type: io.kestra.plugin.fs.tcp.Send
                    host: 127.0.0.1
                    port: 9090
                    payload: \"Hello from Kestra\"
                """
        )
    }
)
public class Send extends Task implements RunnableTask<Send.Output> {

    @Inject
    @Builder.Default
    private TcpService tcpService = TcpService.getInstance();

    @Schema(title = "Target host or IP")
    @NotNull
    private Property<String> host;

    @Schema(title = "Target port")
    @NotNull
    private Property<Integer> port;

    @Schema(title = "Payload to send")
    @NotNull
    private Property<String> payload;

    @Schema(title = "Payload encoding", description = "Use BASE64 for binary data; otherwise UTF-8.", defaultValue = "UTF-8")
    @Builder.Default
    private Property<String> encoding = Property.ofValue(StandardCharsets.UTF_8.name());

    @Schema(title = "Socket timeout", description = "Timeout for establishing the connection and IO; default 10s.")
    private Property<Duration> timeout;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        String rHost = runContext.render(this.host).as(String.class).orElseThrow(() -> new IllegalArgumentException("`host` cannot be null or empty"));
        Integer rPort = runContext.render(this.port).as(Integer.class).orElseThrow(() -> new IllegalArgumentException("`port` cannot be null or empty"));
        String rPayload = runContext.render(this.payload).as(String.class).orElseThrow(() -> new IllegalArgumentException("`payload` cannot be null or empty"));
        String rEncoding = runContext.render(this.encoding).as(String.class).orElse(StandardCharsets.UTF_8.name());
        Duration rTimeout = runContext.render(this.timeout).as(Duration.class).orElse(Duration.ofSeconds(10));

        logger.info("Connecting to {}:{} (encoding: {}, timeout: {}).", rHost, rPort, rEncoding, rTimeout);

        int sentBytes;
        try (Socket socket = tcpService.connect(rHost, rPort, rTimeout)) {
            byte[] bytes = tcpService.encodePayload(rPayload, rEncoding);
            try (OutputStream os = socket.getOutputStream()) {
                os.write(bytes);
                os.flush();
                sentBytes = bytes.length;
                logger.info("Sent {} bytes to {}:{} successfully.", sentBytes, rHost, rPort);
            }
        } catch (IOException e) {
            logger.error("Failed to send TCP message to {}:{} - {}", rHost, rPort, e.getMessage(), e);
            throw e;
        }

        return Output.builder()
            .host(rHost)
            .port(rPort)
            .sentBytes(sentBytes)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The target host.")
        private final String host;

        @Schema(title = "The target port.")
        private final Integer port;

        @Schema(title = "Number of bytes sent.")
        private final Integer sentBytes;
    }
}
