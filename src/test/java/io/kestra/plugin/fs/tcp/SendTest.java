package io.kestra.plugin.fs.tcp;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.models.property.Property;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class SendTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldSendMessageOverTcp() throws Exception {
        String expected = "Hello Kestra TCP Test";
        int port;

        try (ServerSocket serverSocket = new ServerSocket(0)) {
            port = serverSocket.getLocalPort();

            Future<String> listener = Executors.newSingleThreadExecutor().submit(() -> {
                try (Socket socket = serverSocket.accept();
                     BufferedReader reader = new BufferedReader(
                         new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
                    return reader.readLine();
                }
            });

            RunContext runContext = runContextFactory.of();

            Send task = Send.builder()
                .host(Property.ofValue("127.0.0.1"))
                .port(Property.ofValue(port))
                .payload(Property.ofValue(expected))
                .build();

            Send.Output output = task.run(runContext);

            assertThat(output.getHost(), is("127.0.0.1"));
            assertThat(output.getPort(), is(port));
            assertThat(output.getSentBytes(), greaterThan(0));

            String received = listener.get();
            assertThat(received, is(expected));
        }
    }
}
