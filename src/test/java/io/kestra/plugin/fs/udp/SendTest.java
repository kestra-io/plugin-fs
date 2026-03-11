package io.kestra.plugin.fs.udp;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
public class SendTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldSendUdpMessage() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        int testPort = 9200 + (int) (Math.random() * 100);

        final String expectedMessage = "Hello from UDP Send Test";

        Thread listenerThread = new Thread(() -> {
            try (DatagramSocket serverSocket = new DatagramSocket(testPort)) {
                byte[] buffer = new byte[1024];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                serverSocket.receive(packet);
                String receivedMessage = new String(packet.getData(), 0, packet.getLength());
                assertThat(receivedMessage, is(expectedMessage));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        listenerThread.start();

        Thread.sleep(500); 

        Send sendTask = Send.builder()
            .host(Property.ofValue("127.0.0.1"))
            .port(Property.ofValue(testPort))
            .payload(Property.ofValue(expectedMessage))
            .build();

        Send.Output output = sendTask.run(runContext);

        assertThat(output.getHost(), is("127.0.0.1"));
        assertThat(output.getPort(), is(testPort));
        assertThat(output.getSentBytes(), is(expectedMessage.getBytes().length));
    }
}
