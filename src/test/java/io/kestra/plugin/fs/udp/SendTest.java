package io.kestra.plugin.fs.udp;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@KestraTest(startWorker = true)
class SendTest {
    private static int PORT;
    private static DatagramSocket serverSocket;
    private static ExecutorService executor;
    private static final BlockingQueue<String> receivedMessages = new LinkedBlockingQueue<>();

    @Inject
    private RunContextFactory runContextFactory;

    @BeforeAll
    static void setup() throws Exception {
        executor = Executors.newSingleThreadExecutor();
        CompletableFuture<Void> serverReady = new CompletableFuture<>();

        executor.submit(() -> {
            try {
                DatagramSocket socket = new DatagramSocket(0); // bind to a free port
                PORT = socket.getLocalPort();
                serverSocket = socket;
                serverReady.complete(null);

                byte[] buffer = new byte[4096];
                while (!Thread.currentThread().isInterrupted() && !socket.isClosed()) {
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    socket.receive(packet);

                    String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                    receivedMessages.add(message);
                }
            } catch (Exception e) {
                serverReady.completeExceptionally(e);
            }
        });

        serverReady.get(5, TimeUnit.SECONDS);
    }

    @AfterAll
    static void teardown() throws Exception {
        if (serverSocket != null && !serverSocket.isClosed()) {
            serverSocket.close();
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    void testUdpSend() throws Exception {
        String payload = "Hello from UdpSend!";

        Send task = Send.builder()
            .id("udp-send-task")
            .type(Send.class.getSimpleName())
            .host("127.0.0.1")
            .port(PORT)
            .payload(payload)
            .build();

        // Mock RunContext
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        Send.Output output = task.run(runContext);

        // Verify output
        assertEquals(PORT, output.getPort());
        assertEquals(payload.length(), output.getSentBytes());

        // Verify server received it
        String received = receivedMessages.poll(2, TimeUnit.SECONDS);
        assertEquals(payload, received);
    }
}
